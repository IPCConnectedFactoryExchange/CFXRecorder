"""Base class for all broker-link implementations.

The base class provides the generic functionality for:
- running a loop that consumes messages
- checking for commands received from an ephemeral control queue
- dispatching those commands to subclases to handle
- reporting metrics on how the link is running
"""

from typing import Tuple, Dict, Optional, List
import dataclasses
import queue
import uuid
import time
from concurrent.futures import Future
import logging
import json
import gzip
import pickle
import amqp
from .amqp import BackgroundAMQPConnection, MetricSet, WorkerHeartbeat, WorkerEvent
from .metrics import BrokerLinkHealth
from .exceptions import ConnectionClosedError


LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class _ConsumerInfo:
    queue: Optional[str] = None
    """The input queue name that we should consume from.

    This queue must already exist.
    """

    consumer_tag: Optional[str] = None
    """The consumer tag that we registered when we started consuming from this queue.

    If this is None then we are not currently consuming from the queue.  If it
    is a valid string then we are currently consuming from this queue.
    """

    def is_consuming(self) -> bool:
        """Whether we are currently consuming from the queue."""

        return self.consumer_tag is not None


@dataclasses.dataclass
class _ControlInfo:
    """Internal class for capturing all required information for remote control."""

    worker_id: str
    allowed: bool = False

    def main_topic(self) -> str:
        """The main control topic for commands specifically directed at this worker."""

        return "worker.{worker}.control".format(worker=self.worker_id)

    def broadcast_topic(self) -> str:
        """A generic topic for broadcast commands to all workers."""

        return "worker.$broadcast.control"

    def all_bindings(self) -> List[str]:
        """The complete set of topic bindings for this worker."""

        return [self.main_topic(), self.broadcast_topic()]

    def queue_name(self) -> str:
        """The name of the queue that this worker should generate for control commands."""

        return "control-{worker}".format(worker=self.worker_id)

    def classify_message(self, message: amqp.Message) -> Tuple[bool, bool]:
        """Classify a message as a control message and whether it's broadcast or directed."""

        routing_key = message.delivery_info.get('routing_key')

        if routing_key == self.main_topic():
            return True, False

        if routing_key == self.broadcast_topic():
            return True, True

        return False, False

    def is_control_message(self, message: amqp.Message) -> bool:
        """Check if a message is either a broadcast or directed control message."""

        is_directed, is_broadcast = self.classify_message(message)

        return is_directed or is_broadcast


class BaseLink:
    """Base class for a generic broker-link output system that processes messages."""

    WAKEUP_TIME = 0.1

    _input: _ConsumerInfo
    _save_path: Optional[str]

    def __init__(self, conn: BackgroundAMQPConnection, *,
                 worker_id: str = None):

        if worker_id is None:
            worker_id = "broker-link-" + str(uuid.uuid4())

        LOG.info("broker-link configured with worker-id: %s", worker_id)

        self._conn = conn
        self._worker_id = worker_id

        self._control = _ControlInfo(worker_id=worker_id)
        self._input = _ConsumerInfo()
        self._save_path = None
        self._now = time.monotonic()
        self._last_callback = self._now

        self.metrics = MetricSet(monitoring_interval=0.0, worker_id=worker_id)
        self.metrics.register_metric('health', BrokerLinkHealth())
        self.metrics.register_metric('heartbeat', WorkerHeartbeat(heartbeat_interval=0.0))

        self.started = Future()

    def adjust_monitoring(self, interval: float):
        """Adjust the rate at which monitoring data is reported.

        Args:
            interval: The new monitoring interval in seconds.
        """

        self.metrics.heartbeat.heartbeat_interval = interval
        self.metrics.interval = interval

    def enable_remote_control(self):
        """Create and bind a temporary queue for controlling this worker."""

        if self._control.allowed:
            raise RuntimeError("enable_remote_control() called multiple times")

        control_queue = self._control.queue_name()

        self._conn.bind_temporary_queue(control_queue, self._control.all_bindings())
        self._conn.start_consuming(control_queue)
        self._control.allowed = True

        LOG.info("Worker control enabled on topic: %s", self._control.main_topic())

    def record_messages(self, path: str):
        """Save a pickled copy of all messages received that could be played back later.

        All of the input messages are serialized using the pickle module and
        then gzipped into a single file.  You can load that file later using
        ``PickledAMQPConnection`` in order to play it back exactly the same as
        if the messages were being read directly from a live broker.
        """

        self._save_path = path

    def enable_input(self, queue: str):
        """Begin consuming from the given input queue."""

        if self._input.is_consuming():
            raise RuntimeError("enable_input() called multiple times")

        self._input.consumer_tag = self._conn.start_consuming(queue)
        self._input.queue = queue

        LOG.info("Started consuming from %s (consumer=%s)",
                 self._input.queue, self._input.consumer_tag)

    def disable_input(self):
        """Stop consuming from the input queue."""

        if not self._input.is_consuming():
            raise RuntimeError("disable_input called when input not enabled")

        self._conn.stop_consuming(self._input.consumer_tag)
        self._input.consumer_tag = None

    def run(self):
        """Run this batch processor until interrupted by Ctrl-C.

        This loop pulls messages from the input amqp consumers.  These
        messages include data messages and if remote control is enabled, also
        control messages.  The control messages are dispatched to
        ``process_control_message`` and the messages are dispatched to
        ``process_message``.
        """

        try:
            self._start()
        except Exception as err:
            self.started.set_exception(err)
            raise

        self.started.set_result(None)

        if self.metrics.enabled():
            self._conn.publish_metric(self._worker_id, WorkerEvent(event="start"))

        self.metrics.reset_timer()

        should_exit = False
        save_file = None
        if self._save_path is not None:
            save_file = gzip.open(self._save_path, "wb")
            LOG.info("Recording all messages using pickle to file %s", self._save_path)

        try:
            while not should_exit:
                try:
                    message = self._conn.messages.get(timeout=self.WAKEUP_TIME)
                    if message is None:
                        LOG.error("Connection closed unexpectedly")
                        raise ConnectionClosedError("Connection error, aborting")

                    self._now = time.monotonic()

                    LOG.log(5, "Processing message with key: %s", message.delivery_info.get('routing_key'))
                    if save_file is not None:
                        serialize_amqp_message(message, save_file)

                    if self._control.is_control_message(message):
                        should_exit = self.dispatch_control_message(message)
                    else:
                        self.metrics.health.received_messages += 1

                        if _check_message_selected(message.delivery_info.get('routing_key'), None):
                            self.process_message(message)
                        else:
                            self._conn.acknowledge_message(message)
                except queue.Empty:
                    self._now = time.monotonic()

                if (self._now - self._last_callback) > 1.0:
                    self.periodic_callback()
                    self._last_callback = self._now

                self.publish_metrics_if_needed()
        finally:
            if save_file is not None:
                save_file.close()

            self._stop()

            if self.metrics.enabled():
                self._conn.publish_metric(self._worker_id, WorkerEvent(event="stop"))

    def process_message(self, message: amqp.Message):
        """Subclasses override this method to process messages."""

        self.metrics.health.processed_messages += 1
        self._conn.acknowledge_message(message)

    def periodic_callback(self):
        """Subclasses override this method if they need a callback every second.

        Link classes that need to implement timers or other timing triggered
        activities independent of message activity should implement this
        function.
        """

        self.publish_metrics_if_needed()

    def process_control_message(self, action: str, args: dict, broadcast: bool):
        """Subclases override this method to process custom control messages."""

        LOG.warning("Received unknown control message: %s (broadcast=%s)", action, broadcast)

    def dispatch_control_message(self, message: amqp.Message) -> bool:
        """Dispatch a control message to the right handler function."""

        self.metrics.health.received_control_messages += 1

        _, is_broadcast = self._control.classify_message(message)

        try:
            action, args = _decode_control_message(message)
            LOG.debug("Received control message: %s", action)

            if action == 'shutdown':
                LOG.critical("Received shutdown control message, stopping worker")
                return True

            if action == 'pause':
                LOG.critical("Received pause message, pausing input consumption")
                self.disable_input()
            elif action == 'resume':
                LOG.critical("Received resume message, resuming input consumption")
                self.enable_input(self._input.queue)
            elif action == 'send_metrics':
                LOG.info("Received request to immediately send metrics")
                self.publish_metrics_if_needed(immediate=True)
            else:
                self.process_control_message(action, args, is_broadcast)

            self.metrics.health.processed_control_messages += 1
        except:  # pylint: disable=bare-except; We don't want an invalid control message to kill the worker.
            LOG.error("Error processing control message", exc_info=True)
            self.metrics.health.failed_control_messages += 1
        finally:
            self._conn.acknowledge_message(message)

        return False

    def publish_metrics_if_needed(self, immediate: bool = False):
        """Check and possibly publish/reset all metrics.

        This method is safe to call periodically and it will check how long
        its been since the last time the metrics were published and do nothing
        if the monitoring interval has not expired.  However, if you want to
        force metric publish right now, you can pass an ``immediate`` bool and
        it will send the metrics immediately.

        The actual atomic operation of publishing current metrics and
        resetting them is deferred to another internal method so that
        subclasses can override it if they need to take special precautions to
        synchronize publishing with whatever is updating the metrics.

        By default, it is assumed that metric updates are happening inside the
        thread executing ``run()`` so no synchronization is necessary.

        Args:
            immediate: Whether to ignore the builtin timer and send the metrics
                right now.
        """

        if not immediate and not self.metrics.should_publish():
            return

        LOG.debug("Reporting monitoring metrics (forced=%s)", immediate)
        self._atomic_publish_and_reset()

    def _start(self):
        """Override this function to provide subclass specific initialization behavior."""

    def _stop(self):
        """Override this function to provide subclass specific cleanup behavior."""

    def _atomic_publish_and_reset(self):
        """Atomically publish all metrics and reset them.

        This method is designed to allow a subclass of BaseLink to synchronize
        the publish and reset operation with a thread that could be updating
        metrics outside of the main thread which will be blocked inside
        ``run()`` whenever this method is called.

        The default implementation does no synchronization.  However, if a
        subclass has a specific need to synchronize its behavior, it can
        override this and do whatever synchronization is necessary.

        Since this method is always called from within the ``run()`` routing,
        the method can assume that no part of ``BaseLink`` is modifying a
        metric while it runs.
        """

        for metric in self.metrics:
            self._conn.publish_metric(self.metrics.worker_id, metric)

        self.metrics.reset_all()


def _decode_control_message(message: amqp.Message) -> Tuple[str, Dict]:
    body = json.loads(message.body)

    action = body.get('action')
    if action is None:
        raise ValueError("Invalid control message missing an action field")

    return action, body


def serialize_amqp_message(message: amqp.Message, file):
    data = {
        "properties": message.properties,
        "body": message.body,
        "delivery_info": message.delivery_info
    }

    pickle.dump(data, file, protocol=4)


def _check_message_selected(_routing_key: str, _input_type: Optional[str]) -> bool:
    # FIXME: Removed internal message section logic
    return True
