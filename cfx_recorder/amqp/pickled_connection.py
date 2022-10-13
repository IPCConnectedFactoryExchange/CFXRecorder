"""A dummy AMQP connection to use for testing."""

from typing import Optional, Union, List, Dict, Callable
import uuid
import queue
import logging
import gzip
import os
import threading
import pickle
import time
import amqp
from ..exceptions import StaticConfigurationError
from .background_connection import BackgroundAMQPConnection, _PublishMessage, DeliveryConfirmation
from .telegraf import MonitoringMetric

LOG = logging.getLogger(__name__)

class _StopException(Exception):
    """Internal exception raised to cleanly stop the background thread."""


class _PickedReader:
    _current_path: Optional[str]

    def __init__(self, input_path: Union[str, List[str]], queue, prefetch=None, realtime=False, bookmark=False, throttle: Optional[int] = None):
        if isinstance(input_path, str):
            self._remaining_paths = _load_files(input_path, use_bookmark=bookmark)
        else:
            if bookmark is True:
                raise RuntimeError("bookmark=True cannot be combined with a list of input files")

            self._remaining_paths = input_path

        self._file = None
        self._current_path = None
        self._input_path = input_path
        self._queue = queue

        self._realtime = realtime
        self._bookmark = bookmark
        self._prefetch = prefetch


        self._throttle_delay = 0.0
        if throttle is not None:
            if throttle <= 0:
                raise RuntimeError(f"Invalid value specified for throttle: {throttle}")

            self._throttle_delay = 1.0 / throttle
            LOG.info("Throttling output to publish messages at a rate of %d per second", throttle)

        self._thread = None
        self._stop = threading.Event()

        self._outstanding = set()

    def start(self):
        """Open the input file and start reading messages in a background thread."""

        LOG.info("Found %d matching pickle files", len(self._remaining_paths))

        if not self._try_rotate_source_file():
            raise RuntimeError("There were no valid source files for pickled reader")

        self._thread = threading.Thread(target=self._background_read_messages)
        self._thread.start()

    def stop(self):
        """Stop reading messages and kill the background thread cleanly."""

        self._stop.set()
        self._thread.join()

        if self._file is not None:
            self._file.close()

        self._thread = None

    def _try_rotate_source_file(self) -> bool:
        if self._file is not None:
            self._file.close()
            self._file = None

            if self._bookmark:
                bookmark = os.path.basename(self._current_path)
                LOG.info("Updating bookmark with completion of %s", bookmark)
                _update_bookmark(self._input_path, bookmark)

        if len(self._remaining_paths) == 0:
            return False

        next_path = self._remaining_paths.pop(0)
        self._file = gzip.open(next_path, "rb")
        self._current_path = next_path

        LOG.debug("Rotated to next input file %s", next_path)
        return True

    def ack_message(self, delivery_tag: int, multiple: bool = False):
        """Remove outstanding message from in-flight list."""

        if multiple is False:
            self._outstanding.remove(delivery_tag)
            return

        to_remove = []
        for tag in self._outstanding.copy():
            if tag <= delivery_tag:
                to_remove.append(tag)

        for tag in to_remove:
            self._outstanding.remove(tag)

    def _background_read_messages(self):
        if self._file is None:
            LOG.debug("No valid files specified, nothing to do in background reader")
            self._queue.put(None)
            return

        try:
            last_timestamp = None
            last_message_time = time.monotonic()
            count = 0

            while not self._stop.is_set():
                try:
                    serialized_message = pickle.load(self._file)
                    message = _deserialize_amqp_message(serialized_message)
                    timestamp = message.properties["application_headers"]["timestamp_in_ms"] / 1000
                    count += 1
                except EOFError:
                    # Try to go to the next file in our list, otherwise we're done with all files
                    if self._try_rotate_source_file():
                        continue

                    # make sure all messages are processed before exiting.
                    LOG.debug("Finished reading messages (%d total), waiting for processing to finish", count)
                    self._wait_outstanding(1)
                    LOG.debug("Finished waiting for all outstanding messages, closing connection")
                    self._queue.put(None)
                    return

                if self._realtime and last_timestamp is not None:
                    now = time.monotonic()
                    next_message_time = last_message_time + (timestamp - last_timestamp)
                    delay = next_message_time - now

                    if delay > 0:
                        LOG.log(
                            5, "Waiting %.2f seconds before deliverying next message, last_time=%s, time=%s",
                            delay, last_timestamp, timestamp
                        )
                        self._delay_message(timestamp - last_timestamp)

                    last_message_time = now

                if self._throttle_delay > 0:
                    self._delay_message(self._throttle_delay)

                if self._prefetch is not None:
                    self._wait_outstanding(self._prefetch)
                self._outstanding.add(message.delivery_tag)
                self._queue.put(message)
                last_timestamp = timestamp
        except _StopException:
            pass

    def _wait_outstanding(self, count):
        while True:
            if len(self._outstanding) < count:
                return

            time.sleep(0.05)
            if self._stop.is_set():
                raise _StopException()

    def _delay_message(self, delta):
        count = 0
        max_sleep = 0.1
        while count < delta:
            delay = min(max_sleep, (delta - count))
            time.sleep(delay)

            count += delay

            if self._stop.is_set():
                raise _StopException()


class _PickledWriter:
    def __init__(self, output_path: str, confirm_callback: Callable[..., None]):

        self._file = None
        self._output_path = output_path
        self.publish_queue = queue.SimpleQueue()
        self._confirm_callback = confirm_callback

        self._thread = None
        self._stop = threading.Event()

    def start(self):
        """Open the input file and start reading messages in a background thread."""

        self._file = gzip.open(self._output_path, "wb")
        self._thread = threading.Thread(target=self._background_write_messages)
        self._thread.start()

    def stop(self):
        """Stop reading messages and kill the background thread cleanly."""

        self.publish_queue.put(None)
        self._thread.join()

        if self._file is not None:
            self._file.close()

        self._thread = None

    def _background_write_messages(self):
        counter = 1
        if self._file is None:
            LOG.debug("No valid output file specified, nothing to do in background writer")
            return

        while True:
            publish_action = self.publish_queue.get()
            if publish_action is None:
                return

            try:
                delivery_info = {"delivery_tag": counter, "routing_key": publish_action.routing_key}
                counter += 1

                serialized = _serialize_amqp_message(publish_action.message, delivery_info)
                pickle.dump(serialized, self._file)
            except:  #pylint:disable=bare-except;This is a background thread that needs to capture errors for callers
                LOG.error("Could not save serialized message", exc_info=True)

                if publish_action.with_confirm:
                    confirm = DeliveryConfirmation([publish_action.context], failed=True)
                    self._confirm_callback(confirm)
                return

            if publish_action.with_confirm:
                confirm = DeliveryConfirmation([publish_action.context], failed=False)
                self._confirm_callback(confirm)


class PickledAMQPConnection(BackgroundAMQPConnection):
    """A testing class compatible with BackgroundAMQPConnection that loads messages from a file.

    The messages should have been previously serialized to a gzipped pickle
    file and will be read out one at a time and delivered to the ``messages``
    output queue exactly as they were originally queued.

    You can either deliver the messages in "realtime", which means they are
    delayed so that they arrive at the same relative intervals and rate as
    originally published, or you can accelerate time and deliver the messages
    as quickly as possible (simulating a backlog of messages).

    In both cases the prefetch limit is enforced so messages must be
    acknowledged by the amqp consuming code and a maximum of ``prefetch``
    unacknowledged messages will be delivered before blocking.

    For testing purposes, you can inspect they ``mock_queues`` property which
    well as any queues declared.

    Args:
        source_file: The input gzipped pickle file or directory with the messages that should be delivered.
        realtime: Whether to delay messages according to their relative publish times so they
            arrive at the same rate as originally published.
        throttle: Send messages at a fixed rate of this many per second
        bookmark: If given an input directory, tracks which files have been correctly processed and does not
            reprocess them if stopped and restarted.
        prefetch: The maximum number of unacknowledged messages to delivery.  Corresponds to the
            qos setting inside of AMQP 0.9.1 channels.
        output_file: File to save any published output messages.  If specified then messages published will
            be saved in this file.  If left as None then published messages will be dropped.
    """

    mock_queues: Dict[str, dict]

    _writer: Optional[_PickledWriter]
    _reader: Optional[_PickedReader]

    def __init__(self, source_file: Optional[Union[str, List[str]]], output_file: Optional[str] = None,
                 *, realtime: bool = False, prefetch=None, bookmark=False, throttle=None):
        super(PickledAMQPConnection, self).__init__('mock-connection-host')

        self.mock_queues = {}

        if source_file is None and output_file is None:
            raise StaticConfigurationError("You must pass at least one input or output file to PickledAMQPConnection")

        self._reader = None
        if source_file is not None:
            self._reader = _PickedReader(source_file, self.messages, prefetch=prefetch,
                                         realtime=realtime, bookmark=bookmark,
                                         throttle=throttle)

        self._writer = None
        if output_file is not None:
            self._writer = _PickledWriter(output_file, self._event_callback)

    def start(self):
        if self._reader is not None:
            self._reader.start()

        if self._writer is not None:
            self._writer.start()

    def stop(self):
        if self._writer is not None:
            self._writer.stop()

        if self._reader is not None:
            self._reader.stop()
        else:
            self._event_callback(None)

    def bind_temporary_queue(self, queue_name: str, binding_key: Union[str, List[str]], exchange: str = "amq.topic"):
        self.mock_queues[queue_name] = dict(binding_key=binding_key, exchange=exchange)

    def start_consuming(self, queue_name: str, tag: Optional[str] = None) -> str:
        return "ctag-%s" % str(uuid.uuid4())

    def acknowledge_message(self, message_or_delivery_tag: Union[amqp.Message, int], multiple: bool = False):
        """Acknowledge a message delivered to a consumer on this connection.

        This will queue the background thread to send a ``basic_ack`` message
        indicating that the message should be removed from the queue.

        Robustness Note:
            ``basic_ack`` is an asynchronous message that has no response from
            the server so there is not a way to write your application to
            block until the server acknowledges the ack.  You need to assume
            that your application semantics are at-least-once delivery of each
            message and this is your best-effort way of notifying the server
            that you have successfully processed the message but if you get
            unlucky, it may not get to the server, which means the connection
            is dead and you may get the message again on a future connection

        Args:
            message_or_delivery_tag: The consumed message to acknowledge or its delivery tag.
            multiple: Acknowledge all outstanding messages up to and including this one.
        """

        if self._reader is None:
            return

        if isinstance(message_or_delivery_tag, amqp.Message):
            delivery_tag = message_or_delivery_tag.delivery_tag
        else:
            delivery_tag = message_or_delivery_tag

        if delivery_tag is None:
            self._logger.debug("Ignoring acknowledgement for out-of-band message")
            return

        self._reader.ack_message(delivery_tag, multiple)

    def publish_message(self, exchange: str, routing_key: str, message: amqp.Message,
                        *, context: Optional[object] = None, with_confirm: bool = False):

        message.delivery_info = dict(routing_key=routing_key)

        if self._writer is None:
            if with_confirm is True:
                self._event_callback(DeliveryConfirmation([context], failed=False))
            return

        action = _PublishMessage(message, exchange, routing_key, with_confirm, context)
        self._writer.publish_queue.put(action)

    def publish_metric(self, worker_id: str, metric: MonitoringMetric):
        """Convenience function to automatically publish a metric.

        This method does not reset the metric, it just serializes it and
        publishes it over AMQP without confirmation.

        Args:
            worker_id: The id of the worker sending this metric so that we generate the
                correct routing key.
            metric: The metric that should be serialized and reported.
        """

        LOG.info('Monitoring Metric Published: %s\n\n%s\n', worker_id, metric.serialize_string())
        super(PickledAMQPConnection, self).publish_metric(worker_id, metric)


def _deserialize_amqp_message(serialized: dict) -> amqp.Message:
    body = serialized.get('body')
    properties = serialized.get('properties')
    delivery_info = serialized.get('delivery_info')

    message = amqp.Message(body, **properties)
    message.delivery_info = delivery_info
    return message


def _serialize_amqp_message(message: amqp.Message, delivery_info=None) -> dict:
    if 'application_headers' not in message.properties:
        message.properties['application_headers'] = {}

    if message.properties['application_headers'].get('timestamp_in_ms') is None:
        now = time.time()
        message.properties['application_headers']['timestamp_in_ms'] = int(now * 1000)
        message.properties['application_headers']['timestamp'] = int(now)

    if delivery_info is None:
        delivery_info = message.delivery_info

    data = {
        "properties": message.properties,
        "body": message.body,
        "delivery_info": delivery_info
    }

    return data


def _load_files(input_path: str, use_bookmark=False) -> List[str]:
    """Load either a single or many files."""

    if os.path.isfile(input_path):
        return [input_path]

    if os.path.isdir(input_path):
        files = []

        bookmark = None
        if use_bookmark:
            bookmark = _load_bookmark(input_path)
            LOG.info("Found bookmark, starting after filename %s", bookmark)

        skip_count = 0
        for filename in sorted(os.listdir(input_path)):
            if not filename.endswith('.pickle.gz'):
                continue

            if bookmark is not None and filename <= bookmark:
                skip_count += 1
                continue

            files.append(os.path.join(input_path, filename))

        if use_bookmark:
            LOG.info("Skipped %d files due to bookmark", skip_count)

        return files

    raise RuntimeError(f"Path is not a file or directory: {input_path}")


def _load_bookmark(folder: str) -> Optional[str]:
    bookmark_file = os.path.join(folder, '_bookmark.txt')

    if not os.path.exists(bookmark_file):
        return None

    try:
        with open(bookmark_file, "r") as infile:
            bookmark = infile.read().strip()

        return bookmark
    except:
        LOG.warning("Could not load corrupt bookmark file: %s", bookmark_file, exc_info=True)
        return None


def _update_bookmark(folder: str, last_file: str):
    if not os.path.isdir(folder):
        LOG.warning("bookmark=true passed without giving a directory, this does not nothing")
        return

    bookmark_file = os.path.join(folder, '_bookmark.txt')

    with open(bookmark_file, "w") as outfile:
        outfile.write(last_file)
