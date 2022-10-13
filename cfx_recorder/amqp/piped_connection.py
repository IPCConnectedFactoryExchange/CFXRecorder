"""A dummy AMQP connection to use for testing."""

from typing import Optional, Union, Dict, List
import uuid
import logging
import threading
from concurrent.futures import Future
import queue
import json
import amqp
from .background_connection import (
    BackgroundAMQPConnection, _PublishMessage, DeliveryConfirmation
)

LOG = logging.getLogger(__name__)

class PipedAMQPConnection(BackgroundAMQPConnection):
    """An in memory piped AMQP connection for testing purposes."""

    _outstanding: Dict[int, Future]

    def __init__(self):
        super(PipedAMQPConnection, self).__init__('mock-connection-host')

        self.published = queue.SimpleQueue()
        self.registered_queues = {}

        self._next_tag = 1
        self._state_lock = threading.Lock()
        self._outstanding = {}

    def start(self):
        pass

    def stop(self):
        pass

    def bind_temporary_queue(self, queue_name: str, binding_key: Union[str, List[str]], exchange: str = "amq.topic"):
        self.registered_queues[queue_name] = dict(binding_key=binding_key, exchange=exchange)

    def start_consuming(self, queue_name: str, tag: Optional[str] = None) -> str:
        if queue_name not in self.registered_queues:
            raise RuntimeError(f"Attempted to consume from a queue {queue_name} that was not registered")

        if tag is None:
            tag = "ctag-%s" % str(uuid.uuid4())

        return tag

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

        if isinstance(message_or_delivery_tag, amqp.Message):
            delivery_tag = message_or_delivery_tag.delivery_tag
        else:
            delivery_tag = message_or_delivery_tag

        if delivery_tag is None:
            self._logger.debug("Ignoring acknowledgement for out-of-band message")
            return

        if multiple is False:
            with self._state_lock:
                waiter = self._outstanding.pop(delivery_tag)
                _try_notify_waiter(waiter)
                return

        to_remove = []
        with self._state_lock:
            for tag in self._outstanding:
                if tag <= delivery_tag:
                    to_remove.append(tag)

            for tag in to_remove:
                waiter = self._outstanding.pop(tag)
                _try_notify_waiter(waiter)

    def publish_message(self, exchange: str, routing_key: str, message: amqp.Message,
                        *, context: Optional[object] = None, with_confirm: bool = False):

        message.delivery_info = dict(routing_key=routing_key)

        action = _PublishMessage(message, exchange, routing_key, with_confirm, context)
        self.published.put(action)

        if with_confirm:
            delivery = DeliveryConfirmation([context], False)
            self.messages.put(delivery)

    def inject_control(self, worker_id: str, action: str, args: Optional[dict] = None, broadcast=False):
        """Publish a mock control message."""

        if broadcast is True:
            worker_id = "$broadcast"

        topic = f"worker.{worker_id}.control"

        message_dict = dict(action=action)
        if args is not None:
            message_dict.update(args)

        return self.inject_message(topic, message_dict)

    def wait_control(self, worker_id: str, action: str, args: Optional[dict] = None,
                     broadcast=False, *, timeout=1.0) -> List[amqp.Message]:
        """Send a control message and wait for it to be acknowledged.

        Returns:
            All of the messages that were published before the control message was acknowledged.
        """

        done = self.inject_control(worker_id, action, args, broadcast=broadcast)
        done.result(timeout=timeout)

        return self.pop_published()

    def inject_message(self, routing_key, message: Union[Dict, amqp.Message]) -> Future:
        """Publish a mock json message."""

        done = Future()
        with self._state_lock:
            tag = self._next_tag
            self._next_tag += 1
            self._outstanding[tag] = done

        if not isinstance(message, amqp.Message):
            message = amqp.Message(json.dumps(message))

        message.delivery_info = dict(routing_key=routing_key, delivery_tag=tag)
        self.messages.put(message)

        return done

    def wait_message(self, routing_key: str, message: Union[Dict, amqp.Message],
                     timeout: float = 1.0) -> List[amqp.Message]:
        """Publish a message into the connection and wait for it to be acknowledged.

        Returns:
            All of the messages that were published before the message was acknowledged.
        """

        done = self.inject_message(routing_key, message)
        done.result(timeout=timeout)

        return self.pop_published()

    def close(self) -> List[amqp.Message]:
        """Close this amqp connection without waiting for acked messages."""

        self.messages.put(None)

        return self.pop_published()

    def close_and_wait(self, timeout=1.0) -> List[amqp.Message]:
        """Close this AMQP connection and wait for all outstanding messages to be acked."""

        while True:
            with self._state_lock:
                if len(self._outstanding) == 0:
                    break

                next_waiter = next(iter(self._outstanding.values()))

            next_waiter.result(timeout=timeout)

        self.messages.put(None)
        return self.pop_published()

    def pop_published(self, limit=None):
        """Pop and return all or a fixed number of published messages."""

        messages = []
        while self.published.qsize() >= 1:
            message = self.published.get_nowait()
            messages.append(message)

            if limit is not None and len(messages) >= limit:
                break

        return messages

    def clear_published(self) -> int:
        """Clear any existing published messages.

        Returns:
            The number of messages cleared.
        """

        messages = self.pop_published()
        return len(messages)


def _try_notify_waiter(waiter: Optional[Future]):
    if waiter is None:
        return

    waiter.set_result(None)
