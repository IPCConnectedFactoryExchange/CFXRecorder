"""A managed AMQP 0.9.1 connection that can safely consume and produce messages asynchronously.

This is a tricky thing to do safely and also with the potential for high
performance, especially in connections with moderate tcp latency such as
having a consumer or producer running on a different machine than the rabbitmq
broker.

The BackgroundAMQPConnection class is designed to be both safe and performant
with a simple API designed to allow building worker processes that need to
consume messages at high speed and then republish them somewhere else with
at-least-once semantics in the face of any kind of error.


Concurrency Design
------------------

The basic design of the class has a single background thread that synchronizes
all activity on the AMQP connection.  Actions are performed by calling methods
on the class instance that queue the action for the background thread and then
either return immediately (for high performance actions like publishing) or
block until the action has been completed (for infrequent actions like registering
a queue).  All actions can be queued from any thread without coordination since they
are synchronized through a single background thread::

    BackgroundAMQPConnection

    primary thread(s)                   background_thread

    bind_temporary_queue (SYNC)
        - _BindQueue action queued
        - blocked on shared future
                                         - bind action pulled from queue
                                           and performed
                                         - result set in future object
        - future unblocked and returns


    publish_message (ASYNC)
        - _PublishMessage action queued
        - returns immediately

                                        - publish action pulled from queue
                                        - message published

                                        [if with_confirm is set]
                                        - confirmation pulled from connection
                                        - DeliveryConfirmation object added to
                                          output messages queue


BackgroundAMQPConnection can be used in one of two ways:
 - Queuing: All events are automatically queued in a single queue for polling()
 - Callbacks: All events are immediately dispatched to a single callback


Queueing
--------

Generally speaking, queuing is easier for simple use cases where a single
application is directly managing the connection:

There is a single centralized `messages`` queue that contains any events that
the user's code may need to react to (including the connection being
terminated).  This allows the user to design their application as a loop that
performs a blocking ``get()`` on this queue as the main driver of all action.

Based on the event, the user's code can then react to either:
 - a new message being received from a consumer
 - a delivery confirmation being received from a published message
 - the connection being terminated


Callbacks
---------

Callbacks are an advanced use case where you want to distinguish between
events as they happen and perhaps route them to different queues for multiple
subsystems of your application.  In this case, the ``messages`` queue will
never be filled and you need to pass a callback into the connection
constructor.  This callback will be called from the background thread whenever
an event happens and it **must not block or take an action directly on the
connection** and instead deal with the event in whatever way it wants such
as queuing to an appropriate queue based on what kind of event it is.


Implementation Notes
--------------------

1. The original implementation of this class included multiple channels so
   that a single connection could be used for both consuming and publishing on
   a separate channel on the same connection.  However, it seems that py-amqp
   is not stable in that configuration.  It would have race conditions and
   framing errors even though it was only being used on a single thread.
   Resolving this led to separating publish and consume operations on separate
   connections as is recommended by rabbitmq as a best practice.
"""

from typing import Optional, Union, Callable, List, Generator
from threading import Thread
import logging
import socket
import time
from collections import OrderedDict
from dataclasses import dataclass
from concurrent.futures import Future
from queue import SimpleQueue, Empty
import amqp
from .telegraf import MonitoringMetric


@dataclass
class _AcknowledgeMessage:
    """Internal action object to represent a basic_ack call."""

    __slots__ = ['delivery_tag', 'multiple']

    delivery_tag: int
    multiple: bool


@dataclass
class _StartConsuming:
    """Internal action object to represent a basic_consume call."""

    __slots__ = ['queue', 'tag']

    queue: str
    tag: Optional[str]


@dataclass
class _BindQueue:
    """Internal action object to represent a register_queue then bind_queue call."""

    __slots__ = ['queue', 'binding_key', 'exchange']

    queue: str
    binding_key: Union[str, List[str]]  # Support multiple binding keys
    exchange: str


@dataclass
class _PublishMessage:
    """Internal action object to represent a request to publish a message."""

    __slots__ = ['message', 'exchange', 'routing_key', 'with_confirm', 'context']

    message: amqp.Message
    exchange: str
    routing_key: str
    with_confirm: bool
    context: Optional[object]


@dataclass
class DeliveryConfirmation:
    """Object put into output queue indicating that a published message was confirmed or nacked.

    The ``message_contexts`` array will contain on entry for every message
    that is being acknowledged. The value will be whatever context object was
    passed during the call to ``publish_message``. Users should put an object
    into ``context`` that gives them whatever information they need when the
    delivery confirmation comes.
    """

    __slots__ = ['message_contexts', 'failed']

    message_contexts: list
    failed: bool


# Turn off nuisance log messages every second at debug level saying we responded to a heartbeat
logging.getLogger('amqp.connection.Connection.heartbeat_tick').setLevel(logging.INFO)


class BackgroundAMQPConnection:
    """A background AMQP connection designed for high performance producer/consumer applications.

    This class should be considered experimental and not yet ready for
    production use but is designed to test out high speed amqp consumers that
    have many messages in flight at the same time.
    """

    POLL_TIMEOUT = 0.1
    """Socket read timeout to check for locally queued actions if there is no network traffic.

    100ms (0.1 seconds) is a good default value that balances responsiveness
    to local commands while not increasing CPU usage excessively just to
    wakeup and check for local actions.
    """

    CONNECT_TIMEOUT = 5
    """Maximum time (in seconds) to wait for a new connection to be opened in start()."""

    MAX_PUBLISH_BACKLOG = 1000
    """Default maximum queue size before actions like publish() start to be throttled.

    Normally publish_message queues the message to publish and returns
    immediately. However, in certain use cases, this can cause the action
    queue to grow without bound.  There is a default size limit of 1000
    imposed.  When the queue is at capacity, future actions will be throttled
    and enter a check_size/wait backoff loop before queuing.

    This behavior is necessary to allow the use of SimpleQueue() which is an optimized
    C-implemented queue in the python 3.7+ standard library.
    """

    _IGNORE_CONFIRM = object()
    """Internal sentinel value to indicate messages that we do not wish to confirm.

    The entire publish channel is set using confirm_select(), so choosing not to
    confirm a given message just means that when its confirmation is received, it
    will not result in a ``DeliveryConfirmation`` object.
    """

    def __init__(self, host: str, port: int = 5672, username: Optional[str] = None,
                 password: Optional[str] = None, *,
                 heartbeat: int = 10, max_messages: int = 100, callback: Optional[Callable] = None,
                 max_backlog: int = MAX_PUBLISH_BACKLOG):
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._heartbeat = heartbeat
        self._max_messages = max_messages

        self._next_message_tag = 1

        self._thread = None
        self._logger = logging.getLogger(__name__)
        self.messages: SimpleQueue[amqp.Message] = SimpleQueue()
        self._actions: SimpleQueue[amqp.Message] = SimpleQueue()
        self._unconfirmed_publish = OrderedDict()
        self._max_action_backlog = max_backlog

        if callback is None:
            callback = self._queue_message_threaded

        self._event_callback = callback

    def start(self):
        """Connect to the amqp 0.9.1 server and configure for use.

        This is a synchronous method that will block until the connection is
        established.  It uses a background thread to synchronize all activity
        on the connection.
        """

        future = Future()

        self._thread = Thread(target=self._manage_connection_threaded, args=(future,))
        self._thread.start()

        # Block, either raises the exception from management thread or waits until we're connected
        try:
            future.result()
        except:
            self._thread.join()
            self._thread = None
            raise

    def stop(self):
        """Stop the connection and any consumers.

        This method will block until the background thread is shutdown.
        """

        if self._thread is None:
            self._logger.error("Stop called with no active thread")
            return

        self._actions.put(None)
        self._thread.join()
        self._thread = None

    def bind_temporary_queue(self, queue_name: str, binding_key: Union[str, List[str]], exchange: str = "amq.topic"):
        """Create a transient exclusive queue for temporary consumption.

        This will do two things:
          - it will declare a queue with the given name, set as exclusive so that it dies when this connection
            is stopped
          - it will bind that queue to the given exchange with a binding key.

        Both actions will have been completed by the time this method returns.

        Args:
            queue_name: Name of the queue to create
            binding_key: Binding key to attach the created queue to an exchange
            exchange: Name of the exchange to bind the queue to
        """

        action = _BindQueue(queue_name, binding_key, exchange)
        self._perform_action(action, block=True)

    def start_consuming(self, queue_name: str, tag: Optional[str] = None) -> str:
        """Start consuming from the named queue, which should already exist on the server.

        The consumer is always started without automatic acknowledgement,
        which means that you must acknowledge the messages via a call to
        ``acknowledge_message(message_or_delivery_tag)``.

        Args:
            queue_name: The name of the queue to consume from
            tag: Optional tag to name this consumer.  Autogenerated if not passed.

        Returns:
            The tag of the consumer that was started.
        """

        action = _StartConsuming(queue_name, tag)
        return self._perform_action(action, block=True)

    def stop_consuming(self, tag: str):  #pylint:disable=no-self-use;This is a placeholder method.
        """Stop a previously started consumer."""

        raise RuntimeError("Stop consuming is not yet implemented")

    def iter_events(self, check_interval=0.1) -> Generator[Union[amqp.Message, DeliveryConfirmation, None], None, None]:
        """Iterate forever over events, waiting for the next one.

        This method uses an internal timeout to block until a message is available
        while still allowing a Ctrl-C event to abort the wait.

        Returns:
            The next message received.
        """

        while True:
            try:
                yield self.messages.get(timeout=check_interval)
            except Empty:
                pass

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

        if not isinstance(delivery_tag, int):
            raise ValueError("Unknown delivery tag that was not an integer: %s" % delivery_tag)

        action = _AcknowledgeMessage(delivery_tag, multiple)
        self._logger.log(5, "Queuing ack for message %s, multiple=%s", delivery_tag, multiple)
        self._perform_action(action, block=False)

    def publish_message(self, exchange: str, routing_key: str, message: amqp.Message,
                        *, context: Optional[object] = None, with_confirm: bool = False):
        """Queue a message for publishing.

        If ``with_confirm = True``, then the message will be sent with delivery confirmation
        and a DeliveryConfirmation object will be put into the ``messages`` queue when the
        message has been confirmed by the broker.  Otherwise, no confirmation will be returned.

        If a confirmation is returned, it will contain an ``integer`` delivery tag to identify
        which message is being confirmed.  The ``delivery_tag`` corresponding to this message
        will be returned by this method.

        .. note:

            The internal delivery tag for the published message is never exposed directly to
            the user of this method.  You can instead pass whatever context object you want
            that will be included in the returned ``DeliveryConfirmation`` and that context
            needs to contain whatever information your application needs in order to correctly
            process the confirmation.

        Args:
            exchange: The exchange to receive the message
            routing_key: The routing key to publish the message on
            context: An arbitrary object that will be included in the delivery
                confirmation for this message, if with_confirm is True.
            message: The message that should be published.
            with_confirm: Whether we should listen for a delivery confirmation from this message.
        """

        action = _PublishMessage(message, exchange, routing_key,
                                 with_confirm=with_confirm, context=context)
        self._logger.log(5, "Queueing publish with context %s", context)
        self._perform_action(action, block=False)

    def publish_metric(self, worker_id: str, metric: MonitoringMetric):
        """Convenience function to automatically publish a metric.

        This method does not reset the metric, it just serializes it and
        publishes it over AMQP without confirmation.

        Args:
            worker_id: The id of the worker sending this metric so that we generate the
                correct routing key.
            metric: The metric that should be serialized and reported.
        """

        self.publish_message('amq.topic', metric.routing_key(worker_id), metric.serialize_amqp(),
                             with_confirm=False)

    def _perform_action(self, action, block=False, timeout=5.0):
        future = None
        if block:
            future = Future()

        start_time = time.monotonic()
        while self._actions.qsize() > self._max_action_backlog:
            time.sleep(0.1)
            if time.monotonic() - start_time > timeout:
                raise RuntimeError("Could not queue action in specified timeout, queue too large")

        self._actions.put((action, future))

        if future is not None:
            return future.result(timeout=timeout)

        return None

    def _queue_message_threaded(self, message):
        self.messages.put(message)

    def _on_consumer_receive(self, message):
        self._logger.log(5, "Received message with tag %d from consumer", message.delivery_tag)
        self._event_callback(message)

    def _on_message_ack(self, delivery_tag, multiple):
        messages = self._collect_messages(delivery_tag, multiple)
        if len(messages) > 0:
            confirm = DeliveryConfirmation(messages, False)
            self._logger.log(5, "Received ack for %d, multiple=%s (%d messages)", delivery_tag,
                             multiple, len(messages))

            self._event_callback(confirm)

    def _on_message_nack(self, delivery_tag, multiple):
        messages = self._collect_messages(delivery_tag, multiple)
        if len(messages) > 0:
            confirm = DeliveryConfirmation(messages, True)

            self._logger.warning("Received NACK for %d, multiple=%s", delivery_tag, multiple)
            self._event_callback(confirm)

    def _collect_messages(self, last_tag, multiple):
        messages = []
        if multiple:
            first_tag = next(iter(self._unconfirmed_publish))

            for tag in range(first_tag, last_tag + 1):
                context = self._unconfirmed_publish.pop(tag, None)
                if context is None:
                    continue  # See #677

                if context is not self._IGNORE_CONFIRM:
                    messages.append(context)
                else:
                    self._logger.log(5, "delivery tag confirmation %d was ignored because with_confirm=False", tag)
        else:
            context = self._unconfirmed_publish.pop(last_tag)
            if context is not self._IGNORE_CONFIRM:
                messages.append(context)
            else:
                self._logger.log(5, "delivery tag confirmation %d was ignored because with_confirm=False", last_tag)

        return messages

    def _manage_connection_threaded(self, connected_future):
        """Background thread routine that manages the amqp connection."""

        try:
            host_string = "%s:%s" % (self._host, self._port)

            self._logger.debug("Beginning connection to server %s", host_string)

            conn = amqp.Connection(host_string, self._username, self._password,
                                   login_method='PLAIN', heartbeat=self._heartbeat,
                                   connect_timeout=self.CONNECT_TIMEOUT)

            conn.connect()
        except Exception as err:  #pylint:disable=broad-except;This is a background thread and we're logging.
            self._logger.error("Count not open connection to %s", host_string, exc_info=True)
            connected_future.set_exception(err)
            return

        self._logger.info("Successfully connected to host %s with prefetch=%s", host_string, self._max_messages)
        connected_future.set_result(None)

        connection_open = True
        try:
            last_heartbeat = time.monotonic()

            channel = conn.channel()
            channel.basic_qos(0, self._max_messages, True)  # True means that this applies to all consumers.

            channel.confirm_select()  # Make sure we get publish confirmations
            channel.events['basic_ack'].add(self._on_message_ack)
            channel.events['basic_nack'].add(self._on_message_nack)

            while True:
                try:
                    conn.drain_events(self.POLL_TIMEOUT)
                except socket.timeout:
                    # pyamqp does not catch or block socket timeouts, which are expected when
                    # there is no traffic on the connection.
                    pass
                except (socket.error, amqp.exceptions.ConnectionForced) as err:
                    self._logger.error("Connection closed from remote side: %s", str(err))
                    connection_open = False
                    break

                now = time.monotonic()

                if (now  - last_heartbeat) > 1.0:
                    conn.heartbeat_tick()
                    last_heartbeat = now

                if self._process_actions_threaded(channel):
                    connection_open = False
                    self._logger.debug("Stop received, shutting down connection")
                    break
        except Exception as err:  #pylint:disable=broad-except;This is a background thread and we're logging.
            self._logger.error("Error managing connection, stopping", exc_info=True)
        finally:
            try:
                if connection_open:
                    conn.close()
            except:  #pylint:disable=bare-except;We're in a different exception handler and logging.s
                self._logger.warning("Error closing connection after another error", exc_info=True)

            # Always tell whoever is listening to messages that we have closed the connection
            self._event_callback(None)

    def _process_actions_threaded(self, channel: amqp.Channel) -> bool:
        """Process any pending actions in the background thread.

        Note:
            To avoid a situation where we consume new actions slower than they are
            being added and never finish this method, it captures the number of
            actions at start so that it only processes a fixed maximum number per call
            before returning.

        TODO:
            1. Implement ack coelescing so that if we have a run of multiple
               _AcknowledgeMessage actions that are in order with no gaps, we send
               a single multiple_ack for all of them.

        Returns:
            Whether the connection is finished and we should stop the background thread.
        """

        to_process = self._actions.qsize()
        i = 0

        # Group actions together so that we are able to coelesce acknowledgements
        actions = []
        while i < to_process:
            try:
                action_obj = self._actions.get_nowait()
            except Empty:
                break

            actions.append(action_obj)

            i += 1

        for action_obj in actions:
            if action_obj is None:
                return True

            action, future = action_obj
            result = None

            try:
                self._logger.log(5, "Starting action %s", action)

                if isinstance(action, _StartConsuming):
                    result = self._start_consuming_threaded(channel, action)
                elif isinstance(action, _BindQueue):
                    self._bind_queue_threaded(channel, action)
                elif isinstance(action, _AcknowledgeMessage):
                    self._acknowledge_message_threaded(channel, action)
                elif isinstance(action, _PublishMessage):
                    self._publish_message_threaded(channel, action)
                else:
                    raise ValueError("Unknown action: %r" % action)

                if future is not None:
                    future.set_result(result)
            except (amqp.IrrecoverableConnectionError, amqp.IrrecoverableChannelError, ConnectionError) as err:
                if future is not None:
                    future.set_exception(err)

                self._logger.error("Stopping connection because of irrecoverable error", exc_info=True)
                return True
            except (amqp.exceptions.ConnectionForced, socket.error) as err:
                if future is not None:
                    future.set_exception(err)

                self._logger.error("Connection closed from remote side: %s", str(err))
                return True
            except Exception as err:  #pylint:disable=broad-except;We need to return this exception to the caller in a future
                if future is not None:
                    future.set_exception(err)
                else:
                    self._logger.error("Error taking action %r", action, exc_info=True)

        return False

    def _start_consuming_threaded(self, channel: amqp.Channel, action: _StartConsuming) -> str:
        tag = action.tag
        if tag is None:
            tag = ''

        tag = channel.basic_consume(action.queue, tag, callback=self._on_consumer_receive)
        self._logger.info("Started consumer (tag=%s) on queue %s", tag, action.queue)

        return tag

    def _publish_message_threaded(self, channel: amqp.Channel, action: _PublishMessage):
        delivery_tag = self._next_message_tag
        self._next_message_tag += 1

        channel.basic_publish(action.message, exchange=action.exchange, routing_key=action.routing_key,
                              immediate=False, mandatory=False)
        self._logger.log(5, "Published message to %s:%s delivery_tag=%s", action.exchange,
                         action.routing_key, delivery_tag)

        if action.with_confirm:
            self._unconfirmed_publish[delivery_tag] = action.context
        else:
            self._unconfirmed_publish[delivery_tag] = self._IGNORE_CONFIRM

    def _bind_queue_threaded(self, channel: amqp.Channel, action: _BindQueue) -> None:
        channel.queue_declare(action.queue, exclusive=True)

        if action.binding_key is None:
            bindings = []
        elif isinstance(action.binding_key, str):
            bindings = [action.binding_key]
        else:
            bindings = action.binding_key

        for binding in bindings:
            channel.queue_bind(action.queue, action.exchange, binding)
            self._logger.info("Bound transient queue %s to exchange %s with key %s",
                              action.queue, action.exchange, binding)

    def _acknowledge_message_threaded(self, channel: amqp.Channel, action: _AcknowledgeMessage) -> None:
        channel.basic_ack(action.delivery_tag, action.multiple)
        self._logger.log(5, "Acknowledged message with tag %d, multiple=%s", action.delivery_tag, action.multiple)
