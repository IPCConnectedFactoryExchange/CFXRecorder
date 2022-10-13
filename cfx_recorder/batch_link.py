"""Base object for broker links that accumulate messages and operate in a batch."""

import time
import logging
import amqp
from .amqp import BackgroundAMQPConnection
from .base_link import BaseLink

LOG = logging.getLogger(__name__)


class BatchLink(BaseLink):
    """Base class for a generic broker-link output system that processes messages in batches.

    This class handles batching up the input messages and acknowledging them in bulk.  It
    includes two triggers for when a batch is committed:
    - at least N messages are present
    - at most T seconds between commits

    Implementors of specific batching outputs should override the
    ``process_batch`` method with their specific implementation.  It should be
    implemented as a blocking function.
    """

    WAKEUP_TIME = 0.1

    def __init__(self, conn: BackgroundAMQPConnection, *,
                 batch_size: int = 100, batch_timeout: float = 10.0,
                 worker_id: str = None):

        super().__init__(conn, worker_id=worker_id)

        self._batch_size = batch_size
        self._batch_timeout = batch_timeout

        self._last_batch_time = 0.0

        self._in_flight = []

    def _start(self):
        """Override this function to provide subclass specific initialization behavior."""

        # Make sure we don't send a batch immediately on run
        self._last_batch_time = time.monotonic()

    def process_message(self, message: amqp.Message):
        """Subclasses override this method to process messages."""

        self._in_flight.append(message)

        if self._should_process():
            self._process_batch_and_ack()

    def periodic_callback(self):
        """Check if we should commit our batch due to a timeout."""

        if self._should_process():
            self._process_batch_and_ack()

        self.publish_metrics_if_needed()

    def process_batch(self, messages):  # pylint: disable=no-self-use; This is meant to be subclassed and overridden
        """Process a batch of messages synchronously.

        Subclasses should override this method with their own implementation.
        The implementation must be idempotent and succeed or fail for all
        messages in the batch.

        This will always be called with len(messages) > 0 but it may not
        always be called with the given batch size, because there is a maximum
        time between subsequent batches.
        """

        LOG.warning("Dropping %d messages in batch", len(messages))
        self.metrics.health.failed_messages += len(messages)

    def _should_process(self):
        return len(self._in_flight) >= self._batch_size or (self._now - self._last_batch_time) > self._batch_timeout

    def _process_batch_and_ack(self):
        if len(self._in_flight) > 0:
            self.process_batch(self._in_flight)
            self._conn.acknowledge_message(self._in_flight[-1], multiple=True)
            self._in_flight = []

        self._last_batch_time = self._now
