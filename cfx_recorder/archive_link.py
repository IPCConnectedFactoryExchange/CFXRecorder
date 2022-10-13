"""
A broker-link that archives data into compressed, serialized files.
"""

import argparse
import dataclasses
import gzip
import logging
import os
import time

from threading import Lock

from .amqp import BackgroundAMQPConnection

from .base_link import serialize_amqp_message
from .batch_link import BatchLink
from .exceptions import StaticConfigurationError
from .time import parse_duration


LOG = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class ArchiveLinkConfig:
    """Configuration for the ArchiveLink."""

    export_path: str
    """Export path for the archive files. {ts} will be formatted in-place."""

    archive_interval: int = 3600
    """The number of seconds an archive file should be open for, before opening the next one."""

    archive_min_size: int = 1000
    """Minimal number of messages within a single archive. Ignored for the final message."""

    create_dirs: bool = True
    """Whether or not the link is allowed to create missing subdirectories for the export path."""


def _find_files(source_path):
    """Looks for new files in the source folder, and updates the queue."""
    outfiles = []
    filelist = [fname for fname in os.listdir(source_path) if ".inprogress" not in fname]
    for filename in filelist:
        full_filename = os.path.join(source_path, filename)
        LOG.info("Found new file %s", full_filename)
        try:
            _, timestamp, pickle, gzip = filename.split(".")
            if pickle != "pickle" or gzip != "gz":
                raise ValueError()
        except ValueError:
            LOG.warning("File %s doesn't conform to {region}.{ts}.pickle.gz . Skipping.", filename)
            continue
        outfiles.append((full_filename, timestamp))
    return outfiles


class ArchiveLink(BatchLink):
    """This link archives messages into pickle.gz files.

    Optionally, it can also upload these files to an AWS S3 bucket.
    """

    def __init__(
            self,
            conn: BackgroundAMQPConnection,
            config: ArchiveLinkConfig,
            *,
            worker_id: str = None,
            batch_size: int = 10000,
            batch_timeout: float = 10.0
        ):
        super().__init__(
            conn,
            batch_size=batch_size,
            batch_timeout=batch_timeout,
            worker_id=worker_id
        )
        self._config = config

        self.metrics.health.link_processor = "archive"
        self.metrics_lock = Lock()

        self._current_path = None

        self._file = None
        self._current_counter = 0
        self._total_counter = 0

        self._next_rotation = None

    def _create_file_path(self):
        return self._config.export_path.format(ts=int(time.time())) + ".inprogress"

    def _start(self):
        """Starts up the ArchiveLink.

        - Opens a new compressed file for writing, creating subdirectories if needed
        - Sets the next rotation time for the interval-based archive rotation + upload
        - Starts the S3Uploader if needed
        """
        super()._start()
        try:
            path = self._create_file_path()  # Make sure that the file path can be formatted correctly
            self._current_path = path
        except:
            raise StaticConfigurationError(
                f"Invalid export path with formatting error: {self._config.export_path}"
            )

        if self._config.create_dirs:
            # Recursively create subdirectories
            os.makedirs(os.path.split(path)[0], exist_ok=True)

        self._file = gzip.open(path, "wb")

        self._next_rotation = time.monotonic() + self._config.archive_interval

        LOG.info("Started ArchiveLink. Initial archive file is %s", path)

    def _stop(self):
        """Stops and cleans up the ArchiveLink.

        - Closes the file currently open for writing
        - Triggers the S3 processing if needed, in a synchronous manner.
        """
        if self._file is not None:
            self._file.close()
            os.rename(self._current_path, self._current_path.replace(".inprogress", ""))
        LOG.info("Stopped ArchiveLink.")
        super(ArchiveLink, self)._stop()


    def _rotate_archive(self):
        """Closes the current file, and opens a new one for processing."""
        if self._current_counter < self._config.archive_min_size:
            LOG.info("Skipping rotation, currently at %d messages.", self._current_counter)
            return
        if self._file is not None:
            self._file.close()
        os.rename(self._current_path, self._current_path.replace(".inprogress", ""))
        path = self._create_file_path()
        self._file = gzip.open(path, "wb")
        self._current_path = path

        LOG.info(
            "Wrote %d messages to disk, total %d. Setting archive file to %s",
            self._current_counter,
            self._total_counter, path
        )
        self._current_counter = 0

    def periodic_callback(self):
        """Called every second. Processes rotation and upload if needed.
        """
        super(ArchiveLink, self).periodic_callback()
        now = time.monotonic()
        if now > self._next_rotation:
            self._next_rotation = now + self._config.archive_interval
            self._rotate_archive()

    def process_batch(self, messages):
        """Process a batch of messages.

        This writes all the messages to disk,
        using fsync and flush to ensure the data is safely written.
        """
        begin = time.monotonic()
        LOG.debug("Processing %d messages", len(messages))
        for message in messages:
            serialize_amqp_message(message, self._file)
        self._file.flush()
        os.fsync(self._file)
        self._current_counter += len(messages)
        self._total_counter += len(messages)
        LOG.debug("Flushed %d messages in %0.2fs", len(messages), time.monotonic() - begin)

    def _atomic_publish_and_reset(self):
        """Overrides the base atomic_publish_and_reset function.

        We have to override the function, because our background thread updates metrics, and
        as such, there's a race condition between both.
        """
        with self.metrics_lock:
            super(ArchiveLink, self)._atomic_publish_and_reset()

    @classmethod
    def CreateFromArgs(cls, conn: BackgroundAMQPConnection, args: argparse.Namespace) -> 'ArchiveLink':
        """Create an ArchiveLink from command line arguments.

        The argument parser should have been created by a previous call to
        BuildArgParser to ensure that the correct parameters exist in the
        namespace.
        """
        config = ArchiveLinkConfig(
            args.export_path,
            args.archive_interval.total_seconds(),
            args.archive_min_size,
            not args.dont_create_dirs
        )

        link = cls(
            conn,
            config,
            batch_size=args.batch_size,
            batch_timeout=args.batch_timeout.total_seconds(),
            worker_id=args.worker_id,
        )
        link.adjust_monitoring(args.monitor)
        return link

    @classmethod
    def BuildArgParser(cls, subparsers):
        """Build an argument parser for the ArchiveLink."""

        parser = subparsers.add_parser('archive', help="Archives messages to pickle.gz files.")

        parser.add_argument(
            "--export-path",
            default="./archive/noregion.{ts}.pickle.gz",
            help="Export path for the archive files. {ts} will be formatted in-place."
        )
        parser.add_argument(
            "--archive-interval",
            default="1h",
            type=parse_duration,
            help="Time in seconds between archive file rotation."
        )
        parser.add_argument(
            "--archive-min-size",
            default=-1,
            type=int,
            help="Minimum number of messages written in a single file when rotating. Doesn't apply to the final batch."
        )
        parser.add_argument(
            "--dont-create-dirs",
            action="store_true",
            help="Do not automatically create subdirectories if not present."
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=10000,
            help="Maximum number of messages between disk writes are fsynced and the GZIP buffer is flushed."
        )
        parser.add_argument(
            "--batch-timeout",
            type=parse_duration,
            default="10s",
            help="Maximum time between disk writes are fsynced and the GZIP buffer is flushed."
        )

        return parser
