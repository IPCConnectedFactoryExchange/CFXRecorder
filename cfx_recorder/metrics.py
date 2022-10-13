"""Telegraf metrics to be used to monitor running broker link workers in production."""

import dataclasses
from .amqp import MonitoringMetric, measurement_field, tag_field, noreset_field


@dataclasses.dataclass
class BrokerLinkHealth(MonitoringMetric):
    """Basic metric tracking the health of a generic broker-link worker.

    The worker type and actual processor are tags so that metrics can be
    easily aggregrated.  The goal of this metric to allow for stall and
    anomaly detection to be performed on each broker-link.
    """

    measurement: str = measurement_field("broker_link_health")

    link_processor: str = tag_field(default=None)
    """The specific name of the broker-link processor that is running."""

    received_messages: int = 0
    """How many messages have been received.

    This counter is incremented early in the processing of any messages to
    help detect stalls or other blocks that cause messages to be received
    but neither processed nor failed.
    """

    processed_messages: int = 0
    """How many messages have been successfully processed."""

    failed_messages: int = 0
    """How many messages have been dropped because processing failed.

    If a broker-link implementation includes retry processing, then this
    means that all retries have failed and the message has been dropped.
    """

    fixed_timestamps: int = 0
    """The total number of points whose timestamps were far in the past and fixed.

    This can happen when, for example, Access Points don't have valid clocks set and
    send telegraf data that is timestamped in 1970.  We automatically fix this by
    using the timestamp of when the broker received the message instead.
    """

    received_control_messages: int = 0
    """How many control messages have been received.

    This counter is incremented early in the processing of a control message.
    It increments both for broadcast controls and directed controls.
    """

    processed_control_messages: int = 0
    """How many control messages have been successfully processed."""

    failed_control_messages: int = 0
    """How many control messages have failed during processing."""


@dataclasses.dataclass
class InfluxDBWriteMetric(MonitoringMetric):
    """Health metric on writes to influxdb."""

    measurement: str = measurement_field("influxdb_batch_write")

    host: str = tag_field(default=None)
    """The name of the host that we were writing to."""

    database: str = tag_field(default=None)
    """The name of the database we were writing to."""

    min_batch_size: int = None
    """The smallest number of points we wrote in a singe write."""

    max_batch_size: int = None
    """The largest number of points we wrote in a single write."""

    num_writes: int = 0
    """The total number of times we write to this database."""

    total_points: int = 0
    """The total number of points that we attempted to write to this database."""

    rejected_points: int = 0
    """The total number of points that were rejected by the database.

    Rejections are caused by one of two things:

    - schema conflict: a measurement had a field with a different type in the same shard.
    - retention period: a point had a timestamp so far in the past that it was dropped.
    """

    first_invalid_measurement: str = None
    """The first failing measurement that was reported during this reporting interval.

    If there are points that were rejected, either due to type conflict or
    retention period issues, then the name of the measurement with the first
    such failure during this reporting period is stored here so that users can
    know where to look for problems.
    """

    first_invalid_field: str = None
    """The invalid field inside of the first failing measurement with a type conflict."""


@dataclasses.dataclass
class S3UploadMetric(MonitoringMetric):
    """Health metric on parallel file uploads to s3."""

    measurement: str = measurement_field("s3_message_upload")

    bucket: str = tag_field(default=None)
    """The s3 bucket that we are uploading to."""

    compressed: bool = noreset_field(default=False)
    """Whether we are compressing data that is uploaded to s3."""

    max_concurrency: int = noreset_field(default=None)
    """The maximum number of parallel uploads that we support."""

    processed_bytes: int = 0
    """The total number of bytes that we processed from input messages."""

    uploaded_bytes: int = 0
    """The total number of bytes that we uploaded to s3 (after possible compression)."""

    uploaded_messages: int = 0
    """The total number of messages that we uploaded to s3."""

    upload_timeouts: int = 0
    """The total number of messages we dropped because of a timeout uploading to s3."""

    dropped_no_schema: int = 0
    """The total number of messages that were dropped because of unsupported schemas."""

    dropped_no_org: int = 0
    """The total number of messages that were dropped because of missing a valid org."""

    dropped_no_site: int = 0
    """The total number of messages that were dropped because of missing a valid site."""

    peak_concurrency: int = 0
    """The peak number of parallel messages we processed at once.

    Comparing peak_paralellism with max_parallelism provides a view into how close
    this upload process is running to being overloaded and not able to keep up with
    the inflow of messages.
    """

    peak_uploads: int = 0
    """The peak number of parallel uploads that we had at once.

    This metric should also be compared against max_concurrency.  Generally
    the upload process is the rate limiting step in processing a message so
    this should equal peak_concurrency, but if there is a bottleneck, for
    example compressing messages, then that will show up as peak_concurrency
    larger than peak_uploads.
    """

    max_conn_wait: float = 0
    """The maximum number of seconds we waited for a tcp connection.

    This metric tells us when our connection pool should be larger and we
    are limited by waiting for a free connection object from the pool.
    """

    max_upload_time: float = 0
    """The maximum number of seconds it took to upload a message to s3.

    This is measured from the time we start the upload to when the upload
    returned.  It does not include any time spent waiting for a free tcp
    connection object.
    """

@dataclasses.dataclass
class S3SyncUploadMetric(MonitoringMetric):
    """Metrics for the S3Uploader used in the ArchiveLink."""
    measurement: str = measurement_field("s3_sync_upload")

    bucket: str = tag_field(default=None)
    """Bucket the S3Uploader is uploading to."""

    uploaded_files: int = 0
    """Number of files succesfully uploaded to S3."""

    upload_timeouts: int = 0
    """Number of timeouts encountered when uploading files to S3."""

    peak_upload_time: float = 0.0
    """Peak time it took to upload a single file to S3."""

    max_qsize: int = 0
    """Number of files waiting to be uploaded to S3."""

    seconds_since_last_upload: float = noreset_field(default=0.0)
    """Number of seconds since the last successful upload."""

    seconds_since_last_attempt: float = noreset_field(default=0.0)
    """Number of seconds since the last upload attempt."""
