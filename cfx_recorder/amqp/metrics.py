"""Basic metrics that are common to many different kinds of workers."""

from dataclasses import dataclass
from .telegraf import MonitoringMetric, measurement_field, tag_field, noreset_field


@dataclass
class WorkerEvent(MonitoringMetric):
    """A basic metric defined whenever a worker experiences an important event.

    This can be used to record when a worker starts or stops for example.
    """

    measurement: str = measurement_field("worker_event")
    event: str = tag_field(default=None)

    message: str = None
    counter: int = 1  # Lets you easily plot this event


@dataclass
class WorkerHeartbeat(MonitoringMetric):
    """Basic metric for a worker's heartbeat."""

    measurement: str = measurement_field("worker_heartbeat")

    heartbeat: int = 1
    """A fixed heartbeat value sent at each interval to allow plotting."""

    heartbeat_interval: float = noreset_field(default=0)
    """The fixed interval at which heartbeats should be expected.

    This allows setting automated alarms if the time between heartbeats
    exceeds the expected heartbeat_interval by some percentage.
    """


@dataclass
class WorkerHealth(MonitoringMetric):
    """Basic metric tracking the health of a generic worker.

    The worker type and actual processor are tags so that metrics can be
    easily aggregrated.  The goal of this metric to allow for stall and
    anomaly detection to be performed on each broker-link.
    """

    measurement: str = measurement_field("worker_health")

    received_messages: int = 0
    """How many messages have been received.

    This counter is incremented early in the processing of any messages to
    help detect stalls or other blocks that cause messages to be received
    but neither processed nor failed.
    """

    processed_messages: int = 0
    """How many messages have been successfully processed."""

    received_control_messages: int = 0
    """How many control messages have been received.

    This counter is incremented early in the processing of a control message.
    It increments both for broadcast controls and directed controls.
    """

    processed_control_messages: int = 0
    """How many control messages have been successfully processed."""

    failed_control_messages: int = 0
    """How many control messages have failed during processing."""


@dataclass
class WorkerLogActivity(MonitoringMetric):
    """Basic metric tracking how many log entries a worker has produced.

    The goal of this metric is to allow for easily tracking how many log
    messages of each severity level are being generated to use for anomaly
    detection.
    """

    measurement: str = measurement_field("log_activity")

    log_level: str = noreset_field(default="notset")

    debug_messages: int = 0
    info_messages: int = 0
    warning_messages: int = 0
    error_messages: int = 0
    critical_messages: int = 0
