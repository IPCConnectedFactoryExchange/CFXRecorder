"""Helper clases for telegraf monitoring.

These classes are designed to all for easy definition of telegraf compatible
metrics that can be serialized and sent over amqp to be collected and stored
inside an influxdb monitoring database.

The expected usage pattern is that a worker attached to a broker will declare
a ``MetricSet()`` member and periodically iterate over all of the
``MonitoringMetric`` objects contained in it, publish them and then reset
them.

Workers can define their own metrics that are specific to their behavior as
subclasses of ``MonitoringMetric``.  The design of the MonitoringMetric class
validates that all subclasses have the right dataclass fields to be able to be
serialized into telegraf compatible measurements.

Example Custom Monitoring Metric::

    import dataclasses
    from broker_sdk.clients.amqp import MonitoringMetric, measurement_field, tag_field

    @dataclasses.dataclass
    class BrokerLinkHealth(MonitoringMetric):
        measurement: str = measurement_field("broker_link_health")

        link_type: str = tag_field(default=None)
        link_processor: str = tag_field(default=None)

        received_messages: int = 0
        processed_messages: int = 0

This example would define a custom monitoring metric that had two tags and two
integer fields.  The fields would autoreset back to their default values
everytime you call ``reset()`` on the metric, which should happen after
publishing it.
"""

from typing import Dict, Set, Union, Optional
import json
import contextlib
import time
import datetime
import dataclasses
import amqp


def tag_field(**kwargs):
    """Mark a field as a tag.

    All dataclass members not marked as a tag or special are assumed to be
    fields when serialized.
    """

    kwargs['metadata'] = {'telegraf_type': 'tag'}
    return dataclasses.field(**kwargs)


def noreset_field(default):
    """Mark a field that should never be reset to its default value.

    This can be useful for fields that have a customized but fixed value is
    set once and reported on each measurement without changing.
    """

    return dataclasses.field(default=default, metadata={'reset_field': False})


def special_field(**kwargs):
    """Mark a field as a special value that is neither tag nor field.

    This can be used to exclude attributes from serialization.  The only
    builtin field markes as special is the measurement name that line protocol
    hands differently but subclases could chose to include different field
    types that are not meant to be serialized.
    """

    kwargs['metadata'] = {'telegraf_type': 'special'}
    return dataclasses.field(**kwargs)


def measurement_field(name: str):
    """Mark the field containing the name of the measurement."""

    return special_field(default=name)


def time_field():
    """Mark the field containing the explicit timestamp of the measurement.

    This field specifier should only be used on metrics that are sent to
    indicate one-off events, rather than events that are reported
    periodically.
    """

    return special_field(default=None)


@dataclasses.dataclass
class MonitoringMetric:
    """Base class for all telegraf compatible monitoring metrics."""

    _cached_tags: Set[str] = special_field(default=None)
    _cached_fields: Set[str] = special_field(default=None)
    _explicit_timestamp: bool = special_field(default=False)

    def __post_init__(self):
        """Ensure that all tags are strings and all fields are of valid type.

        This function is called by the dataclass logic after __init__() and we
        use it to validate that everything is setup correctly and can be serialized
        into a telegraf compatible output.
        """

        field = self._measurement_field()
        if field.default is None or not issubclass(field.type, str):
            raise TypeError("Invalid metric field: `measurement`, must have a string default value")

        time_field = self._time_field()
        if time_field is not None:
            self._explicit_timestamp = True
            if not issubclass(time_field.type, datetime.datetime):
                raise ValueError("Invalid metric time field that is not a datetime")
        else:
            self._explicit_timestamp = False

        for field in dataclasses.fields(self):
            field_type = field.metadata.get('telegraf_type', 'field')
            if field_type == 'tag' and not issubclass(field.type, str):
                raise ValueError("Invalid metric tag that is not a string: %s" % field)
            if field_type == 'field' and not issubclass(field.type, (str, int, bool, float)):
                raise ValueError("invalid metric field that does not have the correct type: %s" % field)

    def _measurement_field(self) -> dataclasses.Field:
        """Find the measurement field and return it."""

        for field in self._iter_fields('special'):
            if field.name == 'measurement':
                return field

        raise TypeError("All MonitoringMetric subclasses must define a `measurement` field of type `str` "
                        "using measurement_field(\"desired_measurement_name\")")

    def _time_field(self) -> Optional[dataclasses.Field]:
        """Find the timestamp field and return it."""

        for field in self._iter_fields('special'):
            if field.name == 'time':
                return field

        return None

    def tag_keys(self) -> Set[str]:
        """Extract a set of all of the tag keys defined on this metric."""

        if self._cached_tags is None:
            self._cached_tags = set(x.name for x in self._iter_fields('tag'))

        return self._cached_tags

    def field_keys(self) -> Set[str]:
        """Extract a set of all of the fields defined on this metric."""

        if self._cached_fields is None:
            self._cached_fields = set(x.name for x in self._iter_fields('field'))

        return self._cached_fields

    def tags(self, filter_none: bool = False) -> Dict[str, str]:
        """Extract all tag keys/values defined in this metric.

        A tag is a dataclass attribute defined with field metadata of
        telegraf_tag set as True.  This can be set conveniently by
        using the ``tag_field(default_value)`` initializer when defining
        the data class.

        Returns:
            A map of all of the tags in this measurement and their values.

            If filter_none is passed, then tags that are not set will be
            omitted.
        """

        tags = {}

        for key in self.tag_keys():
            value = getattr(self, key)
            if value is None and filter_none:
                continue

            tags[key] = value

        return tags

    def fields(self) -> Dict[str, Union[str, bool, int, float]]:
        """Extract the current values of all fields in this metric."""

        field_vals = {}

        for key in self.field_keys():
            value = getattr(self, key)
            field_vals[key] = value

        return field_vals

    def reset(self):
        """Reset all of the fields in this metric to their default values.

        This method is designed to allow for incremental reporting of metrics
        that should be reset during each reporting interval.  All of the
        `fields` in this metric will be reset to their default values.  The
        tags will be unchanged.

        If a field was created with ``noreset_field()`` then it will not be
        reset.
        """

        for field in self._iter_fields(only_type="field"):
            if field.metadata.get('reset_field', True):
                setattr(self, field.name, field.default)

    def routing_key(self, worker_id) -> str:
        """Get the appropriate routing key for reporting this metric via amqp."""

        #type: ignore
        return f"worker.{worker_id}.telegraf.{self.measurement}"  # pylint: disable=no-member

    def serialize_dict(self, timestamp: Optional[float] = None) -> Dict:
        """Serialize this metric into a telegraf compatible dictionary."""

        if self._explicit_timestamp and timestamp is None:
            timestamp = _extract_epoch_time(self.time)  # pylint: disable=no-member;This is guarded by self._explicit_timestamp
        elif timestamp is None:
            timestamp = time.time()

        metric = {
            "name": self.measurement,  # pylint: disable=no-member;This is defined by each subclass and checked in __postinit__
            "fields": self.fields(),
            "tags": self.tags(),
            "timestamp": timestamp
        }

        return metric

    def serialize_amqp(self, timestamp: Optional[float] = None) -> amqp.Message:
        """Convenience method to create an AMQP message containing this metric."""

        metric_dict = self.serialize_dict(timestamp)
        serialized = json.dumps(metric_dict)

        return amqp.Message(serialized)

    def serialize_string(self) -> str:
        """Serialize this metric into a simple string."""

        tag_str = ", ".join("{}={}".format(key, value) for key, value in sorted(self.tags().items()))
        field_str = ", ".join("{}={}".format(key, value) for key, value in sorted(self.fields().items()))

        return "Metric '{}'\n    Tags: {}\n    Fields: {}".format(self.measurement, tag_str, field_str)  # pylint: disable=no-member;This is defined by each subclass and checked in __postinit__

    @contextlib.contextmanager
    def capture_runtime(self, attribute: str, stat: str = "max", units="ms", include_exceptions=False):
        """Capture statistics about the runtime of a given set of statement.

        This method returns a context manage and is meant to be used in a ``with`` statement.

        It will automatically capture a statistic of the runtime of the statements inside the
        ``with`` block into the named ``attribute``, which should be an integer field.

        By default, no time is captured if the statements exit with an exception, but you can adjust
        this by passing ``include_exceptions=True``.

        Args:
            attribute: The name of the field that the runtime statistic will be saved in
            stat: What running statistic to capture.  The only supported option currently
                is ``max``.
            units: The time units to report the runtime in.  You can choose ``s`` or ``ms``
                for seconds or the default milliseconds.
            include_exceptions: Whether to include or omit runtimes when the with block exits
                due to an exception.  Default behavior is to omit.
        """

        _unit_conversion = {
            'ms': 1000,
            's': 1
        }

        if stat not in frozenset(['max']):
            raise ValueError(f"Unsupported statistic type: {stat}")

        unit_multiplier = _unit_conversion.get(units)
        if unit_multiplier is None:
            raise ValueError(f"Unsupported time units: {units}")

        if attribute not in self.field_keys():
            raise ValueError(f"Unknown field name: {attribute}")

        start = time.monotonic()

        exception = False
        try:
            yield
        except:
            exception = True
            raise
        finally:
            if not exception or include_exceptions:
                end = time.monotonic()

                runtime = int((end - start) * unit_multiplier)
                last_runtime = getattr(self, attribute)

                setattr(self, attribute, max(runtime, last_runtime))

    def dynamic_max(self, attribute: str, value):
        """Dynamically update an attribute by name with max() stat.

        Args:
            attribute: The name of the attribute to update
            value: The new value to check and update based on
                max() statistic.
        """

        last_value = getattr(self, attribute)
        setattr(self, attribute, max(last_value, value))

    def _iter_fields(self, only_type=None):
        for field in dataclasses.fields(self):
            if only_type is not None and field.metadata.get('telegraf_type', 'field') != only_type:
                continue

            yield field


class MetricSet:
    """A set of measurements that are reported together.

    This class is designed to be added as an attribute of running workers that
    periodically report metrics on their performance and health.

    It is created with a ``monitoring_interval`` in seconds that it uses to keep
    track of when the next time metrics should be reported.  It also keeps track
    of the worker_id that is needed to construct the routing keys for reporting
    metrics.

    The expected usage is that a worker has a single MetricSet() instance that
    tracks all of the metrics that the worker is reporting.  The worker
    periodically checks if it needs to report metrics because enough time
    has passed and then iterates over the metrics in the metric set, publishes
    them over AMQP and resets them for the next reporting interval.

    Args:
        monitoring_interval: The number of seconds between subsequent metric
            publications.  If this is None or <= 0.0 then automated metric
            reporting will be disabled.
        worker_id: The unique identifier of the worker sending these metrics.
    """

    def __init__(self, monitoring_interval: float, worker_id: str):
        self._metrics = {}
        self.worker_id = worker_id

        self.interval = monitoring_interval
        self._last_send_time = 0.0

    def __iter__(self):
        return iter(self._metrics.values())

    def __getattr__(self, key):
        metric = self._metrics.get(key)
        if metric is None:
            raise AttributeError("No registered metric named %s" % key)

        return metric

    def __getitem__(self, key):
        metric = self._metrics.get(key)
        if metric is None:
            raise KeyError("No registered metric named %s" % key)

        return metric

    def register_metric(self, short_name: str, metric: MonitoringMetric):
        """Register a new metric that should be reported as part of this set.

        Registered metrics can be accesssed as attributes on this class. For
        example, if you register a metric named ``health``, then you can
        access it using ``metric_set_instance.health``.  This is designed to
        allow for simplified code design when updating metrics inside workers.

        Args:
            short_name: The name of the metric that should be used to make it
                accessible as an attribute on this MetricSet.  The value is not
                reported as part of the metric, it is only used for quick access.
            metric: The metric instance itself that we should be reporting.
        """

        if not isinstance(metric, MonitoringMetric):
            raise ValueError("Attempted to register a metric that's not a MonitoringMetric: type=%r" % metric)

        self._metrics[short_name] = metric

    def unregister_metric(self, short_name: str):
        """Remove a previously registered metric.

        Args:
            short_name: The name of the metric that should be used to make it
                accessible as an attribute on this MetricSet.  The value is not
                reported as part of the metric, it is only used for quick access.
        """

        del self._metrics[short_name]

    def reset_metrics(self):
        """Convenience method to call reset() on all metrics."""
        for metric in self._metrics.values():
            metric.reset()

    def reset_timer(self):
        """Reset the timer used to count the last time we reported metrics."""

        self._last_send_time = time.monotonic()

    def reset_all(self):
        """Reset all metrics and the reporting timer.

        This is a convenience method to reset everything once all metrics have
        been reported for a given reporting interval.
        """

        self.reset_metrics()
        self.reset_timer()

    def should_publish(self) -> bool:
        """Check if enough time has expired that we should report metrics."""
        if self.interval is None or self.interval <= 0.0:
            return False

        now = time.monotonic()
        if now - self._last_send_time >= self.interval:
            return True

        return False

    def enabled(self) -> bool:
        """Check if periodic metric reporting is enabled."""

        if self.interval is None or self.interval <= 0.0:
            return False

        return True


_EPOCH = datetime.datetime(1970, 1, 1)

def _extract_epoch_time(time_obj: Optional[datetime.datetime]) -> float:
    if time_obj is None:
        raise ValueError("Missing time member in measurement requiring explicit timestamp")

    if not isinstance(time_obj, datetime.datetime):
        raise ValueError(f"Measurement `time` field is not a datetime, found {time_obj}")

    return (time_obj - _EPOCH).total_seconds()
