"""Common date and time handling functions."""

from typing import Optional
import datetime
import itertools
import dateutil.parser

def parse_datetime(input_str):
    """Parse a datetime string into a datetime object.

    Args:
        input_str (str): The input datetime string to parse.

    Returns:
        datetime.datetime: The parsed datetime object.
    """

    return dateutil.parser.parse(input_str)


def parse_duration(duration: Optional[str]) -> Optional[datetime.timedelta]:
    """Parse a duration string like 7D into seconds.

    The duration must start with an integer number and end with a recognized
    time unit.  Consult the function code for supported units.

    Args:
        duration (str): A string duration specifier such as 5T for 5 minutes.

    Returns:
        int: The number of seconds in the duration.
    """

    short_codes = {
        'sec': 1,
        'min': 60,
        'hour': 60*60,
        'day': 60*60*24,
        'week': 60*60*24*7,
        'month': 60*60*24*31
    }

    abbrev_codes = {
        's': 'sec',
        't': 'min',
        'h': 'hour',
        'd': 'day',
        'w': 'week',
        'm': 'month'
    }

    long_codes = {
        'second': 'sec',
        'minute': 'min'
    }

    if duration is None:
        return duration

    duration = duration.strip()
    num_str = "".join(itertools.takewhile(str.isdigit, duration))
    code_str = duration[len(num_str):].strip().lower()

    try:
        num = int(num_str)
    except ValueError as err:
        raise RuntimeError("Cannot parse duration number {}".format(num_str)) from err

    code_str = long_codes.get(code_str, code_str)
    code_str = abbrev_codes.get(code_str, code_str)
    multiplier = short_codes.get(code_str)

    if multiplier is None:
        raise RuntimeError("Cannot parse duration: {}".format(duration))

    return datetime.timedelta(seconds=num * multiplier)


def parse_datetime_or_duration(in_string):
    """Parse a datetime or a duration relative to utcnow().

    If the input string is prefixed with a ``-`` character then it is
    interpreted as a duration relative to ``datetime.datetime.utcnow()``
    otherwise it is parsed as an absolute datetime.

    Returns:
        datetime.datetime
    """

    if in_string.startswith('$-'):
        in_string = in_string[2:]
        duration = parse_duration(in_string)

        return datetime.datetime.utcnow() - duration

    return dateutil.parser.parse(in_string)


_Y2K_BASE = datetime.datetime(2000, 1, 1)


def sequential_id_to_datetime(ack):
    """Turn an iotile cloud sequential id into utc datetime."""

    if ack & (1 << 31):
        ack ^= (1 << 31)

    delta = datetime.timedelta(seconds=ack)
    return _Y2K_BASE + delta
