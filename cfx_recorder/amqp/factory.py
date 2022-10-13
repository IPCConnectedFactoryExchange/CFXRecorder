"""Factory to generate a broker connection based on a URL."""

from typing import Union, Optional, Type, Tuple, Any, Dict
import os
from ..utils import parse_connection_url, ConnectionURL
from .background_connection import BackgroundAMQPConnection
from .pickled_connection import PickledAMQPConnection

_DEFAULT_PORTS = {
    'amqp': 5672,
    'amqps': 5671,
    'file': None,
    'outfile': None
}

_VALID_SCHEMES = frozenset(['amqp', 'amqps', 'file', 'outfile'])

_SUPPORTED_PARAMS: Dict[str, Union[Type, Tuple[Type, Any]]] = {
    'prefetch': (int, 100),
    'realtime': (bool, True),  # only for file://
    'bookmark': (bool, False), # only for file://
    'out': (bool, False),      # only for file://
    'throttle': int            # only for file://, send a max of this many messages per second
}


def parse_broker_connection(url: str) -> ConnectionURL:
    """Parse a broker connection URL.

    See docstring on :func:`get_broker_connection` for details on supported
    URL parameters that are extracted.

    Returns:
        The parsed and validated ConnectionURL object.
    """

    parsed = parse_connection_url(url, valid_params=_SUPPORTED_PARAMS, default_port=_DEFAULT_PORTS)

    if parsed.scheme not in _VALID_SCHEMES:
        raise ValueError(f"Unsupported URL scheme for broker connection: {parsed.scheme}")

    return parsed


def get_broker_connection(
        url_or_parsed: Union[str, ConnectionURL]
) -> Union[BackgroundAMQPConnection, PickledAMQPConnection]:
    """Create a broker connection from a URL.

    The supported schemes are ``amqp://`` and ``file:<path>``.  If you pass a
    file scheme, then a PickledAMQPConnection will be returned.  Otherwise a
    real BackgroundAMQPConnection will be returned.  You can configure
    properties by passing parameters.  The supported parameters are:

     - prefetch (int): The maximum number of outstanding/unacknowledged messages allowed
       on the connection
     - realtime (bool): Whether to deliver messages in realtime or as fast as possible.
       Defaults to True.  Only applies to file based connections.

    Args:
        url: The URL to use to create the connection.

    Raises:
        ValueError: The URL could not be parsed correctly.

    Returns:
        The constructed broker connection object.
    """

    if isinstance(url_or_parsed, ConnectionURL):
        parsed = url_or_parsed
    else:
        parsed = parse_broker_connection(url_or_parsed)

    if parsed.scheme == 'outfile':
        return PickledAMQPConnection(None, output_file=parsed.path)

    if parsed.scheme == "file":
        realtime: bool = parsed.params.get('realtime')  # type: ignore
        throttle: Optional[int] = parsed.params.get('throttle')
        output_path = _try_parse_output_path(parsed.path, parsed.params.get('out'))  # type: ignore[arg-type]

        return PickledAMQPConnection(parsed.path, output_path, realtime=realtime,
                                     prefetch=parsed.params.get('prefetch'),
                                     bookmark=parsed.params.get('bookmark'),
                                     throttle=throttle)

    if parsed.scheme == 'amqp':
        if parsed.host is None or parsed.port is None:
            raise ValueError(f"Missing host ({parsed.host}) or port ({parsed.port})")

        return BackgroundAMQPConnection(parsed.host, parsed.port, parsed.username, parsed.password,
                                        max_messages=parsed.params.get('prefetch', 100))

    raise ValueError(f"Unsupported broker connection scheme: {parsed.scheme}")


def _try_parse_output_path(source_file, has_output: bool) -> Optional[str]:
    if has_output is False:
        return None

    if os.path.isdir(source_file):
        output_folder = os.path.normpath(os.path.join(source_file, '..'))
        output_path = os.path.join(output_folder, 'output.pickle.gz')
    else:
        if not source_file.endswith('.pickle.gz'):
            raise ValueError(f"Cannot create output file path for {source_file}")

        base_path = source_file[:-10]
        output_path = base_path + ".out.pickle.gz"

    return output_path
