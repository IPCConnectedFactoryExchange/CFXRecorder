"""Common functions that are reused across the different distributions"""

from typing import Optional, Dict, Any, Type, Union, Tuple, Sequence, List, Callable
import logging
import sys
import argparse
import traceback
from urllib.parse import urlparse, ParseResult, unquote, quote
import dataclasses
import os
from .exceptions import ScriptError


LOG = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class ConnectionURL:
    """Parsed information extracted from a URL."""

    scheme: str
    host: Optional[str] = None
    port: Optional[int] = None
    path: str = ""
    params: Dict[str, Any] = dataclasses.field(default_factory=dict)
    username: Optional[str] = None
    password: Optional[str] = None


def parse_connection_url(url: str, valid_params: Optional[Dict[str, Union[Type, Tuple[Type, Any]]]] = None,
                         required_params: Optional[Sequence[str]] = None, *,
                         default_scheme: Optional[str] = None,
                         default_port: Optional[Union[int, Dict[str, int]]] = None,
                         unquote_password: bool = False) -> ConnectionURL:
    """Parse out all relevant information from a passed URL.

    This supports pulling key/value pairs out of the `parameter` portion of
    the url. Note that the builtin ``urlparse`` in python's stdlib whitelists
    certain URL schemes that are allowed to have parameters, so we extract and
    parse parameters separately to allow them to be passed on all url schemes.

    If ``unquote_password`` is set as true (default is False for backwards
    compatibility), then any password found in the URL is assumed to be URL
    encoded and is automatically decoded back to a string.  This allows
    passing any UTF-8 string as a password in the URL without worrying about
    special characters causing the url to not parse correctly.

    If the password does not have any special characters in it then url
    decoding is a no-op that doesn't change the password.
    """

    url, param_string = _splitparams(url)
    parts = urlparse(url, scheme=default_scheme)  # type: ParseResult

    if parts.scheme is None:
        raise ValueError("Scheme not specified and no default given")

    port = parts.port
    if port is None:
        port = default_port_for_scheme(parts.scheme, default_port)

    if valid_params is None:
        valid_params = {}

    params = extract_param_dict(param_string, valid_params, required_params)

    password = parts.password
    if password is not None and unquote_password:
        password = unquote(password)

    return ConnectionURL(scheme=parts.scheme, host=parts.hostname, port=port, path=parts.path,
                         params=params, username=parts.username, password=password)


_SUB_DELIMS = "!$&'()*+,;="

def quote_password(password: str) -> str:
    """urlquote a password according to the right set of reserved characters.

    Per RFC 3986, the valid characters that may appear in the password part
    of the user information section of the URL are::

        unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
        sub-delims  = "!" / "$" / "&" / "'" / "(" / ")"
                      / "*" / "+" / "," / ";" / "="

    All other characters must be percent encoded.

    Args:
        password: the input, unquoted string.

    Returns:
        The quoted string, could be identical to input if no quoting needed.
    """

    return quote(password, _SUB_DELIMS)


# See: https://github.com/python/cpython/blob/2b201369b435a4266bda5b895e3b615dbe28ea6e/Lib/urllib/parse.py#L399
def _splitparams(url):
    if '/' in url:
        i = url.find(';', url.rfind('/'))
        if i < 0:
            return url, ''
    else:
        i = url.find(';')
    return url[:i], url[i+1:]


def default_port_for_scheme(scheme: str, default_port: Optional[Union[int, Dict[str, int]]]) -> Optional[int]:
    """Helper function to lookup a port based on the scheme provided.

    This allows having a different default port for each supporteds scheme.
    For example, http and https should have different default ports.
    Similarly, amqp and amqps have different default ports.

    Returns:
        The default port number or None if no defaults were given.
    """

    if default_port is None:
        return None

    if isinstance(default_port, int):
        return default_port

    port = default_port.get(scheme)
    return port


def extract_param_dict(params: str, valid_params: Dict[str, Union[Type, Tuple[Type, Any]]],
                       required_params: Optional[Sequence[str]] = None):
    """Extract typed key/value pairs from a comma-separated list of key=value."""

    parts = params.split(',')

    param_dict = {}
    if required_params is None:
        required_params = []

    for part in parts:
        if len(part) == 0:
            continue

        key, equ, val = part.partition('=')
        if equ != '=':
            raise ValueError("Could not parse parameter string key/value pair %s" % part)

        key = key.strip()
        val = val.strip()

        if key not in valid_params:
            raise ValueError(f"Unknown parameter key {key} not in list of valid parameters")

        type_info = valid_params[key]
        if isinstance(type_info, tuple):
            type_info = type_info[0]

        param_dict[key] = _convert_value(val, type_info)

    for key in required_params:
        if key not in param_dict:
            raise ValueError(f"Missing required param {key} in: '{params}'")

    to_add = {}
    for key, type_info in valid_params.items():
        if not isinstance(type_info, tuple):
            continue

        if key not in param_dict:
            to_add[key] = type_info[1]

    param_dict.update(to_add)

    return param_dict


class _EnvironmentalArgumentGroup(argparse._ArgumentGroup):  # pylint: disable=protected-access; This is the cleanest way to support argument groups
    def __init__(self, container, title=None, description=None, **kwargs):
        super().__init__(container, title=None, description=None, **kwargs)

    def _add_action(self, action):
        env_key = action.dest.upper()
        if isinstance(action, argparse._AppendAction):  # pylint: disable=protected-access; This is the cleanest way to support arrays from env
            env_default = _load_env_array(env_key + "_")
            if len(env_default) > 0:
                action.default = env_default
        else:
            action.default = os.environ.get(action.dest.upper(), action.default)
        return super()._add_action(action)


class EnvironmentArgumentParser(argparse.ArgumentParser):
    """Custom argument parser that supports automatically pulling arguments from environment vars.

    Any argument that is not required or positional will have its value
    automatically pulled from the environment based on the ``dest`` of the
    argument.  If the argument takes an array of values via the ``append``
    action, then the environment variables DEST_0, DEST_1, ... DEST_N, are
    consulted to build up the list of values for the array.

    The values from the environment are considered as the default value for
    the arguments so they are completely overridden if the argument is
    specified on the command line.

    Based on: https://stackoverflow.com/a/24662215/9739119
    """

    class _CustomHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
        def _get_help_string(self, action):
            help_str = super()._get_help_string(action)
            if action.dest != 'help' and not action.required:
                env_key = action.dest.upper()
                if isinstance(action, argparse._AppendAction):  # pylint: disable=protected-access; This is the cleanest way to support arrays from env
                    env_key += r"_{INTEGER}"

                help_str += f' [env: {env_key}]'

            return help_str

    def __init__(self, *, formatter_class=_CustomHelpFormatter, **kwargs):
        super().__init__(formatter_class=formatter_class, **kwargs)

    def add_argument_group(self, *args, **kwargs):
        group = _EnvironmentalArgumentGroup(self, *args, **kwargs)
        self._action_groups.append(group)
        return group

def wrap_script_main(main_func: Callable[[List[str]], int]) -> Callable[[], int]:
    """Main entry point for a script."""

    def _wrapped_main() -> int:
        argv = sys.argv[1:]

        try:
            return main_func(argv)
        except ScriptError as err:
            if err.code == 0:
                return 0

            print("ERROR: {}".format(err.reason))
            return err.code
        except KeyboardInterrupt:
            print("Exiting due to ctrl-c")
            return 0
        except Exception as err:  # pylint:disable=broad-except;This is a cli script wrapper
            print("ERROR: Uncaught exception")
            traceback.print_exc()
            return 127

    return _wrapped_main


def configure_logging(verbose: int, quiet: bool, output_file=None, include=None, exclude=None, prefix=None):
    """Configure logging based on verbosity or quiet settings."""

    root = logging.getLogger()

    if exclude is None:
        exclude = []
    if include is None:
        include = []

    if len(include) > 0 and len(exclude) > 0:
        raise ScriptError(11, "You cannot both include and exclude loggers")

    if os.environ.get('DEBUG', '').lower() == 'true':
        verbose = 4
        quiet = False

    if quiet:
        root.addHandler(logging.NullHandler())
        return

    if output_file is None:
        handler = logging.StreamHandler()
    else:
        handler = logging.FileHandler(output_file)

    if prefix is None:
        prefix = ''
    elif prefix is not None and not prefix.endswith(' '):
        prefix = prefix + ' '

    formatter = logging.Formatter(f'%(asctime)s.%(msecs)03d %(levelname).3s {prefix}%(name)s %(message)s',
                                  '%y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    loglevels = [logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]

    if verbose >= len(loglevels):
        verbose = len(loglevels) - 1

    level = loglevels[verbose]

    if len(include) > 0:
        for name in include:
            logger = logging.getLogger(name)
            logger.setLevel(level)
            logger.addHandler(handler)

        root.addHandler(logging.NullHandler())
    else:
        # Disable propagation of log events from disabled loggers
        for name in exclude:
            logger = logging.getLogger(name)
            logger.disabled = True

        root.setLevel(level)
        root.addHandler(handler)


def _load_env_array(prefix: str) -> List[str]:
    """Helper function to load PREFIX{INT} environment variables."""

    values: List[str] = []

    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue

        postfix = key[len(prefix):]
        try:
            int(postfix)
        except ValueError:
            continue

        values.append(value)

    return values


def _parse_bool(val: str) -> bool:
    """Parse a string a bool with a default value."""

    val = val.lower()
    if val not in ('true', 'false'):
        raise ValueError(f"Invalid bool literal: {val}")

    return val == 'true'


def _convert_value(value: str, type_info: Type):
    """Convert a value simply into a typed python object."""

    if issubclass(type_info, bool):
        return _parse_bool(value)

    return type_info(value)
