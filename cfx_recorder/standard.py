"""Helper factory functions for creating a broker connection with optional ssh tunneling."""

from typing import Tuple, Optional
from getpass import getpass
import dataclasses
import logging
from .exceptions import StaticConfigurationError
from .utils import ConnectionURL
from .ssh import TunnelManager
from .amqp import BackgroundAMQPConnection, parse_broker_connection, get_broker_connection

LOG = logging.getLogger(__name__)


def build_tunnel_and_connection(broker_url: Optional[str], ssh_login: Optional[str] = None,
                                explicit_password: Optional[str] = None,
                                keyring: bool = False) -> Tuple[Optional[TunnelManager], BackgroundAMQPConnection]:
    """Create the right broker connection and tunnel based on cli args.

    This helper function substitutes a PickedAMQPConnection to allow for
    playing back messages easily for testing purposes or reproducing issues.
    """

    if broker_url is None:
        raise StaticConfigurationError("You must either connect to a broker or load messages from a file")

    conn_info = parse_broker_connection(broker_url)

    if ssh_login is not None and conn_info.scheme in ('file', 'outfile'):
        raise StaticConfigurationError("You cannot use an ssh tunnel when loading messages from a file")

    if conn_info.scheme in ('file', 'outfile'):
        return None, get_broker_connection(conn_info)

    conn_info = ensure_broker_credentials(conn_info, keyring, explicit_password)
    return build_connection_with_tunnel(conn_info, ssh_login=ssh_login, keyring=keyring)


def prepare_connection_info(broker_url: Optional[str], ssh_login: Optional[str] = None,
                            explicit_password: Optional[str] = None,
                            keyring: bool = False) -> Tuple[Optional[TunnelManager], ConnectionURL]:
    """Create the right broker connection and tunnel based on cli args.

    This helper function substitutes a PickedAMQPConnection to allow for
    playing back messages easily for testing purposes or reproducing issues.
    """

    if broker_url is None:
        raise StaticConfigurationError("You must either connect to a broker or load messages from a file")

    conn_info = parse_broker_connection(broker_url)

    if ssh_login is not None and conn_info.scheme in ('file', 'outfile'):
        raise StaticConfigurationError("You cannot use an ssh tunnel when loading messages from a file")

    if conn_info.scheme in ('file', 'outfile'):
        return None, conn_info

    conn_info = ensure_broker_credentials(conn_info, keyring, explicit_password)
    return build_tunnel_and_map(conn_info, ssh_login=ssh_login, keyring=keyring)


def build_tunnel_and_map(conn_info: ConnectionURL, ssh_login: Optional[str] = None,
                         tunnel_host: Optional[str] = None,
                         keyring: bool = False) -> Tuple[TunnelManager, ConnectionURL]:
    """Build a tunnel (possible a no-op) and modify connection host/port info."""

    broker = conn_info.host
    if broker is None:
        raise StaticConfigurationError(f"No broker host specified when trying to build a tunnel: {conn_info}")

    if conn_info.port is None:
        raise StaticConfigurationError(f"Missing port when configuring an ssh tunnel: {conn_info}")

    if ssh_login is not None and tunnel_host is None:
        tunnel_host = conn_info.host
        broker = '127.0.0.1'

    tunnel_man = TunnelManager(tunnel_host, ssh_login, use_keyring=keyring)

    if tunnel_host is not None:
        tunnel_man.add_tunnel(broker, conn_info.port)
        LOG.info("Tunneling connection over host %s, then connecting to %s:5672", tunnel_host, broker)

    tunnel_man.start()

    try:
        host, port = tunnel_man.map_host_port(broker, conn_info.port)
        conn_info = dataclasses.replace(conn_info, host=host, port=port)

        return tunnel_man, conn_info
    except:
        tunnel_man.stop()
        raise


def build_connection_with_tunnel(conn_info: ConnectionURL, ssh_login: Optional[str] = None,
                                 tunnel_host: Optional[str] = None,
                                 keyring: bool = False) -> Tuple[TunnelManager, BackgroundAMQPConnection]:
    """Create a connection to the broker, starting an SSH tunnel if necessary."""

    tunnel_man, conn_info = build_tunnel_and_map(conn_info, ssh_login, tunnel_host, keyring)

    try:
        return tunnel_man, get_broker_connection(conn_info)
    except:
        tunnel_man.stop()
        raise


def ensure_broker_credentials(conn_info: ConnectionURL, use_keyring=True,
                              explicit_password: Optional[str] = None) -> ConnectionURL:
    """Find the correct broker credentials.

    This first checks for a password passed in via command line.
    If not there, it checks the keyring unless this is forbidden.
    As a last resort it prompts for a password using get_pass().
    """

    if conn_info.username is None:
        raise StaticConfigurationError("Missing user for connecting to the amqp broker.")

    if explicit_password is not None:
        return dataclasses.replace(conn_info, password=explicit_password)

    if conn_info.password is not None and conn_info.password != '':
        return conn_info

    if use_keyring:
        name = "%s-%s" % ('amqp', conn_info.host)

        try:
            import keyring
            password = keyring.get_password(name, conn_info.username)
        except ImportError as exc:
            raise StaticConfigurationError(
                "Missing keyring dependency, `pip install keyring` OR pass `--no-keyring`"
            ) from exc

    if password is None:
        password = getpass("Enter amqp password for %s@%s: " % (conn_info.username, conn_info.host))

    return dataclasses.replace(conn_info, password=password)
