"""A simple ssh port tunneling tool for connecting to servers that are not directly accessible to this machine."""

from typing import Optional
from getpass import getpass
import logging

class TunnelManager:
    """A simple SSH based tunnel manager.

    This can tunnel local port to a remote server and fetch server credentials
    from a secure local key vault.  The key vault is based on the keyring python package
    that uses a secure credential storage vault builtin to each platform.  In order to
    load passwords into the keyring, you can use::

        keyring set ssh-HOST_OR_IP <username>
        (prompt for password)

    The correct service name to use in ``keyring`` is "ssh-HOST" where ``HOST`` is the exact
    same string you pass to create a TunnelManager instance.

    Args:
        host: The ssh host to connect to.  This must be running on port 22.
        username: The username to connect with
        password: Optional password to authenticate with.  Defaults to looking up the password in
            keyring if not specified.  If keyring doesn't have the password, then it is prompted
            via ``getpass``.
        use_keyring: Whether to lookup a password via keyring or not.  Defaults to True.
    """

    def __init__(self, host: Optional[str], username: Optional[str], password: Optional[str] = None, use_keyring: bool = True):
        self.targets = {}

        self.host = host
        self.username = username

        if host is not None and password is None:
            if use_keyring:
                password = _try_fetch_password('ssh', host, username)

            if password is None:
                password = getpass("Enter password for ssh %s@%s: " % (username, host))

        self.password = password
        self._logger = logging.getLogger(__name__)

    def add_tunnel(self, target_ip: str, target_port: int, local: Optional[int] = None):
        """Define a new ssh tunnel.

        This will register a tunnel that forwards connection on localhost port
        ``local`` to remote tcp port ``target_ip:target_port``. Target IP can
        either be localhost (which means the ssh server itself or it can be a
        different IP, which means that the ssh server will connect outbound
        via TCP to a different server).

        Args:
            target_ip: The target that should be connected to
            target_port: The port we should connect to
            local: The local port to forward.  If this is None, then a random
                local port will be allocated and can be read out by using
                ``map_host_port`` after ``start`` has returned.
        """
        key = (target_ip, target_port)

        local_bind_address = None
        if local is not None:
            local_bind_address = ('127.0.0.1', local)

        try:
            from sshtunnel import open_tunnel  # pylint: disable=import-outside-toplevel;This is designed to be an optional dependency
        except ImportError as err:
            raise RuntimeError("Missing sshtunnel dependency to create ssh tunnel, `pip install sshtunnel`") from err

        server = open_tunnel(
            self.host,
            ssh_username=self.username,
            ssh_password=self.password,
            remote_bind_address=(target_ip, target_port),
            local_bind_address=local_bind_address
        )

        self.targets[key] = server

    def map_host_port(self, target_ip: str, target_port: int):
        """Return the correct host and port to connect to a given target.

        This convenience function will also allow you to pass in targets that
        are not tunneled and simply return them unmodified so that you are
        able to tunnel some ports but not others and have a uniform API to
        call to figure out what host and port should be used to connect to
        any resource.

        Args:
            target_ip: The target IP you wish to connect to
            target_port: The port number on the target IP you wish to connect to

        Returns:
            The hostname and port that should be used to connect to the given target.
        """

        key = (target_ip, target_port)
        server = self.targets.get(key)

        if server is None:
            return (target_ip, target_port)

        return ('127.0.0.1', server.local_bind_port)

    def start(self):
        """Start all port forwarding servers in the background and then return."""

        started = []

        try:
            for target_key, server in self.targets.items():
                server.start()

                started.append((target_key, server))

        except:
            self._logger.error("Error opening tunnel to one of the servers, closing all previously opened tunnels",
                               exc_info=True)

            for target_key, started in server:
                try:
                    started.stop()
                except:  # pylint:disable=bare-except;We need to make sure we close all other servers.
                    self._logger.warning("Error closing tunnel to %s during error opening other tunnel", target_key,
                                         exc_info=True)

            raise

    def stop(self):
        """Halt any running port forwarding servers."""

        for server in self.targets.values():
            server.stop()

    def __enter__(self):
        self.start()

    def __exit__(self, *args):
        try:
            self.stop()
        except: # pylint:disable=bare-except;We need to log this error since we already have a potential exception
            self._logger.error("Error in __exit__ stopping the ssh tunnel", exc_info=True)

        return False


def _try_fetch_password(service, host, username):
    """Try to fetch a service password from the local computer's keyring.

    This expects the credential to be named as ``service-host`` so
    for example panacim-10.0.0.1.

    You can load credentials using::

        keyring set service-name user
            PROMPTED FOR PASSWORD VIA STDIN
    """

    try:
        import keyring  # pylint: disable=import-outside-toplevel;This is designed to be an optional dependency
    except ImportError as err:
        raise RuntimeError("Missing keyring depedency, `pip install keyring`") from err

    name = "%s-%s" % (service, host)
    return keyring.get_password(name, username)
