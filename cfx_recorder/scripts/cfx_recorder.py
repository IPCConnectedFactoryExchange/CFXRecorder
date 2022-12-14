"""A configurable python script for recording traffic on an AMQP broker to local archive files."""

from typing import Optional
import logging
import sys
import time
import uuid
import traceback
from ..archive_link import ArchiveLink
from ..standard import build_tunnel_and_connection
from ..utils import EnvironmentArgumentParser, configure_logging
from ..exceptions import ConnectionClosedError, ScriptError, StaticConfigurationError


logger = logging.getLogger('cfx-recorder')  # pylint: disable=invalid-name;This is our cli script logging pattern.


def parse_args(argv):
    """Parse command line arguments."""

    parser = EnvironmentArgumentParser(description="High-performance output link for extracting broker data")

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', action="count", default=0,
                       help="Increase logging level (goes error, warn, info, debug, default is critical)")
    group.add_argument('-q', '--quiet', action="store_true", help="Disable all logging output")

    parser.add_argument('--log-file', help="Log to a file rather than stderr")
    parser.add_argument('--include', action="append", default=[], help="Whitelist only specific loggers")
    parser.add_argument('--exclude', action="append", default=[], help="Blacklist specific loggers")

    parser.add_argument('--queue', help="Use this specific queue name, not an autogenerated name")
    parser.add_argument('--binding', help="Create or verify binding with this binding key")
    parser.add_argument('-c', '--create-queue', action="store_true", help="Autocreate temporary queue")

    parser.add_argument('-r', '--remote-control', action="store_true", help="Listen for remote control commands")

    parser.add_argument('--verify-only', action="store_true", help="Verify the link works then exit immediately")

    parser.add_argument('-b', '--broker', help="The broker host to connect to")
    parser.add_argument('--broker-password', help="Password for broker in case it needs to be specified separately")

    parser.add_argument('-w', '--worker-id', help="Unique identifier for this worker to use in monitoring and control")

    parser.add_argument('-m', '--monitor', type=float, default=0.0, help="Send monitoring metrics every N seconds")

    parser.add_argument('-s', '--ssh-login', help="SSH into the server specified by broker with this username")

    parser.add_argument('-k', '--no-keyring', action="store_false",
                        help="Don't look up ssh or broker password in local keyring")

    parser.add_argument('--save', help="Save all received messages into a gzipped file for later playback")

    parser.add_argument("--sleep", default=0, type=int,
                        help="Sleep for this many seconds after a failed launch so that autorestart logic slows down")

    subparsers = parser.add_subparsers(help='sub-command help', dest='command')

    ArchiveLink.BuildArgParser(subparsers)

    args = parser.parse_args(argv)

    if args.create_queue is True and args.binding is None:
        raise StaticConfigurationError("You must pass a binding key using --binding if you are creating your own queue")

    return args


def get_queue_and_binding(binding_key: str, queue_name: Optional[str] = None, create_queue: bool = False):
    """Generate a queue name and binding keys."""

    if queue_name is None:
        queue_name = 'cfx-recorder-' + str(uuid.uuid4())

    if create_queue is False:
        return queue_name, None

    return queue_name, binding_key


def run_broker_link(link_type, amqp_conn, args):
    """Helper function to run a broker link until the amqp connection is closed."""

    amqp_conn.start()

    try:
        link = link_type.CreateFromArgs(amqp_conn, args)

        queue_name, binding = get_queue_and_binding(args.binding, args.queue, args.create_queue)

        if args.remote_control:
            link.enable_remote_control()

        if binding is not None:
            amqp_conn.bind_temporary_queue(queue_name, binding)

        if args.save:
            link.record_messages(args.save)

        link.enable_input(queue_name)

        if args.verify_only:
            logger.info("--verify-only passed, quitting immediately")
            raise ScriptError(0, "Exiting because of --verify-only")

        logger.info("Starting main broker-link loop")
        link.run()
    finally:
        logger.info("Shutting down amqp connection.")
        amqp_conn.stop()


def main(argv=None):
    """Main entry point for cfx-recorder script."""

    should_raise = argv is not None

    if argv is None:
        argv = sys.argv[1:]

    sleep = 0
    try:
        args = parse_args(argv)

        sleep = args.sleep

        configure_logging(args.verbose, args.quiet, args.log_file,
                          include=args.include, exclude=args.exclude)

        link_type = ArchiveLink
        tunnel_man, amqp_conn = build_tunnel_and_connection(
            args.broker, args.ssh_login,
            args.broker_password, args.no_keyring
        )

        try:
            run_broker_link(link_type, amqp_conn, args)
        finally:
            if tunnel_man is not None:
                tunnel_man.stop()
    except ConnectionClosedError:
        if should_raise or not args.broker.startswith("file:"):
            raise

        # When we're processing presaved messages, an EOF (closed connection) is expected
        logger.info("Finished processing all presaved messages")
        return 0
    except ScriptError as err:
        if err.code == 0:
            return 0

        if should_raise:
            raise

        print("ERROR: {}".format(err.reason))

        if sleep != 0:
            logger.critical("Sleeping %d seconds before exiting", args.sleep)
            time.sleep(args.sleep)

        return err.code
    except KeyboardInterrupt:
        if should_raise:
            raise

        print("Exiting due to ctrl-c")
        return 0
    except Exception as err:  # pylint:disable=broad-except;This is a cli script wrapper
        if should_raise:
            raise

        print("ERROR: Uncaught exception")

        if sleep != 0:
            logger.critical("Sleeping %d seconds before exiting", args.sleep)
            time.sleep(args.sleep)

        traceback.print_exc()
        return 127

    return 0


if __name__ == '__main__':
    sys.exit(main())
