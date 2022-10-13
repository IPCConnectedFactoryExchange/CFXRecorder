"""Broker link exceptions."""


class ScriptError(RuntimeError):
    """Generic base error for all exceptions that should cause a worker to exit."""

    def __init__(self, code: int, reason: str):
        super().__init__()

        self.code = code
        self.reason = reason


class ConnectionClosedError(ScriptError):
    """Class raised when the input amqp connection to the broker-link is closed."""

    def __init__(self, reason: str):
        super().__init__(1, reason)


class StaticConfigurationError(ScriptError):
    """Class raised when static information cannot be validated at startup.

    For example, if there are credentials needed to access an external server,
    this error will be raised if those credentials fail.
    """

    def __init__(self, reason: str):
        super().__init__(2, reason)


class MessageProcessingError(ScriptError):
    """Class raised when a message causes a processing error.

    This error should be used to indicate when a message was processed in a
    way that indicates a logic error inside of the broker-link processor that
    should cause the processor to exit rather than to continue processing more
    messages.
    """

    def __init__(self, reason: str):
        super().__init__(3, reason)


class FatalInternalError(ScriptError):
    """Class raised when a broker-link encounters a fatal error.

    This class may be raised at any time, even during message processing and
    will cause the entire broker-link to shut-down.  This should be thought of
    as equivalent to a kernel panic or an assert.  It should be used to handle
    conditions that do not have a safe resolution that could allow the
    broker-link to continue correct operation.

    So, rather than powering through and potentially triggering silent errors
    or data corruptions downstream, the broker-link should stop operation so
    that it can be fixed.
    """

    def __init__(self, reason: str):
        super().__init__(4, reason)

