class NoSuchJobError(Exception):
    pass


class DeserializationError(Exception):
    pass


class InvalidJobDependency(Exception):
    pass


class InvalidJobOperationError(Exception):
    pass


class InvalidJobOperation(Exception):
    pass


class DequeueTimeout(Exception):
    pass


class ShutDownImminentException(Exception):
    def __init__(self, msg, extra_info):
        self.extra_info = extra_info
        super().__init__(msg)


class TimeoutFormatError(Exception):
    pass


class AbandonedJobError(Exception):
    pass


class StopRequested(Exception):
    pass


class NoRedisConnectionException(Exception):
    pass


class BaseTimeoutException(Exception):
    """Base exception for timeouts."""

    pass


class JobTimeoutException(BaseTimeoutException):
    """Raised when a job takes longer to complete than the allowed maximum
    timeout value.
    """

    pass


class HorseMonitorTimeoutException(BaseTimeoutException):
    """Raised when waiting for a horse exiting takes longer than the maximum
    timeout value.
    """

    pass
