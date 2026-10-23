"""Exceptions raised by the shared GX integration."""


class GXIntegrationError(RuntimeError):
    """Base error for GX configuration or execution failures."""


class GXProjectConfigurationError(GXIntegrationError):
    """Raised when the persistent GX project cannot be loaded safely."""


class GXSuiteNotFoundError(GXProjectConfigurationError):
    """Raised when a requested Expectation Suite is not registered."""


class GXExecutionError(GXIntegrationError):
    """Raised when GX cannot complete a validation run."""


class GXValidationError(GXIntegrationError):
    """Raised by callers that choose to enforce a failed validation."""
