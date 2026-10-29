"""Stable error categories exposed by the validation framework."""


class ValidationFrameworkError(RuntimeError):
    """Base class for framework failures, distinct from data-quality failures."""


class ValidationConfigurationError(ValidationFrameworkError):
    """The validation request, suite, adapter, or storage is misconfigured."""


class ValidationExecutionError(ValidationFrameworkError):
    """The selected validation engine failed to complete."""


class ValidationPersistenceError(ValidationFrameworkError):
    """The normalized validation summary could not be persisted."""


class SuiteNotFoundError(ValidationConfigurationError):
    """A requested versioned suite is absent from the Git suite registry."""


class QualityGateRejectedError(ValidationFrameworkError):
    """Validation completed but its policy decision does not authorize publish."""


class SnapshotMismatchError(QualityGateRejectedError):
    """The candidate identity changed after validation and must be revalidated."""


class MetadataExportError(ValidationFrameworkError):
    """A metadata sink rejected or could not persist validation metadata."""
