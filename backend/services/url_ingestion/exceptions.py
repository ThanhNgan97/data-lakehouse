class UrlIngestionError(Exception):
    code = "URL_INGESTION_ERROR"


class InvalidUrlError(UrlIngestionError):
    code = "INVALID_URL"


class UnsupportedProviderError(UrlIngestionError):
    code = "UNSUPPORTED_PROVIDER"


class AccessDeniedError(UrlIngestionError):
    code = "ACCESS_DENIED"


class PolicyViolationError(UrlIngestionError):
    code = "POLICY_VIOLATION"


class SourceChangedError(UrlIngestionError):
    code = "SOURCE_CHANGED"
