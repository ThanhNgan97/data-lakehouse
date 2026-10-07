from .direct_http import DirectHttpAdapter
from .exceptions import UnsupportedProviderError


def adapter_for(provider: str):
    if provider == "HTTP":
        return DirectHttpAdapter()
    if provider == "GOOGLE_DRIVE":
        from .google_drive import GoogleDriveAdapter
        return GoogleDriveAdapter()
    raise UnsupportedProviderError(f"Provider {provider} chưa được hỗ trợ.")
