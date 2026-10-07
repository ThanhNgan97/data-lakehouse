import re
from urllib.parse import urlparse

from .contracts import SourceDescriptor
from .exceptions import InvalidUrlError, UnsupportedProviderError


DRIVE_FOLDER = re.compile(r"/folders/([A-Za-z0-9_-]+)")
DRIVE_FILE = re.compile(r"/file/d/([A-Za-z0-9_-]+)")


def resolve_url(value: str) -> SourceDescriptor:
    url = value.strip()
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise InvalidUrlError("Chỉ hỗ trợ URL HTTP/HTTPS hợp lệ.")
    host = parsed.hostname.lower().rstrip(".")
    if host == "drive.google.com":
        folder = DRIVE_FOLDER.search(parsed.path)
        if folder:
            return SourceDescriptor("GOOGLE_DRIVE", "FOLDER", url, folder.group(1))
        file_match = DRIVE_FILE.search(parsed.path)
        if file_match:
            return SourceDescriptor("GOOGLE_DRIVE", "FILE", url, file_match.group(1))
        raise UnsupportedProviderError("Không nhận diện được file hoặc folder Google Drive.")
    return SourceDescriptor("HTTP", "FILE", url, url)
