import ipaddress
import os
import socket
from tempfile import SpooledTemporaryFile
from urllib.parse import unquote, urljoin, urlparse

import requests

from .contracts import DownloadResult, FileCandidate, ScanResult, SourceDescriptor
from .exceptions import InvalidUrlError, PolicyViolationError
from .policies import MAX_FILE_BYTES, MAX_REDIRECTS, SUPPORTED_EXTENSIONS, SUPPORTED_MIME_TYPES


class DirectHttpAdapter:
    provider = "HTTP"

    @staticmethod
    def _validate_public_url(url: str) -> str:
        parsed = urlparse(url.strip())
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
            raise InvalidUrlError("Chỉ hỗ trợ URL HTTP/HTTPS công khai.")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if port not in {80, 443}:
            raise InvalidUrlError("Port URL không được phép.")
        try:
            addresses = {item[4][0] for item in socket.getaddrinfo(parsed.hostname, port)}
        except socket.gaierror as exc:
            raise InvalidUrlError("Không thể phân giải tên miền nguồn.") from exc
        for address in addresses:
            if not ipaddress.ip_address(address).is_global:
                raise InvalidUrlError("URL nội bộ hoặc không công khai không được phép.")
        return parsed.geturl()

    def _request(self, method: str, url: str, **kwargs):
        current = self._validate_public_url(url)
        for _ in range(MAX_REDIRECTS + 1):
            response = requests.request(method, current, allow_redirects=False, **kwargs)
            if response.is_redirect or response.is_permanent_redirect:
                location = response.headers.get("location")
                response.close()
                if not location:
                    raise InvalidUrlError("Redirect không hợp lệ.")
                current = self._validate_public_url(urljoin(current, location))
                continue
            return response
        raise InvalidUrlError("URL chuyển hướng quá nhiều lần.")

    @staticmethod
    def _filename(response, source_url: str) -> str:
        disposition = response.headers.get("content-disposition", "")
        marker = disposition.lower().find("filename=")
        if marker >= 0:
            value = disposition[marker + len("filename="):].split(";", 1)[0].strip(' "')
            if value:
                return os.path.basename(unquote(value))
        return os.path.basename(unquote(urlparse(response.url or source_url).path)) or "download.bin"

    def scan(self, source: SourceDescriptor) -> ScanResult:
        try:
            response = self._request("HEAD", source.original_url, timeout=10)
            if response.status_code >= 400 or response.status_code in {405, 501}:
                response.close()
                response = self._request("GET", source.original_url, stream=True, timeout=15,
                                         headers={"Range": "bytes=0-8191"})
            response.raise_for_status()
        except requests.RequestException as exc:
            raise InvalidUrlError(f"Không thể truy cập URL: {exc}") from exc
        name = self._filename(response, source.original_url)
        mime_type = response.headers.get("content-type", "").split(";", 1)[0] or None
        size_text = response.headers.get("content-length")
        size = int(size_text) if size_text and size_text.isdigit() and response.status_code != 206 else None
        extension = os.path.splitext(name)[1].lower()
        supported = extension in SUPPORTED_EXTENSIONS or mime_type in SUPPORTED_MIME_TYPES
        reason = None
        if not supported:
            reason = "Định dạng không được hỗ trợ"
        elif size is not None and size > MAX_FILE_BYTES:
            supported = False
            reason = "File vượt giới hạn 100 MB"
        candidate = FileCandidate(
            external_id=source.original_url, name=name, provider_reference=response.url or source.original_url,
            provider_reference_kind="HTTP_URL", mime_type=mime_type, size=size,
            etag=response.headers.get("etag"), supported=supported, unsupported_reason=reason,
        )
        response.close()
        return ScanResult(source, [candidate])

    def revalidate(self, source: SourceDescriptor, candidate: FileCandidate) -> FileCandidate:
        refreshed = self.scan(source).files[0]
        if candidate.etag and refreshed.etag and candidate.etag != refreshed.etag:
            raise PolicyViolationError("Nguồn đã thay đổi kể từ lúc Scan.")
        if not refreshed.supported:
            raise PolicyViolationError(refreshed.unsupported_reason or "File không được hỗ trợ.")
        return refreshed

    def download(self, source: SourceDescriptor, candidate: FileCandidate) -> DownloadResult:
        if not candidate.supported:
            raise PolicyViolationError(candidate.unsupported_reason or "File không được hỗ trợ.")
        try:
            response = self._request("GET", candidate.provider_reference, stream=True, timeout=(10, 60))
            response.raise_for_status()
            stream = SpooledTemporaryFile(max_size=8 * 1024 * 1024)
            total = 0
            for chunk in response.iter_content(1024 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if total > MAX_FILE_BYTES:
                    stream.close()
                    raise PolicyViolationError("File vượt giới hạn 100 MB.")
                stream.write(chunk)
            stream.seek(0)
            return DownloadResult(stream, candidate.name, candidate.mime_type)
        except requests.RequestException as exc:
            raise InvalidUrlError(f"Tải file thất bại: {exc}") from exc
