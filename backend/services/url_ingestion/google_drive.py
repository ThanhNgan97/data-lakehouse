import os
import requests
import html
import mimetypes
import re
from tempfile import SpooledTemporaryFile
from urllib.parse import quote

from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

from .contracts import DownloadResult, FileCandidate, ScanResult, SourceDescriptor
from .exceptions import AccessDeniedError, PolicyViolationError
from .policies import MAX_FILE_BYTES, MAX_FILES_PER_JOB, SUPPORTED_EXTENSIONS, SUPPORTED_MIME_TYPES


DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
FOLDER_MIME = "application/vnd.google-apps.folder"
EXPORTS = {
    "application/vnd.google-apps.document": ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx", "DOCX"),
    "application/vnd.google-apps.spreadsheet": ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx", "XLSX"),
    "application/vnd.google-apps.presentation": ("application/pdf", ".pdf", "PDF"),
}


class GoogleDriveAdapter:
    provider = "GOOGLE_DRIVE"

    def __init__(self):
        credential_file = os.getenv("GOOGLE_DRIVE_SERVICE_ACCOUNT_FILE")
        self.api_key = os.getenv("GOOGLE_DRIVE_API_KEY")
        self.configured = bool(credential_file or self.api_key)
        self.public_fallback = not self.configured
        if credential_file:
            credentials = service_account.Credentials.from_service_account_file(
                credential_file, scopes=[DRIVE_SCOPE],
            )
            self.session = AuthorizedSession(credentials)
        elif self.api_key:
            # API key supports public resources; users only need to paste a public link.
            self.session = requests.Session()
        else:
            self.session = requests.Session()
        # The app may run with a development/sandbox proxy that cannot reach Google.
        # Provider traffic must not inherit that unrelated process-level proxy.
        if hasattr(self.session, "trust_env"):
            self.session.trust_env = False

    @staticmethod
    def _public_download_url(file_id: str) -> str:
        return f"https://drive.google.com/uc?export=download&id={quote(file_id)}"

    def _public_candidate(self, file_id: str, fallback_name: str | None = None) -> FileCandidate:
        response = self.session.head(self._public_download_url(file_id), allow_redirects=True, timeout=20)
        if response.status_code in {401, 403, 404} or "accounts.google.com" in response.url:
            raise AccessDeniedError("File Google Drive không cho phép hệ thống tải xuống.")
        response.raise_for_status()
        disposition = response.headers.get("content-disposition", "")
        name_match = re.search(r'filename="?([^";]+)', disposition, re.IGNORECASE)
        name = html.unescape(name_match.group(1)) if name_match else (fallback_name or file_id)
        size_text = response.headers.get("content-length")
        size = int(size_text) if size_text and size_text.isdigit() else None
        mime_type = mimetypes.guess_type(name)[0] or response.headers.get("content-type")
        extension = os.path.splitext(name)[1].lower()
        supported = extension in SUPPORTED_EXTENSIONS or mime_type in SUPPORTED_MIME_TYPES
        reason = None
        if not supported:
            reason = "Định dạng Google Drive không được hỗ trợ"
        elif size is not None and size > MAX_FILE_BYTES:
            supported = False
            reason = "File vượt giới hạn 100 MB"
        return FileCandidate(
            external_id=file_id, name=name, provider_reference=file_id,
            provider_reference_kind="GOOGLE_PUBLIC_FILE_ID", mime_type=mime_type,
            size=size, supported=supported, unsupported_reason=reason,
            metadata={"public_link": True, "original_mime_type": mime_type},
        )

    def _scan_public(self, source: SourceDescriptor) -> ScanResult:
        if source.resource_type == "FILE":
            return ScanResult(source, [self._public_candidate(source.resource_id)])
        response = self.session.get(source.original_url, allow_redirects=True, timeout=30)
        if "accounts.google.com" in response.url:
            raise AccessDeniedError(
                "Folder Google Drive chưa bật quyền 'Bất kỳ ai có đường liên kết'."
            )
        response.raise_for_status()
        pattern = re.compile(
            r'data-id="([A-Za-z0-9_-]{10,})"[^>]{0,1500}?data-tooltip="([^"]+)"',
            re.DOTALL,
        )
        files = []
        seen = set()
        supported_name = re.compile(r'^(.+?\.(?:pdf|docx|xlsx|xls|csv|tsv|json|parquet))(?:\s|$)', re.IGNORECASE)
        for file_id, tooltip in pattern.findall(response.text):
            if file_id in seen:
                continue
            name_match = supported_name.match(html.unescape(tooltip))
            if not name_match:
                continue
            seen.add(file_id)
            files.append(self._public_candidate(file_id, name_match.group(1)))
            if len(files) >= MAX_FILES_PER_JOB:
                break
        if not files:
            raise AccessDeniedError(
                "Không tìm thấy file được hỗ trợ trong folder công khai này."
            )
        return ScanResult(source, files, ["Đang sử dụng chế độ Google Drive public link."])

    def _ensure_access(self, source: SourceDescriptor):
        if self.configured:
            return
        try:
            response = self.session.get(source.original_url, allow_redirects=False, timeout=10)
            location = response.headers.get("location", "")
        except requests.RequestException:
            location = ""
        if "accounts.google.com" in location:
            raise AccessDeniedError(
                "Folder Google Drive chưa cho phép hệ thống xem. "
                "Hãy mở Chia sẻ → Quyền truy cập chung → Bất kỳ ai có đường liên kết, rồi Scan lại."
            )
        self.public_fallback = True

    def _get(self, url: str, **kwargs):
        if self.api_key:
            params = dict(kwargs.pop("params", {}) or {})
            params["key"] = self.api_key
            kwargs["params"] = params
        response = self.session.get(url, timeout=30, **kwargs)
        if response.status_code in {401, 403, 404}:
            raise AccessDeniedError("Service account không có quyền truy cập tài nguyên Google Drive.")
        response.raise_for_status()
        return response

    @staticmethod
    def _candidate(item: dict, relative_path: str | None = None) -> FileCandidate:
        mime = item.get("mimeType")
        name = item.get("name") or item["id"]
        export = EXPORTS.get(mime)
        metadata = {"modifiedTime": item.get("modifiedTime"), "original_mime_type": mime}
        if export:
            export_mime, extension, export_format = export
            if not name.lower().endswith(extension):
                name += extension
            metadata.update({"export_mime_type": export_mime, "export_format": export_format})
            supported = True
            output_mime = export_mime
            size = None
        else:
            output_mime = mime
            size_text = item.get("size")
            size = int(size_text) if size_text else None
            supported = os.path.splitext(name)[1].lower() in SUPPORTED_EXTENSIONS or mime in SUPPORTED_MIME_TYPES
        reason = None
        if not supported:
            reason = "Định dạng Google Drive không được hỗ trợ"
        elif size is not None and size > MAX_FILE_BYTES:
            supported = False
            reason = "File vượt giới hạn 100 MB"
        return FileCandidate(
            external_id=item["id"], name=name, relative_path=relative_path,
            mime_type=output_mime, size=size, provider_reference=item["id"],
            provider_reference_kind="GOOGLE_FILE_ID", provider_revision=item.get("version"),
            etag=item.get("md5Checksum"), supported=supported,
            unsupported_reason=reason, metadata=metadata,
        )

    def _metadata(self, file_id: str) -> dict:
        fields = "id,name,mimeType,size,md5Checksum,modifiedTime,version"
        return self._get(f"https://www.googleapis.com/drive/v3/files/{quote(file_id)}?fields={quote(fields)}").json()

    def scan(self, source: SourceDescriptor) -> ScanResult:
        self._ensure_access(source)
        if self.public_fallback:
            return self._scan_public(source)
        if source.resource_type == "FILE":
            return ScanResult(source, [self._candidate(self._metadata(source.resource_id))])
        files = []
        page_token = None
        while len(files) < MAX_FILES_PER_JOB:
            params = {
                "q": f"'{source.resource_id}' in parents and trashed = false",
                "pageSize": min(100, MAX_FILES_PER_JOB - len(files)),
                "fields": "nextPageToken,files(id,name,mimeType,size,md5Checksum,modifiedTime,version)",
            }
            if page_token:
                params["pageToken"] = page_token
            payload = self._get("https://www.googleapis.com/drive/v3/files", params=params).json()
            for item in payload.get("files", []):
                if item.get("mimeType") == FOLDER_MIME:
                    continue  # MVP is one folder level only.
                files.append(self._candidate(item, item.get("name")))
            page_token = payload.get("nextPageToken")
            if not page_token:
                break
        warnings = []
        if page_token:
            warnings.append(f"Chỉ lấy {MAX_FILES_PER_JOB} file đầu tiên theo policy MVP.")
        return ScanResult(source, files, warnings)

    def revalidate(self, source: SourceDescriptor, candidate: FileCandidate) -> FileCandidate:
        if candidate.metadata.get("public_link"):
            refreshed = self._public_candidate(candidate.external_id, candidate.name)
            if candidate.size is not None and refreshed.size is not None and candidate.size != refreshed.size:
                raise PolicyViolationError("File Google Drive đã thay đổi kích thước kể từ lúc Scan.")
            return refreshed
        refreshed = self._candidate(self._metadata(candidate.external_id), candidate.relative_path)
        if candidate.provider_revision and refreshed.provider_revision != candidate.provider_revision:
            raise PolicyViolationError("File Google Drive đã thay đổi kể từ lúc Scan.")
        if not refreshed.supported:
            raise PolicyViolationError(refreshed.unsupported_reason or "File không được hỗ trợ.")
        return refreshed

    def download(self, source: SourceDescriptor, candidate: FileCandidate) -> DownloadResult:
        if candidate.metadata.get("public_link"):
            response = self.session.get(self._public_download_url(candidate.external_id), allow_redirects=True, stream=True, timeout=(10, 60))
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
        export_mime = candidate.metadata.get("export_mime_type")
        file_id = quote(candidate.external_id)
        if export_mime:
            url = f"https://www.googleapis.com/drive/v3/files/{file_id}/export?mimeType={quote(export_mime)}"
        else:
            url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
        response = self._get(url, stream=True)
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
