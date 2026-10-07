from dataclasses import dataclass, field
from typing import BinaryIO, Protocol


@dataclass(frozen=True)
class SourceDescriptor:
    provider: str
    resource_type: str
    original_url: str
    resource_id: str | None = None


@dataclass
class FileCandidate:
    external_id: str
    name: str
    provider_reference: str
    provider_reference_kind: str
    relative_path: str | None = None
    mime_type: str | None = None
    size: int | None = None
    etag: str | None = None
    provider_revision: str | None = None
    supported: bool = True
    unsupported_reason: str | None = None
    metadata: dict = field(default_factory=dict)


@dataclass
class ScanResult:
    source: SourceDescriptor
    files: list[FileCandidate]
    warnings: list[str] = field(default_factory=list)


@dataclass
class DownloadResult:
    stream: BinaryIO
    filename: str
    mime_type: str | None


class SourceAdapter(Protocol):
    provider: str

    def scan(self, source: SourceDescriptor) -> ScanResult: ...
    def revalidate(self, source: SourceDescriptor, candidate: FileCandidate) -> FileCandidate: ...
    def download(self, source: SourceDescriptor, candidate: FileCandidate) -> DownloadResult: ...
