import logging
from typing import Protocol, runtime_checkable

from etude_core.pipelines.protocols import PipelineJob
from etude_core.pipelines.scanner import MetadataScanHandler, FileToProcess
from etude_core.pipelines.base import FileHandler, DatasetKey

logger = logging.getLogger(__name__)


@runtime_checkable
class JobContext(Protocol):
    """
    A polymorphic contract for a job's context.

    This object provides all necessary details to the `job_scope`
    manager, removing the need for `if/else` logic within the scope.
    """

    @property
    def handler_instance(self) -> PipelineJob:
        """The handler instance responsible for executing the job."""
        ...

    @property
    def job_name(self) -> str:
        """The human-readable name for the job (used in logs)."""
        ...

    @property
    def file_type(self) -> str:
        """The file type associated with the job (or 'N/A')."""
        ...

    @property
    def file_id(self) -> int | None:
        """The FileMetadata ID, if available."""
        ...

    @property
    def hash_id(self) -> int | None:
        """The FileHashRegistry ID, if available."""
        ...

    @property
    def dataset_key(self) -> str:
        """Returns the string name of the dataset key (e.g., 'PRIMARY' or 'Scan')."""
        ...


class ScanJobContext:
    """Context for a folder-level MetadataScan job."""

    def __init__(self, handler: MetadataScanHandler):
        self._handler = handler
        # folder_id is an attribute on MetadataScanHandler
        self._folder_id = getattr(handler, "folder_id", "UnknownFolder")

    @property
    def handler_instance(self) -> PipelineJob:
        return self._handler

    @property
    def job_name(self) -> str:
        return f"{self._handler.PIPELINE_ID}: FolderID {self._folder_id}"

    @property
    def file_type(self) -> str:
        return "N/A"

    @property
    def file_id(self) -> int | None:
        return None

    @property
    def hash_id(self) -> int | None:
        return None

    @property
    def dataset_key(self) -> str:
        return "Scan"  # The explicitly defined key for scan jobs


class FileJobContext:
    """Context for a file-level, per-dataset job."""

    def __init__(self, handler: FileHandler, file: FileToProcess, key_enum: DatasetKey):
        self._handler = handler
        self._file = file
        self._key_enum = key_enum
        self._key_str = key_enum.name

    @property
    def handler_instance(self) -> PipelineJob:
        return self._handler

    @property
    def job_name(self) -> str:
        return (
            f"{self._handler.PIPELINE_ID}: {self._file.relative_path} [{self._key_str}]"
        )

    @property
    def file_type(self) -> str:
        return self._file.file_type

    @property
    def file_id(self) -> int | None:
        return self._file.file_id

    @property
    def hash_id(self) -> int | None:
        return self._file.hash_id

    @property
    def dataset_key(self) -> str:
        return self._key_str
