import logging
from typing import Protocol, runtime_checkable

from etude_core.pipelines.protocols import PipelineJob
from etude_core.pipelines.scanner import MetadataScanHandler, FileToProcess
from etude_core.pipelines.base import FileHandler

logger = logging.getLogger(__name__)


@runtime_checkable
class JobContext(Protocol):
    """
    A protocol defining the context for a single processing job.
    """

    @property
    def handler_instance(self) -> PipelineJob: ...
    @property
    def job_name(self) -> str: ...
    @property
    def file_type(self) -> str: ...
    @property
    def file_id(self) -> int | None: ...
    @property
    def hash_id(self) -> int | None: ...

    @property
    def dataset_key(self) -> str: ...


class ScanJobContext:
    """Context for a folder-level MetadataScan job."""

    def __init__(self, handler: MetadataScanHandler):
        self._handler = handler
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
        return "Scan"


class FileJobContext:
    """Context for a file-level, per-table job."""

    def __init__(self, handler: FileHandler, file: FileToProcess, table_name: str):
        self._handler = handler
        self._file = file
        self._table_name = table_name

    @property
    def handler_instance(self) -> PipelineJob:
        return self._handler

    @property
    def job_name(self) -> str:
        return f"{self._handler.PIPELINE_ID}: {self._file.relative_path} [{self._table_name}]"

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
        return self._table_name
