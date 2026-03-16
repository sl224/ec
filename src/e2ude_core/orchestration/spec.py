from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha1
from typing import Optional, Sequence, Type

from e2ude_core.db.models import Base
from e2ude_core.runtime_files import FileType, PipelineId


class JobSubjectKind(StrEnum):
    FILE_ARTIFACT = "FILE_ARTIFACT"
    METADATA_SCAN = "METADATA_SCAN"


@dataclass(frozen=True)
class JobTarget:
    label: str
    key: str


def build_job_target(target_models: Sequence[Type[Base]]) -> JobTarget:
    normalized = tuple(
        model
        for _, model in sorted(
            {
                model.__tablename__: model
                for model in target_models
                if getattr(model, "__tablename__", "")
            }.items()
        )
    )
    if not normalized:
        raise ValueError(
            "target_models must contain at least one model with a table name"
        )

    if len(normalized) == 1:
        table_name = normalized[0].__tablename__
        return JobTarget(label=table_name, key=table_name)

    digest = sha1(
        "\n".join(model.__tablename__ for model in normalized).encode("utf-8")
    ).hexdigest()[:16]
    return JobTarget(label="BATCH", key=f"batch:{len(normalized)}:{digest}")


@dataclass(frozen=True)
class JobRunResult:
    rows_uploaded: int = 0
    completion_message: str | None = None


@dataclass(frozen=True)
class JobSpec:
    pipeline_id: PipelineId
    job_name: str
    target_label: str
    target_key: str
    handler_version: int
    subject_kind: JobSubjectKind

    file_id: Optional[int] = None
    hash_id: Optional[int] = None
    file_type: FileType | None = None

    def __post_init__(self):
        if self.subject_kind == JobSubjectKind.FILE_ARTIFACT and self.file_type is None:
            raise ValueError("File artifact jobs must declare a file_type")
        if (
            self.subject_kind == JobSubjectKind.METADATA_SCAN
            and self.file_type is not None
        ):
            raise ValueError("Metadata scan jobs cannot declare a file_type")

    @classmethod
    def for_metadata_scan(
        cls,
        *,
        pipeline_id: PipelineId,
        job_name: str,
        target_label: str,
        target_key: str,
        handler_version: int,
    ) -> "JobSpec":
        return cls(
            pipeline_id=pipeline_id,
            job_name=job_name,
            target_label=target_label,
            target_key=target_key,
            handler_version=handler_version,
            subject_kind=JobSubjectKind.METADATA_SCAN,
        )

    @classmethod
    def for_file(
        cls,
        *,
        pipeline_id: PipelineId,
        job_name: str,
        target_label: str,
        target_key: str,
        handler_version: int,
        file_type: FileType,
        file_id: Optional[int] = None,
        hash_id: Optional[int] = None,
    ) -> "JobSpec":
        return cls(
            pipeline_id=pipeline_id,
            job_name=job_name,
            target_label=target_label,
            target_key=target_key,
            handler_version=handler_version,
            subject_kind=JobSubjectKind.FILE_ARTIFACT,
            file_id=file_id,
            hash_id=hash_id,
            file_type=file_type,
        )
