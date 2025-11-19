from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class JobSpec:
    pipeline_id: str
    job_name: str
    target_name: str
    handler_version: int

    file_id: Optional[int] = None
    hash_id: Optional[int] = None
    file_type: Optional[str] = None
