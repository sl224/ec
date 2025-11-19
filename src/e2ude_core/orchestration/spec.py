from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class JobSpec:
    pipeline_id: str
    job_name: str
    target_name: str     # Renamed from dataset_key
    handler_version: int # New field
    
    file_id: Optional[int] = None
    hash_id: Optional[int] = None