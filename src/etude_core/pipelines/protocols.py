from typing import Protocol, runtime_checkable


@runtime_checkable
class PipelineJob(Protocol):
    """
    Protocol for any class that can be tracked by the job_scope context manager.
    It must identify itself with a PIPELINE_ID.
    """

    PIPELINE_ID: str
