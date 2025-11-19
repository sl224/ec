import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Protocol,
    Tuple,
    Type,
    TypeAlias,
    runtime_checkable,
    Optional,
)

import pandas as pd
import sqlalchemy as sa

from e2ude_core.orchestration.managers import JobManager
from e2ude_core.db import access as sql_io
from e2ude_core.db.models import Base

logger = logging.getLogger(__name__)


@runtime_checkable
class HashVerifiableModel(Protocol):
    """Protocol for SQLAlchemy models that are keyed by `hash_id`."""
    __tablename__: str
    __table__: Any
    hash_id: Any


ParserResult: TypeAlias = Dict[Type[Base], pd.DataFrame]
PayloadType: TypeAlias = List[Tuple[Type[HashVerifiableModel], pd.DataFrame]]


class BaseHandler(ABC):
    """
    Abstract Base Class for all ETL Handlers.
    Enforces the contract that the Orchestrator relies on.
    """
    PIPELINE_ID: str
    VERSION: int = 1
    expected_models: List[Type[Base]]

    @abstractmethod
    def run(
        self,
        eng: sa.Engine,
        hash_id: int,
        file_path: Path,
        job_updater: JobManager,
        keys_to_process: List[str] = None,
    ):
        """
        Execute the handler's logic.
        Must report status/rows to job_updater.
        """
        pass


class FileHandler(BaseHandler):
    """
    Standard Handler for files that map to one or more database tables.
    Uses a pure parser function and performs atomic replacement.
    """

    def __init__(
        self,
        pipeline_id: str,
        parser_func: Callable[[Path], ParserResult],
        table_config: List[Type[HashVerifiableModel]],
        version: int = 1,
    ):
        self.PIPELINE_ID = pipeline_id
        self.VERSION = version
        self._parser = parser_func

        if not table_config:
            raise ValueError(f"[{pipeline_id}] 'table_config' list cannot be empty.")

        self.expected_models = table_config
        self.model_map = {
            model.__tablename__: model for model in table_config
        }

        for model in self.expected_models:
            if not hasattr(model, "hash_id"):
                logger.warning(
                    f"[{pipeline_id}] Model {model.__name__} is missing required 'hash_id' column."
                )

    def run(
        self,
        eng: sa.Engine,
        hash_id: int,
        file_path: Path,
        job_updater: JobManager,
        keys_to_process: List[str] = None,
    ):
        logger.info(
            f"[{self.PIPELINE_ID}] Processing HashID {hash_id} for keys: {keys_to_process or 'ALL'}"
        )
        try:
            model_to_df_map = self._parser(file_path)
        except Exception:
            logger.error(
                f"[{self.PIPELINE_ID}] Parser failed for {file_path}", exc_info=True
            )
            raise

        payload = self._normalize_and_filter(model_to_df_map, keys_to_process)

        if not payload:
            logger.info(f"[{self.PIPELINE_ID}] No data found for specified keys.")
            job_updater._rows_uploaded_in_scope = 0
            return

        try:
            total_rows = self._atomic_upload(eng, hash_id, payload, job_updater)
            job_updater._rows_uploaded_in_scope = total_rows
            logger.info(f"[{self.PIPELINE_ID}] Complete. Total rows: {total_rows}")
        except Exception as e:
            logger.error(f"[{self.PIPELINE_ID}] Upload failed: {e}", exc_info=True)
            raise

    def _normalize_and_filter(
        self,
        model_map: ParserResult,
        keys_to_process: Optional[List[str]],
    ) -> PayloadType:
        payload: PayloadType = []
        if not model_map:
            return []

        for model, df in model_map.items():
            table_name = model.__tablename__
            if table_name not in self.model_map:
                continue
            if keys_to_process and table_name not in keys_to_process:
                continue
            payload.append((model, df))

        return payload

    def _atomic_upload(self, eng, hash_id, payload: PayloadType, job_updater) -> int:
        total_rows = 0
        row_count_sum = sum(len(item[1]) for item in payload)
        job_updater.mark_running(f"Uploading {row_count_sum} rows...")

        with eng.begin() as conn:
            for model, df in payload:
                if df.empty:
                    continue

                df_copy = df.copy()
                df_copy["hash_id"] = hash_id
                conn.execute(model.__table__.delete().where(model.hash_id == hash_id))
                sql_io.bulk_upload(df_copy, conn, model.__table__)
                total_rows += len(df_copy)

        return total_rows