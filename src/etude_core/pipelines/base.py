import logging
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
)

import pandas as pd
import sqlalchemy as sa

from etude_core.orchestration.managers import JobManager
from etude_core.db import access as sql_io
from etude_core.db.models import Base  # Import your Base

logger = logging.getLogger(__name__)

# --- 1. TYPES & PROTOCOLS ---


@runtime_checkable
class HashVerifiableModel(Protocol):
    __tablename__: str
    __table__: Any
    hash_id: Any


ParserResult: TypeAlias = Dict[Type[Base], pd.DataFrame]

# --- FIX: The payload no longer needs the redundant table_name string ---
PayloadType: TypeAlias = List[Tuple[Type[HashVerifiableModel], pd.DataFrame]]

# --- 2. THE REFACTORED HANDLER ---


class FileHandler:
    # ... (__init__ is unchanged) ...
    def __init__(
        self,
        pipeline_id: str,
        parser_func: Callable[[Path], ParserResult],
        table_config: List[Type[HashVerifiableModel]],
    ):
        self.PIPELINE_ID = pipeline_id

        if not table_config:
            raise ValueError(f"[{pipeline_id}] 'table_config' list cannot be empty.")

        self.expected_models = table_config

        self.model_map: Dict[str, Type[Base]] = {
            model.__tablename__: model for model in table_config
        }

        self._parser = parser_func

        for model in self.expected_models:
            if not hasattr(model, "hash_id"):
                logger.warning(
                    f"[{pipeline_id}] Model {model.__name__} is missing required 'hash_id' column."
                )

    # ... (run is unchanged) ...
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
        keys_to_process: List[str],  # List of table names
    ) -> PayloadType:
        """
        Converts the parser's {Model: df} map into the internal payload,
        while filtering for the table names this job is responsible for.
        """
        # --- FIX: Use new PayloadType ---
        payload: PayloadType = []
        if not model_map:
            return []

        for model, df in model_map.items():
            table_name = model.__tablename__

            if table_name not in self.model_map:
                logger.warning(
                    f"[{self.PIPELINE_ID}] Parser returned Model {model.__name__} which was not in 'table_config'."
                )
                continue

            if keys_to_process and table_name not in keys_to_process:
                continue

            # --- FIX: Append the simpler tuple ---
            payload.append((model, df))

        return payload

    def _atomic_upload(self, eng, hash_id, payload: PayloadType, job_updater) -> int:
        total_rows = 0
        row_count_sum = sum(len(item[1]) for item in payload)  # <-- Index 1 for df
        job_updater.mark_running(f"Uploading {row_count_sum} rows...")
        with eng.begin() as conn:
            # --- FIX: Unpack the simpler tuple ---
            for model, df in payload:
                if df.empty:
                    continue
                df = df.copy()
                df["hash_id"] = hash_id

                conn.execute(model.__table__.delete().where(model.hash_id == hash_id))

                sql_io.bulk_upload(df, conn, model.__table__)
                total_rows += len(df)
        return total_rows
