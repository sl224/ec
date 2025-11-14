import logging
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,  # <-- IMPORT
    Protocol,
    Tuple,
    Type,
    TypeAlias,
    Union,
    runtime_checkable,
)

import pandas as pd
import sqlalchemy as sa

from etude_core.orchestration.managers import JobManager
from etude_core.db import access as sql_io

logger = logging.getLogger(__name__)

# --- 1. TYPES & PROTOCOLS ---


@runtime_checkable
class HashVerifiableModel(Protocol):
    """Protocol for Tables that support Hash-Centric Deduplication."""

    __tablename__: str
    __table__: Any
    hash_id: Any


# RENAMED: Represents a logical grouping of data returned by the parser
class DatasetKey(Enum):
    pass


# RENAMED: The default key for single-table parsers
class StandardDataset(DatasetKey):
    PRIMARY = auto()


# --- FIX: The PayloadType alias was incorrect. It must include the key. ---
PayloadType: TypeAlias = List[Tuple[DatasetKey, Type[HashVerifiableModel], pd.DataFrame]]

# Parser returns either a specific Dict of Datasets, or a single DataFrame
ParserResult: TypeAlias = Union[Dict[DatasetKey, pd.DataFrame], pd.DataFrame]


# --- 2. THE HANDLER ---


class FileHandler:
    """
    Generic ETL Handler.

    Binds a 'Parser' (which produces Datasets) to 'Target Tables'.
    """

    def __init__(
        self,
        pipeline_id: str,
        parser_func: Callable[[Path], ParserResult],
        # Config: Map logical Datasets to physical Tables
        table_config: Union[
            Type[HashVerifiableModel], Dict[DatasetKey, Type[HashVerifiableModel]]
        ],
        # Optional: The Enum Class used for validation in complex mode
        dataset_enum: Type[DatasetKey] = None,
    ):
        self.PIPELINE_ID = pipeline_id
        self._parser_func = parser_func

        # --- NORMALIZATION LOGIC ---
        if isinstance(table_config, dict):
            # Complex Mode (Multi-Dataset)
            if not dataset_enum:
                raise ValueError(
                    f"[{pipeline_id}] Multi-table config requires 'dataset_enum' for validation."
                )
            self.TABLE_MAPPING = table_config
            self.DATASET_ENUM = dataset_enum
            self._is_simple_mode = False
        else:
            # Simple Mode (Single Dataset)
            self.TABLE_MAPPING = {StandardDataset.PRIMARY: table_config}
            self.DATASET_ENUM = StandardDataset
            self._is_simple_mode = True

        # --- STARTUP VALIDATION ---
        # Ensure every logical dataset defined in the contract is mapped to a table
        if not self._is_simple_mode:
            defined_datasets = set(self.DATASET_ENUM)
            mapped_datasets = set(self.TABLE_MAPPING.keys())

            missing = defined_datasets - mapped_datasets
            if missing:
                raise ValueError(
                    f"[{pipeline_id}] CONFIG ERROR: Parser defines datasets {missing} which are NOT mapped to tables."
                )

            extra = mapped_datasets - defined_datasets
            if extra:
                raise ValueError(
                    f"[{pipeline_id}] CONFIG ERROR: Config maps datasets {extra} which are NOT defined in the parser contract."
                )

    def run(
        self,
        eng: sa.Engine,
        hash_id: int,
        file_path: Path,
        job_updater: JobManager,
        keys_to_process: List[DatasetKey] = None,  # <-- FIX: ADDED
    ):
        logger.info(
            f"[{self.PIPELINE_ID}] Processing HashID {hash_id} for keys: {keys_to_process or 'ALL'}"
        )

        # 1. PARSE
        try:
            raw_data = self._parser_func(file_path)
        except Exception:
            raise

        # 2. NORMALIZE
        payload = self._normalize_payload(raw_data)

        # --- FIX: NEW FILTERING LOGIC ---
        # Filter the payload to *only* the keys this job is responsible for.
        if keys_to_process:
            payload = [
                (key, model, df)
                for key, model, df in payload
                if key in keys_to_process
            ]
        # --- END FIX ---

        if not payload:
            logger.info(f"[{self.PIPELINE_ID}] No data found for specified keys.")
            job_updater._rows_uploaded_in_scope = 0
            return

        # 3. UPLOAD
        try:
            # 'payload' is now pre-filtered
            total_rows = self._atomic_upload(eng, hash_id, payload, job_updater)
            job_updater._rows_uploaded_in_scope = total_rows
            logger.info(f"[{self.PIPELINE_ID}] Complete. Total rows: {total_rows}")
        except Exception as e:
            logger.error(f"[{self.PIPELINE_ID}] Upload failed: {e}", exc_info=True)
            raise

    def _normalize_payload(self, raw_data: ParserResult) -> PayloadType:
        payload: PayloadType = []  # Explicitly type

        # Case A: Simple Mode (DataFrame -> StandardDataset)
        if isinstance(raw_data, pd.DataFrame):
            if not self._is_simple_mode:
                raise ValueError(
                    f"[{self.PIPELINE_ID}] Parser returned DataFrame, but Handler expects Dict[{self.DATASET_ENUM.__name__}]."
                )

            target_table = self.TABLE_MAPPING[StandardDataset.PRIMARY]
            payload.append((StandardDataset.PRIMARY, target_table, raw_data))
            return payload

        # Case B: Complex Mode (Dict -> Mapped Keys)
        if isinstance(raw_data, dict):
            for key, df in raw_data.items():
                # Type Check
                if not isinstance(key, self.DATASET_ENUM):
                    if self._is_simple_mode:
                        raise ValueError(
                            f"[{self.PIPELINE_ID}] Parser returned Dict, but Handler is configured for single-table."
                        )
                    logger.warning(
                        f"[{self.PIPELINE_ID}] Invalid key {key}. Expected {self.DATASET_ENUM}."
                    )
                    continue

                target_table = self.TABLE_MAPPING[key]
                payload.append((key, target_table, df))
            return payload

        if raw_data is None:
            return []

        raise TypeError(
            f"[{self.PIPELINE_ID}] Unexpected parser return type: {type(raw_data)}"
        )

    def _atomic_upload(self, eng, hash_id, payload: PayloadType, job_updater) -> int:
        # Standard Atomic Logic
        total_rows = 0
        row_count_sum = sum(len(item[2]) for item in payload)
        job_updater.mark_running(f"Uploading {row_count_sum} rows...")
        with eng.begin() as conn:
            for data_key, table_model, df in payload:
                if df.empty:
                    continue
                df = df.copy()
                df["hash_id"] = hash_id
                # --- FIX: Use .name to store the string representation ---
                df["dataset_key"] = data_key.name
                conn.execute(
                    table_model.__table__.delete().where(
                        table_model.hash_id == hash_id,
                        # --- FIX: Ensure delete is also per-key ---
                        table_model.dataset_key == data_key.name,
                    )
                )
                sql_io.bulk_upload(df, conn, table_model.__table__)
                total_rows += len(df)
        return total_rows