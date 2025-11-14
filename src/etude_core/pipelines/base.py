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
    Optional,
)

import pandas as pd
import sqlalchemy as sa

from etude_core.orchestration.managers import JobManager
from etude_core.db import access as sql_io
from etude_core.db.models import Base  # Import your Base

logger = logging.getLogger(__name__)


@runtime_checkable
class HashVerifiableModel(Protocol):
    """Protocol for SQLAlchemy models that are keyed by `hash_id`."""

    __tablename__: str
    __table__: Any
    hash_id: Any


# The parser's contract is to return a map of {Model Class: DataFrame}
ParserResult: TypeAlias = Dict[Type[Base], pd.DataFrame]
# The internal payload for uploading is a list of (Model_Class, DataFrame)
PayloadType: TypeAlias = List[Tuple[Type[HashVerifiableModel], pd.DataFrame]]


class FileHandler:
    """
    A generic, configurable ETL handler.

    This class orchestrates the parsing and atomic uploading of data for
    a specific file type. It is configured with a list of expected
    SQLAlchemy models and a parser function that returns data for those models.
    """

    PIPELINE_ID: str
    expected_models: List[Type[HashVerifiableModel]]
    model_map: Dict[str, Type[HashVerifiableModel]]
    _parser: Callable[[Path], ParserResult]

    def __init__(
        self,
        pipeline_id: str,
        parser_func: Callable[[Path], ParserResult],
        table_config: List[Type[HashVerifiableModel]],
    ):
        """
        Initializes the FileHandler.

        Args:
            pipeline_id: A unique string ID for this pipeline (e.g., "rsm_mcdata").
            parser_func: The pure function that parses a file path and returns data.
            table_config: A list of the SQLAlchemy Model classes this handler
                          is responsible for (e.g., [Rpcs, NavData, ...]).
        """
        self.PIPELINE_ID = pipeline_id
        self._parser = parser_func

        if not table_config:
            raise ValueError(f"[{pipeline_id}] 'table_config' list cannot be empty.")

        # The source of truth for what this handler produces
        self.expected_models = table_config

        # A convenience map for {table_name: ModelClass}
        self.model_map: Dict[str, Type[HashVerifiableModel]] = {
            model.__tablename__: model for model in table_config
        }

        # Validate that all configured models are hash-verifiable
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
        """
        Executes the ETL process for a single file hash.

        Args:
            eng: The SQLAlchemy engine.
            hash_id: The unique hash_id for this file's content.
            file_path: The full path to the extracted file to be parsed.
            job_updater: The job manager object for status updates.
            keys_to_process: A specific list of table names to process.
                             If None, processes all tables.
        """
        logger.info(
            f"[{self.PIPELINE_ID}] Processing HashID {hash_id} for keys: {keys_to_process or 'ALL'}"
        )
        try:
            # 1. Parse (expensive I/O)
            model_to_df_map = self._parser(file_path)
        except Exception:
            logger.error(
                f"[{self.PIPELINE_ID}] Parser failed for {file_path}", exc_info=True
            )
            raise

        # 2. Filter payload based on what this job is for
        payload = self._normalize_and_filter(model_to_df_map, keys_to_process)

        if not payload:
            logger.info(f"[{self.PIPELINE_ID}] No data found for specified keys.")
            job_updater._rows_uploaded_in_scope = 0
            return

        # 3. Upload data
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
        keys_to_process: Optional[List[str]],  # List of table names
    ) -> PayloadType:
        """
        Converts the parser's {Model: df} map into the internal payload,
        while filtering for the table names this job is responsible for.
        """
        payload: PayloadType = []
        if not model_map:
            return []

        for model, df in model_map.items():
            table_name = model.__tablename__

            # Validation: Did the parser return a model we don't manage?
            if table_name not in self.model_map:
                logger.warning(
                    f"[{self.PIPELINE_ID}] Parser returned Model {model.__name__} which was not in 'table_config'."
                )
                continue

            # Filtering: If keys are specified, skip tables not in the list
            if keys_to_process and table_name not in keys_to_process:
                continue

            payload.append((model, df))

        return payload

    def _atomic_upload(self, eng, hash_id, payload: PayloadType, job_updater) -> int:
        """
        *Performs a transactional "delete-by-hash-and-insert" upload.*
        """
        total_rows = 0
        row_count_sum = sum(len(item[1]) for item in payload)  # item[1] is the df
        job_updater.mark_running(f"Uploading {row_count_sum} rows...")

        with eng.begin() as conn:
            for model, df in payload:
                if df.empty:
                    continue

                df_copy = df.copy()
                df_copy["hash_id"] = hash_id

                # Idempotency: Delete all data for this hash_id from this table
                conn.execute(model.__table__.delete().where(model.hash_id == hash_id))

                # Bulk upload the new data
                sql_io.bulk_upload(df_copy, conn, model.__table__)
                total_rows += len(df_copy)

        return total_rows
