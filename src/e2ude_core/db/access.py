import logging
import numpy as np
import pandas as pd
from sqlalchemy import URL, Connection, Table, create_engine

logger = logging.getLogger(__name__)


class BadParameter(ValueError):
    pass


def get_engine(db_settings, fast_executemany: bool = True, echo: bool = False):
    """
    Creates and returns a SQLAlchemy engine.
    """
    engine_args = {"echo": echo}
    url_object = None

    match db_settings.type:
        case "mssql":
            url_object = URL.create(
                drivername="mssql+pyodbc",
                host=db_settings.server_name,
                database=db_settings.db_name,
                query={
                    "driver": db_settings.driver,
                    "trusted_connection": db_settings.trusted_connection,
                },
            )
            # SQLAlchemy handles fast_executemany automatically if enabled here
            engine_args["fast_executemany"] = fast_executemany

        case "sqlite3":
            if db_settings.in_memory:
                url_object = "sqlite:///:memory:"
            else:
                url_object = f"sqlite:///{db_settings.db_location}"

        case _:
            raise ValueError(
                "Pydantic should have thrown an error for unsupported DB types"
            )

    if url_object is None:
        raise ValueError("Database URL object was not created. Check configuration.")

    return create_engine(url_object, **engine_args)


def bulk_upload(
    df: pd.DataFrame,
    conn: Connection,
    sa_table: Table,
    chunksize: int = 10000,
):
    """
    Uploads a DataFrame to a database table using SQLAlchemy Core.
    Safe for all data types (Timestamps, Integers, etc).
    """
    if df.empty:
        return

    # Align columns: only upload columns that exist in both DataFrame and Table
    table_cols = [c.name for c in sa_table.columns]
    common_cols = [c for c in table_cols if c in df.columns]

    if not common_cols:
        logger.warning(
            f"No matching columns found for table {sa_table.name}. Skipping."
        )
        return

    df_aligned = df[common_cols]
    total_rows = len(df_aligned)

    # Chunking loop to prevent memory spikes
    for start_idx in range(0, total_rows, chunksize):
        # Use .iloc for positional slicing
        df_chunk = df_aligned.iloc[start_idx : start_idx + chunksize]

        # Sanitize: Convert NaN to None (SQL NULL)
        clean_chunk = df_chunk.replace({np.nan: None, pd.NA: None})

        # Use SQLAlchemy Core (Fast enough, type-safe)
        conn.execute(
            sa_table.insert(),
            clean_chunk.to_dict(orient="records"),
        )
