import logging
import numpy as np
import pandas as pd
from sqlalchemy import URL, Connection, Table, create_engine
from tqdm import tqdm

logger = logging.getLogger(__name__)


class BadParameter(ValueError):
    pass


def get_engine(db_settings, fast_executemany: bool = True, echo: bool = False):
    """
    Creates and returns a SQLAlchemy engine based on the loaded pydantic settings.
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
    chunksize: int = 2000,
    tqdm_description: str = "Uploading",
    show_progress: bool = False,
    leave: bool = True,
):
    """
    Uploads a DataFrame to a database table in chunks.
    """
    if df.empty:
        return

    total_rows = len(df)

    with tqdm(
        total=total_rows,
        desc=tqdm_description,
        unit="rows",
        leave=leave,
        disable=not show_progress,
    ) as pbar:
        # Slice via `iloc` for memory-efficient chunking.
        for start_idx in range(0, total_rows, chunksize):
            df_chunk = df.iloc[start_idx : start_idx + chunksize]

            # Sanitize chunk just before upload, converting pandas/numpy nulls to SQL NULL.
            clean_chunk = df_chunk.replace({np.nan: None, pd.NA: None})

            conn.execute(
                sa_table.insert(),
                clean_chunk.to_dict(orient="records"),
            )
            pbar.update(len(df_chunk))
