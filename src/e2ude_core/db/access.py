import logging
import numpy as np
import pandas as pd
from sqlalchemy import URL, Connection, Table, create_engine

logger = logging.getLogger(__name__)


class BadParameter(ValueError):
    pass


def get_engine(db_settings, default_pool_size: int = 5, fast_executemany: bool = True, echo: bool = False):
    """
    Creates and returns a SQLAlchemy engine.
    
    Args:
        db_settings: The database configuration object.
        default_pool_size: The fallback pool size (usually settings.worker_threads)
                           if db_settings.pool_size is None.
    """
    # Determine effective pool size
    # Priority: Configured DB Pool Size > Global Worker Threads > Default(5)
    configured_size = getattr(db_settings, "pool_size", None)
    effective_pool_size = configured_size if configured_size is not None else default_pool_size
    
    # Base engine arguments common to most DBs
    base_args = {"echo": echo}

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
            
            # Construct MSSQL specific args
            mssql_args = base_args.copy()
            mssql_args["fast_executemany"] = fast_executemany
            mssql_args["pool_size"] = effective_pool_size
            
            # Since db_settings is validated as MSSQLConfig, these fields are guaranteed
            mssql_args["max_overflow"] = db_settings.max_overflow
            mssql_args["pool_timeout"] = db_settings.pool_timeout
            mssql_args["pool_pre_ping"] = db_settings.pool_pre_ping
            
            logger.debug(f"Initializing MSSQL Engine with pool_size={effective_pool_size}")
            return create_engine(url_object, **mssql_args)

        case "sqlite3":
            sqlite_args = base_args.copy()
            
            if db_settings.in_memory:
                url_object = "sqlite:///:memory:"
                from sqlalchemy.pool import StaticPool
                # In-memory SQLite must use StaticPool to share state across threads
                sqlite_args["poolclass"] = StaticPool
                # StaticPool doesn't support sizing args, so we don't add them
            else:
                url_object = f"sqlite:///{db_settings.db_location}"
                # Standard SQLite file doesn't support high concurrency well, 
                # but we can bump the pool size anyway if not using NullPool
                sqlite_args["pool_size"] = effective_pool_size
                
                # Yes, these apply to file-based SQLite if pool_size is set (implies QueuePool)
                if hasattr(db_settings, "max_overflow"):
                    sqlite_args["max_overflow"] = db_settings.max_overflow
                if hasattr(db_settings, "pool_timeout"):
                    sqlite_args["pool_timeout"] = db_settings.pool_timeout

            logger.debug(f"Initializing SQLite Engine ({'Memory' if db_settings.in_memory else 'File'})")
            return create_engine(url_object, **sqlite_args)

        case _:
            raise ValueError(
                f"Unsupported DB type: {db_settings.type}"
            )


def bulk_upload(
    df: pd.DataFrame,
    conn: Connection,
    sa_table: Table,
    chunksize: int = 10000,
):
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

    for start_idx in range(0, total_rows, chunksize):
        df_chunk = df_aligned.iloc[start_idx : start_idx + chunksize]
        clean_chunk = df_chunk.replace({np.nan: None, pd.NA: None})

        conn.execute(
            sa_table.insert(),
            clean_chunk.to_dict(orient="records"),
        )