import pandas as pd
import logging
from typing import Dict, Type
from datetime import datetime

from etude_core.db.base_session import Base

logger = logging.getLogger(__name__)


def get_model_dtypes(model: Type[Base]) -> Dict[str, Type]:
    """
    Inspects an SQLAlchemy model and returns a map of
    `{column_name: python_type}` for all non-primary-key columns.
    """
    col_dtype = {}
    for col in model.__table__.columns:
        if not col.primary_key:
            col_dtype[col.name] = col.type.python_type
    return col_dtype


def clean_dataframe_from_model(df: pd.DataFrame, model: Type[Base]) -> pd.DataFrame:
    """
    Cleans a raw DataFrame using fast, vectorized pandas functions,
    casting columns to the types defined in the SQLAlchemy model.
    """
    dtypes = get_model_dtypes(model)

    # Operate on an explicit copy to avoid pandas' SettingWithCopyWarning.
    df = df.copy()

    for col_name, py_type in dtypes.items():
        if col_name not in df.columns:
            logger.warning(
                f"Column {col_name} from model {model.__name__} not in DataFrame."
            )
            continue

        try:
            # Numeric types
            if py_type in (int, float, complex):
                s = pd.to_numeric(df[col_name], errors="coerce")

                # Use pandas' nullable Int64 for integer columns.
                if py_type is int:
                    s = s.astype("Int64")

                df[col_name] = s

            # Datetime types
            elif py_type is datetime:
                # Parse with an explicit datetime format for performance and stability.
                # Matches common MCData timestamp format.
                datetime_format = "%m/%d/%Y %H:%M:%S"

                df[col_name] = pd.to_datetime(
                    df[col_name], errors="coerce", format=datetime_format
                )

            # Handle boolean
            elif py_type is bool:
                # Map common string representations to boolean values.
                bool_map = {
                    "true": True,
                    "1": True,
                    "t": True,
                    "false": False,
                    "0": False,
                    "f": False,
                }

                # Coerce to string, lowercase, map, and use nullable BooleanDtype.
                df[col_name] = (
                    df[col_name]
                    .astype(str)
                    .str.lower()
                    .map(bool_map)
                    .astype("boolean")  # Use pandas' nullable boolean type
                )

            # String types
            elif py_type is str:
                # Replace common null placeholders and clean whitespace.
                df[col_name] = (
                    df[col_name]
                    .replace(["None", "nan", ""], pd.NA)
                    .astype(str)
                    .str.strip()
                )
                # Handle edge case where 'astype' creates literal "None" or "nan"
                df[col_name] = df[col_name].replace(["None", "nan"], pd.NA)

        except Exception as e:
            logger.error(f"Failed to cast {col_name} in {model.__name__}: {e}")
            # On failure, just fill the col with nulls to be safe
            df[col_name] = pd.NA

    return df
