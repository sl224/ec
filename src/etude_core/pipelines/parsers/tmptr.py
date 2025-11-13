import pandas as pd
from pathlib import Path

def parse_tmptr_dataframe(file_path: Path) -> pd.DataFrame:
    """
    Reads a TMPTR_LOG file and returns a cleaned DataFrame.
    """
    df = pd.read_csv(
        file_path,
        header=None,
        names=["afmc", "date", "time", "category", "temp_f_raw", "temp_c_raw"],
        dtype=str,
    )

    
    for col, raw_col in [("temp_f", "temp_f_raw"), ("temp_c", "temp_c_raw")]:
        # 1. Clean whitespace
        s = df[raw_col].str.strip()
        
        # 2. Filter: Keep only strings exactly length 4 (matches your 'if len(stoken) == 4')
        #    and slice off the last character (the unit).
        valid_mask = (s.str.len() == 4)
        numeric_part = s.where(valid_mask).str[:-1]
        
        # 3. Convert to numeric, coercing errors (malformed numbers) to NaN
        #    Int64 allows for integers mixed with NaN (pd.NA)
        df[col] = pd.to_numeric(numeric_part, errors='coerce').astype("Int64")
    # --- REFACTORED SECTION END ---

    date_str = df["date"].str.strip()
    time_str = df["time"].str.strip()

    # Fix '00' year prefix
    date_str_fixed = date_str.str.replace(r"^00", "20", regex=True)
    datetime_full = date_str_fixed + " " + time_str

    # Convert to datetime
    df["datetime"] = pd.to_datetime(datetime_full, format="%Y%m%d %H:%M:%S.%f")
    df["line_number"] = df.index + 1

    return df.drop(columns=["date", "time", "temp_f_raw", "temp_c_raw"])