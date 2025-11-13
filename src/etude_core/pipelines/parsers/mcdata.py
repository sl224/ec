# %%
# __________________________________________________________ # nopep8
# Uncomment to run code from jupyter # nopep8
# import sys  # nopep8
# import os

# ROOT_DIR = os.path.dirname(os.path.abspath("../"))  # nopep8
# sys.path.append(ROOT_DIR)  # nopep8
# print(ROOT_DIR)  # nopep8
# __________________________________________________________ # nopep8


from datetime import datetime
from multiprocessing import Pool
from tqdm import tqdm
from typing import List, Dict
from pandas import DataFrame, read_feather, read_sql, read_sql
from etude_core.db import access as sql_io
from etude_core.utils.clean import filtercast_df

from etude_core.pipelines.parsers.mcdata_helpers.sa_tables import (
    LCS_TEMP_CONFIG,
    NAV_DATA_CONFIG,
    RPCS_PRES_CONFIG,
    RFC_DB_CONFIG,
    PFC_DB_CONFIG,
    RPCS_CONFIG,
    RADAR_STATE_CONFIG,
    ROTOSCAN_CONFIG,
    MC_IN_DISCR_CONFIG,
)

from sqlalchemy import inspect

from etude_core.pipelines.parsers.mcdata_helpers.scrape import (
    scrape_nav_record,
    scrape_rdr_state_record,
    scrape_rfc_db_record,
    scrape_pfc_db_record,
    scrape_lcs_temp_record,
    scrape_rotoscan_record,
    scrape_rpcs_pres_record,
    scrape_rpcs_record,
    scrape_mc_in_discr,
)


def multi_process_rows(scrape_func, records, chunksize=1) -> List:
    parsed_rows = []
    with Pool() as p:
        multi_proc_iter = p.imap_unordered(scrape_func, records, chunksize=chunksize)
        with tqdm(total=len(records)) as pbar:
            for parsed_rec in multi_proc_iter:
                if parsed_rec:
                    parsed_rows.append(parsed_rec)
                pbar.update(1)
    return parsed_rows


def pipeline_factory(**kwargs) -> Dict:
    pipelines = {}
    # config_parser_bundles = [
    #     (NAV_DATA_CONFIG, scrape_nav_record),
    #     (RPCS_PRES_CONFIG, scrape_rpcs_pres_record),
    #     (RADAR_STATE_CONFIG, scrape_rdr_state_record),
    #     (ROTOSCAN_CONFIG, scrape_rotoscan_record),
    #     (RPCS_CONFIG, scrape_rpcs_record),
    #     (RFC_DB_CONFIG, scrape_rfc_db_record),
    #     (PFC_DB_CONFIG, scrape_pfc_db_record),
    #     (LCS_TEMP_CONFIG, scrape_lcs_temp_record),
    #     (MC_IN_DISCR_CONFIG, scrape_mc_in_discr),
    # ]
    config_parser_bundles = [
        (PFC_DB_CONFIG, scrape_pfc_db_record),
    ]

    for config, parser in config_parser_bundles:
        pipelines[config.Table.name] = E2D_DataPipeline(config, parser, **kwargs)

    return pipelines


from collections import namedtuple

Record = namedtuple("Record", "FolderID, LineNumber, RawText")


class E2D_DataPipeline:
    """
    ETL Pipelines for each MCData defined by the MCDATA.satables
    """

    def __init__(
        self,
        sa_config,
        scrape_func,
        chunksize=1000,
        multi_process=False,
        select_top=None,
        read_all_records=False,
        top=None,
        src_server_name: str,
        dest_server_name: str,
        db_name: str,
    ):
        self.scrape_func = scrape_func
        self.record_type = sa_config.record_type
        self.table_name = sa_config.Table.name
        self.chunksize = chunksize
        self.select_top = select_top
        self.multi_process = multi_process
        self.sa_config = sa_config
        self.read_all_records = read_all_records
        self.top = top
        self.src_server_name = src_server_name
        self.dest_server_name = dest_server_name
        self.db_name = db_name

    def scrape(self, data: DataFrame) -> DataFrame:
        REQ_COLUMNS = ["FolderID", "LineNumber", "RawText"]
        missing_cols = set(REQ_COLUMNS) - set(data.columns)
        if len(missing_cols) > 0:
            raise ValueError(f"Missing columns {missing_cols}")
        # Transform
        if self.multi_process:
            rows = [Record(*row) for row in data[REQ_COLUMNS].values]
            parsed_rows = multi_process_rows(
                self.scrape_func, rows, chunksize=min(self.chunksize, len(rows))
            )
        else:
            parsed_rows = []
            for row in data.itertuples():
                parsed_row = self.scrape_func(row)
                cols = self.sa_config.get_table_cols()
                assert len(parsed_row) == len(cols)
                parsed_rows.append(parsed_row)

        parsed_df = DataFrame(parsed_rows, columns=self.sa_config.get_table_cols())
        return parsed_df

    def sync(self, show_progress=False, replace_table=False, use_cache=False) -> None:
        # Extract
        raw_data_df = self.get_record_data(use_cache)
        if raw_data_df is not None and raw_data_df.empty:
            raise Exception("Error: No data to parse")

        # Transform
        parsed_df = self.scrape(raw_data_df)

        # Type cast
        dtypes = self.sa_config.get_dtypes()
        # multi_process_rows(filtercast_df, dfs)
        fil_df = filtercast_df(parsed_df, dtypes, show_progress=show_progress)

        fil_df.to_feather(f"{self.table_name}_fil.feather")
        eng = sql_io.get_engine(self.dest_server_name, self.db_name)
        sync_table = sql_io.SyncTable(eng, table_instance=self.sa_config.Table)
        if replace_table:
            sync_table.drop_table()

        # Load
        sync_table.atomic_bulk_upload(fil_df, show_progress=show_progress)

    def get_unparsed_records(self) -> DataFrame:
        top_str = f"TOP ({self.top})" if self.top else ""
        eng = sql_io.get_engine(self.src_server_name, self.db_name)
        schema = self.sa_config.Table.schema
        self.table_exists = inspect(eng).has_table(self.table_name, schema=schema)
        # Pull the unparsed text fields
        if self.table_exists and not self.read_all_records:
            print("reading diff")
            query = f"""
                        select {top_str}
                            recs.FolderID,
                            recs.LineNumber,
                            recs.RawText
                        from
                            [{schema}].[{self.table_name}] as rec_type_table
                            right join E2D_MCDATA.RAW_TEXT as recs
                            on rec_type_table.FolderID = recs.FolderID
                            and rec_type_table.LineNumber = recs.LineNumber
                        where
                            rec_type_table.FolderID is NULL
                            and rec_type_table.LineNumber is NULL
                            and recs.RecordType = '{self.record_type}'
                        """
        else:
            print("reading all records")
            query = f"""
                        select {top_str}
                            recs.FolderID,
                            recs.LineNumber,
                            recs.RawText
                        from E2D_MCDATA.RAW_TEXT as recs
                        where recs.RecordType = '{self.record_type}'
                    """
        return read_sql(query, eng)

    def get_record_data(self, use_cache: bool) -> DataFrame:
        """
        Read from cache if exists else pull from DB
        """
        # WARN: Assumes you want to read cache from the same day
        timestamp = datetime.now().today().strftime("%y%m%d")
        # filenames = glob(f'.*_{self.table_name}.feather')
        file_name = f"{timestamp}_{self.table_name}.feather"
        df = None
        if use_cache:
            print(f"Trying to read {self.table_name} from feather")
            try:
                df = read_feather(file_name)
                df.drop("index", inplace=True, errors="ignore")
            except Exception:
                print("Could not read from feather, reading from sql")
                df = self.get_unparsed_records()
                print("Writing to feather")
                df.reset_index().to_feather(file_name)
        else:
            print("Reading from sql")
            df = self.get_unparsed_records()
            print("Writing to feather")
            df.reset_index().to_feather(file_name)
            print(df)

        return df


def get_upstream():
    query = "select FolderID, LineNumber from E2D_MC_IN_DISCR"
    # eng = sql_io.get_engine(...) # This needs db_settings from config
    return read_sql(query, eng)


# %%
if __name__ == "__main__":
    # Test all pipelines
    pipelines_dict = pipeline_factory(read_all_records=False, multi_process=True)
    # sync_table.drop_table()
    # %%
    pipelines_dict["PFC_DB"].sync(show_progress=True, use_cache=False)


# %%
