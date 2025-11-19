#%%
import sqlalchemy as sa
# import logging

# Import models used for queries
from e2ude_core.db.models import (
    ProcessingSession,
    ProcessingJob,
    StatusEnum,
    FolderMetadata,
    # FileMetadata,
)
from e2ude_core.pipelines.scanner import MetadataScanHandler
from e2ude_core.registry import HANDLER_REGISTRY
from e2ude_core.db.access import get_engine
from e2ude_core.config import settings

from typing import List
from pathlib import Path
from multiprocessing import Pool
from tqdm import tqdm
from re import search as re_search
import os
import pandas as pd
import re
from datetime import datetime
import logging
import e2ude_core.db.access as sql_io

E2D_SHARED_DRIVE = Path("//rsiny1-ilsfs/RSM")

def main(eng, folder_cfg, years_filter, use_cache=False):
    search_dirs = get_search_dirs(years_filter)
    zip_paths = multi_process_scan(search_dirs)
    folder_meta_df = strip_metadata(zip_paths)
    with eng.connect() as conn:
        sql_io.bulk_upload(folder_meta_df,
                           conn,
                           table_instance=folder_cfg.Table)

def strip_metadata(zip_paths):
    seen = set()
    meta = []
    for zp in zip_paths:

        if zp in seen:
            logging.info(f"Skipping {zp}\n...already exists in DB")
            continue

        match = re.search(r"([0-9]+)_([0-9]{8}_[0-9]{6})", zp.name)
        if not match:
            logging.warning(f"Could not strip info from {zp}")
            continue

        buno, dt_str = match.groups()
        dt = datetime.strptime(dt_str, "%Y%m%d_%H%M%S")
        meta.append((str(zp), buno, dt))

    df = pd.DataFrame(meta, columns=["FolderPath", "Buno", "FolderDatetime"])

    return df


def get_search_dirs(years_filter=None) -> List[Path]:
    """
    Returns a list of directories to crawl
    Sometimes we only want to crawl the most recent directories
    or sometimes we want to crawl everything

    Example Path to zip
    \\\\rsiny1-ilsfs\RSM\167931\2023\01\*.zip
    """
    root_dir = E2D_SHARED_DRIVE
    buno_pattern = r"\b\d{6}\b"
    # Search all the folders
    if not years_filter:
        search_dirs = [
            buno_dir
            for buno_dir in root_dir.iterdir()
            if re_search(buno_pattern, buno_dir.name)
        ]
    # Search only folders from the years filter
    else:
        search_dirs = []
        buno_dirs = [
            buno_dir
            for buno_dir in root_dir.iterdir()
            if re_search(buno_pattern, buno_dir.name)
        ]
        for buno_dir in buno_dirs:
            for year_dir in buno_dir.iterdir():
                if year_dir.name in years_filter:
                    search_dirs.append(year_dir)
    return search_dirs


def find_zips_worker(search_folder: Path):
    return list(search_folder.glob("**/*RSM*.fpkg.e2d.zip"))


def multi_process_scan(search_dirs, procs=None) -> List:
    zip_paths = []
    total_cpus = os.cpu_count()
    if procs is None:
        procs = total_cpus
    else:
        procs = min(total_cpus, procs)
    with Pool(procs) as p:
        multi_proc_iter = p.imap_unordered(find_zips_worker, search_dirs)
        with tqdm(
            desc=f"Scanning {E2D_SHARED_DRIVE} for RSM Zips", total=len(search_dirs)
        ) as pbar:
            for zip_paths_sub in multi_proc_iter:
                zip_paths.extend(zip_paths_sub)
                pbar.update(1)
    return zip_paths


def get_data(eng, where_session_has_status=None):
    stmt = (
        sa.select(FolderMetadata.id)
            .join_from(FolderMetadata,
                       ProcessingSession,
                       ProcessingSession.folder_id == FolderMetadata.id,
                       isouter=True)
            .where(ProcessingSession.folder_id == None)
    )
    print(stmt)
    with eng.connect() as conn:
        ret = list(map(tuple, conn.execute(stmt).fetchall()))
    return ret


if __name__ == '__main__':
    eng = get_engine(settings.database)
    ret = get_data(eng)
    print(ret[:10])
