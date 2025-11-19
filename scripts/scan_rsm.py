#%%
import sqlalchemy as sa
# import logging
import sqlalchemy as sa

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


def get_data(eng, with_status=None):
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
