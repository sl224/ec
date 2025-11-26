import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from threading import Event, Thread
from typing import Dict, List

import sqlalchemy as sa
from tqdm import tqdm

from e2ude_core.context import EtlContext
from e2ude_core.orchestration.workflow import process_staged_directory

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class StagingJob:
    folder_id: int
    zip_path: Path
    stage_path: Path
    is_sentinel: bool = False


class StagingPipeline:
    def __init__(
        self,
        eng: sa.Engine,
        zip_paths: List[Path],
        folder_id_map: Dict[Path, int],
        staging_root: Path,
        num_consumers: int = 8,
        queue_size: int = 16,
    ):
        self.eng = eng
        self.zip_paths = zip_paths
        self.folder_id_map = folder_id_map
        self.staging_root = staging_root
        self.num_consumers = num_consumers
        self.queue_size = queue_size

        self.queue: Queue[StagingJob] = Queue(maxsize=queue_size)
        self.stop_event = Event()
        self.ctx = EtlContext.capture()

    def run(self):
        """
        Starts the pipeline threads and waits for completion.
        Tracks progress using TQDM.
        """
        # We track TOTAL zips to be processed.
        total_work = len(self.zip_paths)
        
        # TQDM is thread-safe for updates.
        # We use unit='zip' to give semantic meaning to the counters.
        with tqdm(total=total_work, desc="Processing Archives", unit="zip") as pbar:
            
            consumers = []
            for i in range(self.num_consumers):
                t = Thread(
                    target=self._consumer_task,
                    # Pass the pbar instance to the thread
                    args=(f"Consumer-{i}", pbar),
                    name=f"Consumer-{i}",
                )
                t.start()
                consumers.append(t)

            producer = Thread(target=self._producer_task, name="Producer")
            producer.start()

            try:
                producer.join()
                
                # Broadcast Sentinels (Poison Pills) to stop consumers
                sentinel = StagingJob(0, Path(""), Path(""), is_sentinel=True)
                for _ in range(self.num_consumers):
                    self.queue.put(sentinel)

                # Wait for all items in queue to be processed
                self.queue.join()
                
                # Ensure threads exit
                for c in consumers:
                    c.join()

            except KeyboardInterrupt:
                logger.warning("Pipeline interrupted. Stopping threads...")
                self.stop_event.set()
                # Drain queue to unblock threads so they can see stop_event
                while not self.queue.empty():
                    try:
                        self.queue.get_nowait()
                        self.queue.task_done()
                    except:
                        pass
                raise

    def _producer_task(self):
        """
        Producer: Unzips -> Explodes -> Queues
        """
        logger.info(f"[Producer] Started. Items: {len(self.zip_paths)}")

        for zip_path in self.zip_paths:
            if self.stop_event.is_set():
                break

            folder_id = self.folder_id_map.get(zip_path)
            if not folder_id:
                continue

            safe_name = f"{folder_id}_{zip_path.stem}"
            local_stage_path = self.staging_root / safe_name

            try:
                if local_stage_path.exists():
                    shutil.rmtree(local_stage_path)
                local_stage_path.mkdir(parents=True, exist_ok=True)

                logger.debug(f"[Producer] Staging {zip_path.name}...")

                # 1. Unpack Outer (Network Bound)
                shutil.unpack_archive(str(zip_path), str(local_stage_path), "zip")

                # 2. Explode Inner (Disk Bound - Flattening)
                for nested_zip in local_stage_path.rglob("*RSM_RawArchive.zip"):
                    try:
                        shutil.unpack_archive(
                            str(nested_zip), str(nested_zip.parent), "zip"
                        )
                        nested_zip.unlink()  # Cleanup archive
                    except Exception as e:
                        logger.warning(f"[Producer] Failed to explode {nested_zip}: {e}")

                # 3. Enqueue
                job = StagingJob(
                    folder_id=folder_id,
                    zip_path=zip_path,
                    stage_path=local_stage_path,
                )
                self.queue.put(job)

            except Exception as e:
                logger.error(f"[Producer] Failed to stage {zip_path}: {e}")
                if local_stage_path.exists():
                    shutil.rmtree(local_stage_path, ignore_errors=True)

        logger.info("[Producer] Done.")

    def _consumer_task(self, name: str, pbar: tqdm):
        """
        Consumer: Catalog -> Process -> Cleanup -> Update Progress
        """
        while not self.stop_event.is_set():
            try:
                job = self.queue.get(timeout=1)
            except:
                continue

            if job.is_sentinel:
                self.queue.task_done()
                break

            try:
                # Delegate actual logic to workflow
                process_staged_directory(
                    self.eng, job.folder_id, job.stage_path, self.ctx
                )
            except Exception as e:
                logger.error(
                    f"[{name}] Failed processing folder {job.folder_id}: {e}"
                )
            finally:
                # Immediate cleanup of SSD space
                try:
                    if job.stage_path.exists():
                        shutil.rmtree(job.stage_path)
                except OSError as e:
                    logger.warning(f"Failed to cleanup {job.stage_path}: {e}")

                # Mark item done in Queue
                self.queue.task_done()
                
                # Update Progress Bar
                # This reflects "One Zip Fully Processed (or Failed)"
                pbar.update(1)