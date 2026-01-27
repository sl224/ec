import logging
import logging.handlers
import multiprocessing
from typing import Optional

# Global variable to hold the queue in worker processes
queue: Optional[multiprocessing.Queue] = None


def worker_configurer(log_queue: multiprocessing.Queue, level: str):
    """
    Configures the logging for a worker process.
    Instead of writing to file/stdout, it pushes LogRecords to the queue.
    """
    h = logging.handlers.QueueHandler(log_queue)
    root = logging.getLogger()

    # Clear existing handlers (inherited from parent on fork) to avoid dupes
    if root.handlers:
        root.handlers = []

    root.addHandler(h)
    root.setLevel(level.upper())


def listener_configurer(
    log_file: str, level: str, rotation_size_mb: int, backup_count: int, fmt: str
):
    """
    Configures the listener process which is the ONLY one writing to disk.
    """
    root = logging.getLogger()

    # 1. File Handler
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=rotation_size_mb * 1024 * 1024,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_formatter = logging.Formatter(fmt)
    file_handler.setFormatter(file_formatter)

    # 2. Console Handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(fmt)
    console_handler.setFormatter(console_formatter)

    root.addHandler(file_handler)
    root.addHandler(console_handler)
    root.setLevel(level.upper())


def listener_process(log_queue: multiprocessing.Queue, config: dict):
    """
    The target function for the dedicated logging process.
    """
    listener_configurer(
        config["log_file"],
        config["level"],
        config["rotation_size_mb"],
        config["rotation_backup_count"],
        config["format"],
    )

    while True:
        try:
            record = log_queue.get()
            if record is None:  # Sentinel to stop
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception:
            import sys
            import traceback

            print("Log Queue Problem:", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
