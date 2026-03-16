import logging
import logging.config
import sys
from pathlib import Path
from typing import Any, Dict

from e2ude_core.config import AppSettings


def setup_logging(settings: AppSettings) -> None:
    """
    Configures logging using Python's dictConfig.
    This ensures file rotation, correct formatting, and granular control over libraries.
    """
    cfg = settings.logging

    # Ensure log directory exists if we are logging to a file
    if cfg.log_to_file:
        log_path = Path(cfg.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

    # Standard log format
    standard_format = cfg.format

    logging_config: Dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": standard_format,
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "simple": {
                "format": "%(levelname)s: %(message)s",
            },
        },
        "handlers": {
            "console": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": "standard",
            },
        },
        "loggers": {
            # Root logger catches everything else (e.g., libraries)
            "": {
                "handlers": ["console"],
                "level": "WARNING",
                "propagate": True,
            },
            # e2ude_core application code
            "e2ude_core": {
                "handlers": ["console"],
                "level": cfg.level.upper(),
                "propagate": False,  # Stop double logging
            },
            # 3rd Party: SQLAlchemy (noise reduction)
            "sqlalchemy.engine": {
                "handlers": ["console"],
                "level": "WARNING",  # Change to INFO to see SQL queries
                "propagate": False,
            },
            # 3rd Party: Alembic (migrations)
            "alembic": {
                "handlers": ["console"],
                "level": "INFO",
                "propagate": False,
            },
        },
    }

    # Conditionally add the File Handler if enabled
    if cfg.log_to_file:
        file_handler_config = {
            "level": cfg.level.upper(),
            "class": "logging.handlers.RotatingFileHandler",
            "filename": cfg.log_file,
            "maxBytes": cfg.rotation_size_mb * 1024 * 1024,
            "backupCount": cfg.rotation_backup_count,
            "formatter": "standard",
            "encoding": "utf8",
        }

        logging_config["handlers"]["file_rotate"] = file_handler_config

        # Attach file handler to relevant loggers
        logging_config["loggers"][""]["handlers"].append("file_rotate")
        logging_config["loggers"]["e2ude_core"]["handlers"].append("file_rotate")
        logging_config["loggers"]["sqlalchemy.engine"]["handlers"].append("file_rotate")

    # Apply configuration
    try:
        logging.config.dictConfig(logging_config)
    except Exception as e:
        print(f"Failed to configure logging: {e}", file=sys.stderr)
        # Fallback to basic config so we at least see something
        logging.basicConfig(level=logging.INFO)
