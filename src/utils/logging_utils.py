"""Logging utilities for standardized logger configuration across the pipeline."""
from __future__ import annotations
import logging
from typing import Optional
from .config import config

_LOGGERS_CREATED = False

def configure_root_logging(force: bool = False) -> None:
    """Configure root logging once using settings from config.
    Args:
        force: If True, reconfigure even if already configured.
    """
    global _LOGGERS_CREATED
    if _LOGGERS_CREATED and not force:
        return
    config.ensure_directories()
    log_file = config.get_file_path("logs")
    handlers: list[logging.Handler] = []
    file_handler = logging.FileHandler(log_file)
    stream_handler = logging.StreamHandler()
    handlers.extend([file_handler, stream_handler])
    logging.basicConfig(
        level=getattr(logging, config.LOGGING_SETTINGS.get("level", "INFO")),
        format=config.LOGGING_SETTINGS.get("format", "%(asctime)s - %(levelname)s - %(message)s"),
        handlers=handlers,
        force=force,
    )
    _LOGGERS_CREATED = True

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a configured logger. Ensures root configuration."""
    configure_root_logging()
    return logging.getLogger(name or __name__)

__all__ = ["get_logger", "configure_root_logging"]
