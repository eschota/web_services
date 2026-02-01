"""Logger configuration for RenderFin MCP Server."""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Log directory relative to workspace
LOG_DIR = Path("./vars/logs")
LOG_FILE = LOG_DIR / "renderfin.log"

# Max log file size: 5MB, keep 3 backups
MAX_BYTES = 5 * 1024 * 1024
BACKUP_COUNT = 3


def setup_logger(name: str = "renderfin") -> logging.Logger:
    """Configure and return the application logger.
    
    Writes to both file (./vars/logs/renderfin.log) and stderr.
    Disables duplicate logging from MCP/uvicorn.
    """
    # Create log directory
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Get or create logger
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers on re-init
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)
    logger.propagate = False  # Prevent duplicate logs
    
    # Log format
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(event)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        defaults={"event": "GENERAL"}
    )
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Stderr handler (for MCP client visibility)
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.INFO)
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)
    
    # Suppress noisy loggers
    for noisy in ["httpx", "httpcore", "mcp", "uvicorn", "asyncio"]:
        logging.getLogger(noisy).setLevel(logging.WARNING)
    
    return logger


# Singleton logger instance
logger = setup_logger()


def log_request(tool: str, params: dict) -> None:
    """Log incoming tool request."""
    logger.info(
        f"Tool: {tool}, Params: {params}",
        extra={"event": "REQUEST"}
    )


def log_queued(task_id: str, output_file: str) -> None:
    """Log task added to queue."""
    logger.info(
        f"Task {task_id} queued, output: {output_file}",
        extra={"event": "QUEUED"}
    )


def log_success(task_id: str, output_url: str, local_path: str) -> None:
    """Log successful generation."""
    logger.info(
        f"Task {task_id} completed: {output_url} -> {local_path}",
        extra={"event": "SUCCESS"}
    )


def log_error(task_id: str, error: str) -> None:
    """Log generation or request error."""
    logger.error(
        f"Task {task_id} failed: {error}",
        extra={"event": "ERROR"}
    )


def log_timeout(task_id: str) -> None:
    """Log task timeout."""
    logger.warning(
        f"Task {task_id} timed out after 5 minutes",
        extra={"event": "TIMEOUT"}
    )
