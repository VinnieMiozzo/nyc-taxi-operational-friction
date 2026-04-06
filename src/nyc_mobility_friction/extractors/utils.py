"""
Shared utilities for all extractors in the NYC Mobility Friction project.
"""

from pathlib import Path
import logging
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nyc_mobility_friction.paths import get_project_paths


def setup_logger(log_name: str = __name__) -> logging.Logger:
    """Configure root logging to console + file once per pipeline run."""
    paths = get_project_paths()
    log_dir = paths.root / "logs" / "extract"
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"{log_name}_{timestamp}.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return log_path


def ensure_external_dirs() -> None:
    """Create data/raw/external/ folder using the centralized project paths.

    creates:
        data/raw/external/
    """
    paths = get_project_paths()
    paths.raw.mkdir(parents=True, exist_ok=True)
    (paths.raw / "external").mkdir(exist_ok=True)


def ensure_raw_dirs() -> None:
    """Create data/raw/taxi/ folder using the centralized project paths.

    creates:
        data/raw/taxi/
    """
    paths = get_project_paths()
    paths.raw.mkdir(parents=True, exist_ok=True)
    (paths.raw / "taxi").mkdir(exist_ok=True)

def make_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session
