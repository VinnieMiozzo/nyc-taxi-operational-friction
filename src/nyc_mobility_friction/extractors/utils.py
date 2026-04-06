"""
Shared utilities for all extractors in the NYC Mobility Friction project.
"""

from pathlib import Path
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nyc_mobility_friction.paths import get_project_paths


def setup_logger(name: str = __name__) -> logging.Logger:
    """Configure and return a logger with consistent formatting."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True  # ensure config is applied even if logging was set earlier
    )
    return logging.getLogger(name)


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
