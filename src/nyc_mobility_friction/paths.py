"""Project path utilities for the nyc mobility friction project.

This module provides a single source of truth for all directory paths in the project.
Every other module should import paths from here instead of hard-coding paths or using 
relative imports. This guarantees reproducibility across notebooks, scripts, and CI 
environments.
"""

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

@dataclass(frozen=True)
class ProjectPaths:
    """All project directories in one imutable object.

    Use `get_project_paths()` to obtain as instance. All paths are absolute and cached
    for performance
    """

    root: Path
    configs: Path
    data: Path
    raw: Path
    interim: Path
    processed: Path
    artifacts: Path
    reports: Path
    notebooks: Path
    dashboards: Path
    src: Path

@lru_cache(maxsize=1)
def get_project_paths() -> ProjectPaths:
    """Rerturn a single object containing every single path."""
    root = Path(__file__).resolve().parents[2]
    data = root / "data"
    return ProjectPaths(
        root=root,
        configs=root / "configs",
        data= data,
        raw=data / "raw",
        interim=data / "interim",
        processed= data / "processed",
        artifacts=root / "artifacts",
        reports=root / "report",
        notebooks=root / "notebooks",
        dashboards = root / "dashboards",
        src = root / "src"
        )
