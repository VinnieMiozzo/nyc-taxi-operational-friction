"""Configuration loading utilities for the nyc mobility friction project.

This module provides a single source of truth for loading YAML configuration files from
the `config/` directory. All downstream code (models, scenarios, budget optimizer, etc.
) should import config via `load_config()` rather then reading YAML directly.
"""

from pathlib import Path 
from typing import Any

import yaml

from mmm_budget_reallocation.paths import get_project_paths


def load_config(filename: str = "base.yaml") -> dict[str, Any]:
    """Load a YAML config file from the configs directory.
    
    Args:
        filename: Name of the YAML file (with or without .yaml extension).
                  Default is `base.yaml`
                  
    Returns:
        The parsed configuration as a dictionary.

    Raises:
        FileNotFoundError: If the requested config file does not exists.
        ValueError: If the file is not valid YAML or does not parse to a dict.
    """
    paths = get_project_paths()
    config_path = paths.configs / filename

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with Path(config_path).open("r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ValueError(f"Config file must load to a dictionary: {config_path}")

    return config
