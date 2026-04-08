"""Configuration loading utilities for the NYC mobility friction project."""

from pathlib import Path
from typing import Any

import yaml

from nyc_mobility_friction.paths import get_project_paths


def load_config(filename: str = "base.yaml") -> dict[str, Any]:
    """Load a YAML config file from the configs directory.

    Args:
        filename: Name of the YAML file (with or without .yaml extension).
            Default is `base.yaml`.

    Returns:
        The parsed configuration as a dictionary.

    Raises:
        FileNotFoundError: If the requested config file does not exist.
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
