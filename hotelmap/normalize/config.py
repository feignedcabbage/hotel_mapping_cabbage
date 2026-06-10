"""Config loading and merging.

A run is driven by one country config (e.g. configs/india.yaml). The shared
provider / property-type / country lookup files live alongside it and are loaded
together into a single dict so the rest of the pipeline takes one `config` arg.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

NORMALIZATION_VERSION = "v1"


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text()) or {}


def load_config(config_path: str) -> dict[str, Any]:
    """Load the run config plus the sibling lookup configs.

    The lookup files (providers.yaml, property_type_maps.yaml, country_maps.yaml)
    are expected in the same directory as the run config.
    """
    run_path = Path(config_path)
    config = _load_yaml(run_path)

    config_dir = run_path.parent
    config["providers_config"] = _load_yaml(config_dir / "providers.yaml")
    config["property_type_maps"] = _load_yaml(config_dir / "property_type_maps.yaml")
    config["country_maps"] = _load_yaml(config_dir / "country_maps.yaml")
    config["state_maps"] = _load_yaml(config_dir / "state_maps.yaml")

    config.setdefault("normalization_version", NORMALIZATION_VERSION)
    return config
