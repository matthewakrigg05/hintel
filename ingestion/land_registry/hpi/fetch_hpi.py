"""Command-line entry point for the Land Registry HPI ingestion pipeline.

The module supports both ``python -m`` execution and running this file
directly from the repository. Direct execution needs the repository root on
``sys.path`` so the shared ``ingestion`` package can be imported.
"""

import sys
from pathlib import Path


if __package__ in {None, ""}:
    # Direct file execution starts with the HPI folder on sys.path.
    repository_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(repository_root))
    from ingestion.land_registry.hpi.pipeline import run_land_registry_all
else:
    from .pipeline import run_land_registry_all


if __name__ == "__main__":
    run_land_registry_all()
