"""Command-line entry point for the EPC ingestion pipeline.

The module supports both ``python -m`` execution and running this file
directly from the repository. Direct execution needs the repository root on
``sys.path`` so the shared ``ingestion`` package can be imported.
"""

import sys
from pathlib import Path


if __package__ in {None, ""}:
    repository_root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(repository_root))
    from ingestion.epc.england_wales.pipeline import main
else:
    from .pipeline import main


if __name__ == "__main__":
    main()
