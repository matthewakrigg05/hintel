"""Command-line entry point for Scottish EPC ingestion."""

import sys
from pathlib import Path


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from ingestion.epc.scotland.pipeline import main
else:
    from .pipeline import main


if __name__ == "__main__":
    main()
