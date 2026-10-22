"""Command-line entry point for the Land Registry Price Paid pipeline."""

import sys
from pathlib import Path


if __package__ in {None, ""}:
	repository_root = Path(__file__).resolve().parents[3]
	sys.path.insert(0, str(repository_root))
	from ingestion.land_registry.price_paid.pipeline import run
else:
	from .pipeline import run


if __name__ == "__main__":
	run()
