"""Reserved for future dbt Gold publication to SQLite.

Housing and ingested data are intentionally not part of the current core schema.
Implement this publisher only after a Gold serving contract has been modelled.
"""

from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Placeholder for future dbt Gold publication to SQLite"
    )
    parser.parse_args()
    raise RuntimeError(
        "Gold publication is disabled until the housing serving schema is modelled"
    )


if __name__ == "__main__":
    main()
