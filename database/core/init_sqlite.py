"""Initialize or migrate the local hIntel SQLite demonstration database."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


CORE_DIR = Path(__file__).resolve().parent
MIGRATIONS_DIR = CORE_DIR / "migrations"
SEEDS_DIR = CORE_DIR / "seeds"
DEFAULT_DATABASE = CORE_DIR / "hintel.sqlite3"


def apply_sql_file(connection: sqlite3.Connection, sql_path: Path) -> None:
    """Apply one SQL file atomically."""
    sql = sql_path.read_text(encoding="utf-8")
    connection.executescript(sql)


def initialize_database(database_path: Path, seed: bool = False) -> list[str]:
    """Apply unapplied numbered SQL migrations and optionally demo seeds."""
    database_path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(database_path)
    connection.execute("PRAGMA foreign_keys = ON")
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migration (
                name TEXT PRIMARY KEY,
                applied_at TEXT NOT NULL
                    DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            )
            """
        )

        applied = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM schema_migration"
            ).fetchall()
        }
        migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
        newly_applied: list[str] = []

        for migration_path in migration_files:
            if migration_path.name in applied:
                continue
            with connection:
                apply_sql_file(connection, migration_path)
                connection.execute(
                    "INSERT INTO schema_migration (name) VALUES (?)",
                    (migration_path.name,),
                )
            newly_applied.append(migration_path.name)

        if seed:
            seed_files = sorted(SEEDS_DIR.glob("*.sql"))
            with connection:
                for seed_path in seed_files:
                    apply_sql_file(connection, seed_path)

        return newly_applied
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Initialize or migrate the hIntel SQLite demonstration database"
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DATABASE,
        help="SQLite database file (default: database/core/hintel.sqlite3)",
    )
    parser.add_argument(
        "--seed",
        action="store_true",
        help="Apply controlled SQL seed data",
    )
    args = parser.parse_args()

    applied = initialize_database(args.database, seed=args.seed)
    if applied:
        print(f"Applied migrations: {', '.join(applied)}")
    else:
        print("No migrations to apply")
    print(f"SQLite database: {args.database}")


if __name__ == "__main__":
    main()
