Core database
=============

This directory contains the local SQLite application and demonstration database.
It does not contain ingested source data or modelled housing datasets yet.

The API, regardless of language, can use this SQLite database through a normal
SQLite driver. Python scripts may initialize and seed it locally, but SQLite is
not coupled to Python application code.

Current scope
-------------

The initial schema contains only application data:

- application users
- saved properties
- saved searches
- user alerts

Ingestion and curated housing facts remain in Databricks until dbt Gold models
and an explicit publication contract have been designed.

Files
-----

- migrations/ - ordered SQL schema migrations
- seeds/ - optional controlled demo/reference seed SQL
- tests/ - schema and integration tests
- hintel.sqlite3 - local database file (generated, not source-controlled)
- init_sqlite.py - create or migrate the database
- publish_sqlite.py - reserved placeholder for future dbt Gold publication

Initialize
----------

From the repository root:

    python database/core/init_sqlite.py --seed

To select a separate database file:

    python database/core/init_sqlite.py --database D:\hIntel_demo\hintel.sqlite3 --seed

Future housing publication
--------------------------

When dbt Gold contracts are established, add a separate publication component
that exports the selected models and upserts the curated website-serving tables.
Do not add raw bronze or silver datasets to this database. The publication
schema should be introduced through another numbered migration and tested
before enabling it.

Migration policy
----------------

Add numbered SQL files to migrations/ for future schema changes. PostgreSQL or
MySQL migrations should later use equivalent relational concepts rather than
copying SQLite-specific SQL verbatim.

Do not commit hintel.sqlite3. Keep passwords and API secrets outside source
control. Store password hashes or authentication details behind the API's own
security implementation rather than in demo seed data.
