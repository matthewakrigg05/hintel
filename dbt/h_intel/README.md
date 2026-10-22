# hIntel dbt project

This project contains the SQL transformations for hIntel housing data. Ingestion pipelines load source data into the bronze Databricks catalog, and dbt builds the staging, analytics, and feature models consumed by the hIntel application.

## Getting started

The project uses the `h_intel` profile from `~/.dbt/profiles.yml`.

```powershell
dbt debug
dbt deps
dbt build
```

The bronze catalog is split by source:

```text
bronze.land_registry
bronze.ons
bronze.epc
```

Ingestion pipelines own these landing tables. Add dbt source declarations under `models/` to reference them, and use the matching seed namespace for small local fixtures when needed:

```text
seeds/land_registry/ -> bronze.land_registry
seeds/ons/          -> bronze.ons
```
