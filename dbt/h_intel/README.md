# hIntel dbt project

This project contains the SQL transformations for hIntel housing data. Ingestion pipelines load source data into the bronze Databricks catalog, and dbt builds the staging, analytics, and feature models consumed by the hIntel application.

## Medallion layers

Raw ingestion tables are the Bronze layer and remain owned by the ingestion
pipelines. dbt builds the first Price Paid slice through Silver and Gold:

| Layer | dbt model or source | Relation |
| --- | --- | --- |
| Bronze | `land_registry.price_paid` source | `bronze.land_registry.price_paid` |
| Silver | `stg_price_paid` view | `silver.<target_schema>_staging.stg_price_paid` |
| Gold | `fact__price_paid_transactions` table | `gold.<target_schema>_marts.fact__price_paid_transactions` |
| Gold | `fact__month_area_price_paid` table | `gold.<target_schema>_marts.fact__month_area_price_paid` |

`stg_price_paid` trims strings, changes blank values to null, standardizes
source codes, and casts price, transfer date, and ingestion timestamp. It
retains rows even when casts fail so the model tests can expose invalid
transaction IDs, prices, or dates. The transaction fact adds calendar fields
and a boolean new-build indicator. The monthly area aggregate excludes rows
without a valid transfer date or positive price and reports transaction count,
average, approximate median, minimum, and maximum price by location and
property type.

The Silver and Gold names above are separate Databricks catalogs configured in
`dbt_project.yml`; `<target_schema>` is the schema from the active `h_intel`
profile. The active identity needs permission to read Bronze and create models
in both catalogs.

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
