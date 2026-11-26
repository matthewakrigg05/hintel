# hIntel dbt project

This project contains the SQL transformations for hIntel housing data. Ingestion pipelines load source data into the bronze Databricks catalog, and dbt organizes source declarations and models into Bronze, Silver, Gold Analytics, and Gold Serving layers.

## Medallion layers

Raw ingestion tables are the Bronze layer and remain owned by the ingestion
pipelines. Source declarations live in `models/bronze/`; dbt builds the first
Price Paid slice through Silver and Gold Analytics:

| Layer | dbt model or source | Relation |
| --- | --- | --- |
| Bronze | `land_registry.price_paid` source | `bronze.land_registry.price_paid` |
| Silver | `stg_price_paid` view (`models/silver/`) | `silver.<target_schema>_staging.stg_price_paid` |
| Gold Analytics | `fact__price_paid_transactions` table (`models/gold_analytics/`) | `gold.<target_schema>_analytics.fact__price_paid_transactions` |
| Gold Analytics | `fact__month_area_price_paid` table (`models/gold_analytics/`) | `gold.<target_schema>_analytics.fact__month_area_price_paid` |
| Gold Serving | Placeholder (`models/gold_serving/`) | No models yet |

`stg_price_paid` trims strings, changes blank values to null, standardizes
source codes, and casts price, transfer date, and ingestion timestamp. It
retains rows even when casts fail so the model tests can expose invalid
transaction IDs, prices, or dates. The transaction fact adds calendar fields
and a boolean new-build indicator. The monthly area aggregate excludes rows
without a valid transfer date or positive price and reports transaction count,
average, approximate median, minimum, and maximum price by location and
property type.

Bronze, Silver, and Gold are separate Databricks catalogs configured in
`dbt_project.yml`. Gold Analytics and Gold Serving use separate schemas within
the `gold` catalog; `<target_schema>` is the schema from the active `h_intel`
profile. Gold Serving is reserved for models published to PostgreSQL for
application and website queries. The active identity needs permission to read
Bronze and create models in Silver and Gold.

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
