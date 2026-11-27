# Managing Data from Ingestion to the Website

## Architectural Decision

Use Databricks and dbt as the data and modelling platform, PostgreSQL with
PostGIS as the website serving database, and FastAPI as the application API.

The two databases should not receive raw ingestion independently. Ingestion
lands source data once, and Databricks produces curated analytical models. The
required models are then published into PostgreSQL for the website.

External sources
      |
      v
Local or object raw storage
      |
      v
Databricks Bronze
      |
      v
dbt Silver cleaning
      |
      v
dbt Gold models
      |
      v
PostgreSQL + PostGIS serving database
      |
      v
FastAPI
      |
      v
Website

This matches the repository's existing medallion structure and separates
analytical processing from low-latency application queries.

## Responsibility by Layer

### External sources

External sources provide downloadable and API-accessible datasets. Ingestion
is responsible for acquisition, source manifests, accepted-file validation,
checksums, and local raw-file retention.

### Databricks Bronze

Bronze owns landed source records. It should retain:

- source columns
- source dataset identifiers
- source file references
- source SHA-256 checksums
- publication periods
- ingestion timestamps
- ingestion batch identifiers where useful

Bronze is the evidence and replay layer. Raw source columns should remain
strings when they have not yet been typed or cleaned.

### dbt Silver

Silver contains cleaned, typed, standardized, and tested rows.

It may:

- normalize whitespace and nulls
- cast values to analytical types
- standardize codes and dates
- retain invalid records for data-quality visibility
- establish stable keys
- document the cleaned contract

The Price Paid staging model is already an example of this boundary.

### dbt Gold

Gold contains models intended for specific analytical or application purposes.
Good examples in this repository are:

- [Price Paid transaction fact](../dbt/h_intel/models/gold_analytics/land_registry/fact__price_paid_transactions.sql)
- [Monthly area Price Paid aggregate](../dbt/h_intel/models/gold_analytics/land_registry/fact__month_area_price_paid.sql)

Gold models should be:

- repeatable
- versioned
- documented
- tested
- suitable for publication
- separated by analytical subject and refresh period

#### Analytical and website-serving models

Gold models have two related consumers, and it is useful to distinguish their
needs:

- **Analytical models** support reporting, exploration, and future modelling.
  They can retain detailed facts and reusable aggregates, such as transaction
  history and monthly area statistics.
- **Website-serving models** are shaped around API and website queries. They
  should expose only the required fields, have stable keys and clear contracts,
  and precompute results where that makes application queries simpler or faster.

Both kinds of models should build on shared Silver cleaning and business rules;
do not independently reimplement those rules for the website. Build serving
models in dbt on Databricks, then publish the selected serving outputs to
PostgreSQL. They are the application-facing contract, not a second source of
truth.

For example, the transaction fact and monthly area aggregate already in the
project can remain useful analytical models. Gold Serving models should provide
purpose-shaped PostgreSQL-ready data for website needs such as postcode search
results, property comparisons, or map summaries. The dbt project keeps these
groups in separate `gold_analytics` and `gold_serving` model directories; keep
the distinction based on their consumers and query needs.

### PostgreSQL and PostGIS

PostgreSQL is the application serving store. It should contain curated copies
of Gold data plus operational application data.

Likely website-facing tables include:

- property transaction facts
- EPC certificate facts
- EPC recommendation summaries
- monthly area price trends
- postcode-to-area mappings
- administrative boundaries
- geospatial boundary tables
- comparable-property aggregates
- saved properties
- saved searches
- user alerts
- refresh metadata

PostGIS is appropriate for coordinates, administrative boundaries, spatial
queries, postcode proximity searches, and map overlays.

PostgreSQL should not independently own source ingestion. Published housing
facts are derived copies of Databricks Gold models.

### FastAPI

The API should read from PostgreSQL and expose application-specific query
operations.

The API should not contain source-cleaning logic or direct Databricks ingestion
logic. It may:

- validate request parameters
- enforce authorization
- implement domain-level query rules
- serve pagination and map payloads
- trigger controlled refresh or publication workflows if required

### Website

The website should use the API as its only application data interface. It can
query aggregates, transactions, EPC summaries, and PostGIS-backed areas without
connecting to Databricks or external source datasets.

## Publication Flow

1. Ingestion lands and validates the source.
2. Bronze stores the source data.
3. dbt builds Silver cleaning models.
4. dbt builds or refreshes analytical and website-serving Gold models.
5. A publication job selects the required website-serving Gold outputs.
6. PostgreSQL receives the batch using upserts or partition replacement.
7. Publication status and lineage are recorded.
8. The API begins serving the new version only after validation succeeds.

Publishing complete rows or partition snapshots is preferable to updating many
individual records during ingestion.

## Stable Keys

Use stable keys consistently across models and published copies:

- Price Paid: `transaction_id`
- EPC certificates: source certificate identifier
- Postcode: normalized postcode
- Area aggregates: period plus geography plus property type
- Website entities: application UUID or ULID

Published tables must distinguish source IDs from application IDs where both
are needed.

## Provenance

Postgres copies should retain enough lineage to identify their analytical
origin:

```text
source_dataset
source_sha256
publication_period
databricks_gold_model
databricks_gold_version
published_at
```

This helps explain a published value and supports later removal or replacement
when a source dataset is corrected.

## Publication Strategies

### Full snapshot

Suitable for small or moderate datasets and simple MVP operation.

Build a complete curated table in Databricks, export it, load it into a staging
table in PostgreSQL, validate it, then swap or rename it into service.

### Partition replacement

Suitable for transaction or monthly data.

Replace the affected period or geography partition while leaving unrelated
partitions unchanged.

### Upsert

Suitable for dimension tables or records that can change in place. Upserts
require stable keys and an explicit handling policy for deleted source records.

## Consistency Checks

Before publishing a model, verify:

- expected row count
- Gold primary-key uniqueness
- required columns and null rules
- source and publication period
- summary counts and totals
- geospatial geometry validity where applicable
- target table freshness

PostgreSQL publication should be atomic at the table or partition level where
possible. Avoid leaving a half-published serving table.

## Operational Ownership

Databricks owns analytical truth:

- raw and replayable source data
- cleaned analytical models
- Gold facts and aggregates
- forecasting features
- model experiments

PostgreSQL owns website-serving truth:

- application entities
- approved published copies
- users and authorization
- saved properties and searches
- alerts
- API-facing caches and derived records

PostgreSQL is a projection of Databricks for application use, not a second
independent ingestion store.

## Recommended MVP

For the first implementation:

1. Finish Bronze and dbt Silver/Gold models for Price Paid.
2. Add PostgreSQL and PostGIS.
3. Publish only transaction, trend, area, and lookup tables required by the MVP.
4. Build FastAPI endpoints against PostgreSQL.
5. Keep the website disconnected from Databricks.
6. Extend the publication pattern to EPC and later datasets.

Do not publish every Bronze or EPC source column. Only publish data required
by website features or explicitly useful for future application models.

## Implementation Boundary

The repository areas should map to these responsibilities:

```text
ingestion/                 Acquisition and Bronze delivery
dbt/h_intel/               Silver cleaning and Gold modelling
databricks/                Jobs and analytical delivery configuration
database/core/             PostgreSQL schema and migrations
services/api/              FastAPI application and query endpoints
apps/web/                  Website
docs/architecture/         Architecture decisions and diagrams
docs/                     Operational and design documentation
```
