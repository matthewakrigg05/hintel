WITH source AS (
    SELECT *
    FROM {{ source('land_registry', 'price_paid') }}
)

SELECT
    NULLIF(TRIM(transaction_id), '') AS transaction_id,
    TRY_CAST(NULLIF(TRIM(price), '') AS DECIMAL(18, 2)) AS price_gbp,
    TRY_CAST(NULLIF(TRIM(date_of_transfer), '') AS DATE) AS date_of_transfer,
    NULLIF(UPPER(TRIM(postcode)), '') AS postcode,
    NULLIF(UPPER(TRIM(property_type)), '') AS property_type,
    NULLIF(UPPER(TRIM(old_new)), '') AS old_new,
    NULLIF(UPPER(TRIM(duration)), '') AS duration,
    NULLIF(TRIM(paon), '') AS paon,
    NULLIF(TRIM(saon), '') AS saon,
    NULLIF(TRIM(street), '') AS street,
    NULLIF(TRIM(locality), '') AS locality,
    NULLIF(TRIM(town_city), '') AS town_city,
    NULLIF(TRIM(district), '') AS district,
    NULLIF(TRIM(county), '') AS county,
    NULLIF(UPPER(TRIM(ppd_category_type)), '') AS ppd_category_type,
    NULLIF(UPPER(TRIM(record_status)), '') AS record_status,
    TRY_CAST(NULLIF(TRIM(_ingested_at), '') AS TIMESTAMP) AS ingested_at,
    NULLIF(TRIM(_source_dataset), '') AS source_dataset,
    NULLIF(TRIM(_source_file), '') AS source_file,
    NULLIF(TRIM(_source_sha256), '') AS source_sha256,
    NULLIF(TRIM(_publication_period), '') AS publication_period
FROM source
