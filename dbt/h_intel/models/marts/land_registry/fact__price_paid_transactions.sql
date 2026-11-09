SELECT
    transaction_id,
    date_of_transfer,
    CAST(DATE_TRUNC('month', date_of_transfer) AS DATE) AS transfer_month,
    YEAR(date_of_transfer) AS transfer_year,
    MONTH(date_of_transfer) AS transfer_month_number,
    price_gbp,
    postcode,
    property_type,
    old_new,
    CASE
        WHEN old_new = 'Y' THEN TRUE
        WHEN old_new = 'N' THEN FALSE
    END AS is_new_build,
    duration,
    paon,
    saon,
    street,
    locality,
    town_city,
    district,
    county,
    ppd_category_type,
    record_status,
    ingested_at,
    source_dataset,
    source_file,
    source_sha256,
    publication_period
FROM {{ ref('stg_price_paid') }}
