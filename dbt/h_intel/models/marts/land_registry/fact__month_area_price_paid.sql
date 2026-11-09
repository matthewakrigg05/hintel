SELECT
    CAST(DATE_TRUNC('month', date_of_transfer) AS DATE) AS transfer_month,
    county,
    district,
    town_city,
    property_type,
    COUNT(*) AS transaction_count,
    AVG(price_gbp) AS average_price_gbp,
    PERCENTILE_APPROX(price_gbp, 0.5) AS median_price_gbp,
    MIN(price_gbp) AS minimum_price_gbp,
    MAX(price_gbp) AS maximum_price_gbp
FROM {{ ref('stg_price_paid') }}
WHERE date_of_transfer IS NOT NULL
    AND price_gbp > 0
GROUP BY
    CAST(DATE_TRUNC('month', date_of_transfer) AS DATE),
    county,
    district,
    town_city,
    property_type
