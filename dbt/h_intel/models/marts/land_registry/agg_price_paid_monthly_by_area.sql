select
    cast(date_trunc('month', date_of_transfer) as date) as transfer_month,
    county,
    district,
    town_city,
    property_type,
    count(*) as transaction_count,
    avg(price_gbp) as average_price_gbp,
    percentile_approx(price_gbp, 0.5) as median_price_gbp,
    min(price_gbp) as minimum_price_gbp,
    max(price_gbp) as maximum_price_gbp
from {{ ref('stg_price_paid') }}
where date_of_transfer is not null
    and price_gbp > 0
group by
    cast(date_trunc('month', date_of_transfer) as date),
    county,
    district,
    town_city,
    property_type
