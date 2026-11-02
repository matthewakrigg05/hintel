select
    transaction_id,
    date_of_transfer,
    cast(date_trunc('month', date_of_transfer) as date) as transfer_month,
    year(date_of_transfer) as transfer_year,
    month(date_of_transfer) as transfer_month_number,
    price_gbp,
    postcode,
    property_type,
    old_new,
    case
        when old_new = 'Y' then true
        when old_new = 'N' then false
    end as is_new_build,
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
from {{ ref('stg_price_paid') }}
