with source as (
    select *
    from {{ source('land_registry', 'price_paid') }}
)

select
    nullif(trim(transaction_id), '') as transaction_id,
    try_cast(nullif(trim(price), '') as decimal(18, 2)) as price_gbp,
    try_cast(nullif(trim(date_of_transfer), '') as date) as date_of_transfer,
    nullif(upper(trim(postcode)), '') as postcode,
    nullif(upper(trim(property_type)), '') as property_type,
    nullif(upper(trim(old_new)), '') as old_new,
    nullif(upper(trim(duration)), '') as duration,
    nullif(trim(paon), '') as paon,
    nullif(trim(saon), '') as saon,
    nullif(trim(street), '') as street,
    nullif(trim(locality), '') as locality,
    nullif(trim(town_city), '') as town_city,
    nullif(trim(district), '') as district,
    nullif(trim(county), '') as county,
    nullif(upper(trim(ppd_category_type)), '') as ppd_category_type,
    nullif(upper(trim(record_status)), '') as record_status,
    try_cast(nullif(trim(_ingested_at), '') as timestamp) as ingested_at,
    nullif(trim(_source_dataset), '') as source_dataset,
    nullif(trim(_source_file), '') as source_file,
    nullif(trim(_source_sha256), '') as source_sha256,
    nullif(trim(_publication_period), '') as publication_period
from source
