with source as (
    select * from {{ source('raw', 'sellers') }}
),

cleaned as (
    select
        seller_id,
        trim(seller_zip_code_prefix)  as seller_zip_code_prefix,
        initcap(trim(seller_city))    as seller_city,
        upper(trim(seller_state))     as seller_state
    from source
    where seller_id is not null
)

select * from cleaned