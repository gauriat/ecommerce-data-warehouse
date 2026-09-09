with source as (
    select * from {{ source('raw', 'customers') }}
),

cleaned as (
    select
        customer_id,
        customer_unique_id,
        trim(customer_zip_code_prefix)      as customer_zip_code_prefix,
        initcap(trim(customer_city))        as customer_city,
        upper(trim(customer_state))         as customer_state
    from source
    where customer_id is not null
)

select * from cleaned