with source as (
    select * from {{ source('raw', 'order_payments') }}
),

cleaned as (
    select
        order_id,
        (payment_sequential)::int  as payment_sequential,
        lower(trim(payment_type))  as payment_type,
        payment_installments,
        payment_value::numeric(12, 2) as payment_value
    from source
    where order_id is not null
      and payment_value >= 0
)

select * from cleaned