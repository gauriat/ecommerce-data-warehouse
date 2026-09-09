with source as (
    select * from {{ source('raw', 'order_items') }}
),

cleaned as (
    select
        order_id,
        (order_item_id)::int          as order_item_id,
        product_id,
        seller_id,
        shipping_limit_date::timestamp as shipping_limit_ts,
        price::numeric(12, 2)          as price,
        freight_value::numeric(12, 2)  as freight_value
    from source
    where order_id is not null
      and product_id is not null
      and price >= 0
)

select * from cleaned