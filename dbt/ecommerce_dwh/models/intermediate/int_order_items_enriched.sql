with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

enriched as (
    select
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_ts,
        oi.price,
        oi.freight_value,
        oi.price + oi.freight_value as item_total_value,
        oi.shipping_limit_ts
    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
)

select * from enriched