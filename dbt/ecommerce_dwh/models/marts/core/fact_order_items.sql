with items as (
    select * from {{ ref('int_order_items_enriched') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

sellers as (
    select * from {{ ref('dim_sellers') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['i.order_id', 'i.order_item_id']) }} as order_item_key,
        i.order_id,
        i.order_item_id,
        c.customer_key,
        p.product_key,
        s.seller_key,
        cast(to_char(i.order_purchase_ts, 'YYYYMMDD') as int)  as order_purchase_date_key,
        i.order_status,
        i.price,
        i.freight_value,
        i.item_total_value
    from items i
    left join customers c on i.customer_id = c.customer_id
    left join products p on i.product_id = p.product_id
    left join sellers s on i.seller_id = s.seller_id
)

select * from final