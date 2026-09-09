with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

order_item_totals as (
    select
        order_id,
        count(*)                      as item_count,
        sum(price)                    as merchandise_value,
        sum(freight_value)            as freight_value,
        sum(item_total_value)         as order_value
    from {{ ref('int_order_items_enriched') }}
    group by 1
),

payment_totals as (
    select
        order_id,
        sum(payment_value)   as payment_value,
        count(*)             as payment_count
    from {{ ref('stg_order_payments') }}
    group by 1
),

review_summary as (
    select
        order_id,
        avg(review_score)  as avg_review_score
    from {{ ref('stg_order_reviews') }}
    group by 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }}      as order_key,
        o.order_id,
        c.customer_key,
        o.order_status,
        cast(to_char(o.order_purchase_ts, 'YYYYMMDD') as int)        as order_purchase_date_key,
        o.order_purchase_ts,
        o.order_approved_ts,
        o.order_delivered_carrier_ts,
        o.order_delivered_customer_ts,
        o.order_estimated_delivery_ts,
        o.delivery_days,
        o.was_late,
        coalesce(it.item_count, 0)          as item_count,
        coalesce(it.merchandise_value, 0)   as merchandise_value,
        coalesce(it.freight_value, 0)       as freight_value,
        coalesce(it.order_value, 0)         as order_value,
        coalesce(pt.payment_value, 0)       as payment_value,
        coalesce(pt.payment_count, 0)       as payment_count,
        rs.avg_review_score
    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join order_item_totals it on o.order_id = it.order_id
    left join payment_totals pt on o.order_id = pt.order_id
    left join review_summary rs on o.order_id = rs.order_id
)

select * from final