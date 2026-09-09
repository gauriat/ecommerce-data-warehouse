with reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

orders as (
    select order_id, customer_id, order_purchase_ts from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['r.review_id']) }}  as review_key,
        r.review_id,
        r.order_id,
        c.customer_key,
        cast(to_char(o.order_purchase_ts, 'YYYYMMDD') as int)  as order_purchase_date_key,
        r.review_score,
        (r.review_comment_message is not null)                  as has_comment,
        r.review_creation_ts,
        r.review_answer_ts,
        extract(epoch from (r.review_answer_ts - r.review_creation_ts)) / 3600.0  as response_time_hours
    from reviews r
    left join orders o on r.order_id = o.order_id
    left join customers c on o.customer_id = c.customer_id
)

select * from final