with payments as (
    select * from {{ ref('stg_order_payments') }}
),

orders as (
    select order_id, customer_id, order_purchase_ts from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['p.order_id', 'p.payment_sequential']) }} as payment_key,
        p.order_id,
        p.payment_sequential,
        c.customer_key,
        cast(to_char(o.order_purchase_ts, 'YYYYMMDD') as int)  as order_purchase_date_key,
        p.payment_type,
        p.payment_installments,
        p.payment_value
    from payments p
    left join orders o on p.order_id = o.order_id
    left join customers c on o.customer_id = c.customer_id
)

select * from final