with source as (
    select * from {{ source('raw', 'orders') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        lower(trim(order_status))              as order_status,
        order_purchase_timestamp::timestamp    as order_purchase_ts,
        order_approved_at::timestamp           as order_approved_ts,
        order_delivered_carrier_date::timestamp   as order_delivered_carrier_ts,
        order_delivered_customer_date::timestamp  as order_delivered_customer_ts,
        order_estimated_delivery_date::timestamp  as order_estimated_delivery_ts,
        case
            when order_delivered_customer_date is not null
                then extract(day from (order_delivered_customer_date - order_purchase_timestamp))
        end as delivery_days,
        case
            when order_delivered_customer_date is not null and order_estimated_delivery_date is not null
                then order_delivered_customer_date > order_estimated_delivery_date
        end as was_late
    from source
    where order_id is not null
      and customer_id is not null
)

select * from cleaned