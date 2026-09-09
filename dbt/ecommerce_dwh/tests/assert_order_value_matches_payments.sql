select
    order_id,
    order_value,
    payment_value,
    abs(order_value - payment_value) as diff
from {{ ref('fact_orders') }}
where order_status not in ('canceled', 'unavailable')
  and abs(order_value - payment_value) > 1.00