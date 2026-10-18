with orders as(
    select * from {{ ref('stg_orders') }}
    where order_status = 'delivered'
      and order_delivered_timestamp is not null
),

with_delay as (
    select
        *,
        date_diff(
            'day',
            order_purchase_timestamp,
            order_delivered_timestamp
        ) as actual_delivery_days,

        date_diff(
            'day',
            order_purchase_timestamp,
            order_delivered_timestamp
        ) > order_estimated_delivery_days as is_late_delivery
    from orders
)

select * from with_delay