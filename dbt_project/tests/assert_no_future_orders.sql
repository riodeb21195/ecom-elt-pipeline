select order_id, order_purchase_timestamp
from {{ ref('stg_orders') }}
where order_purchase_timestamp > current_timestamp