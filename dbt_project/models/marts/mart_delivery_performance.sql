-- Business-Mart: beantwortet die eigentliche Frage des Projekts --
-- "Welche Kategorien/Regionen haben die hoechste Verspaetungsrate, und wie
-- haengt das mit der Kundenzufriedenheit zusammen?"
--
-- incremental + insert_overwrite => idempotent: ein erneuter Lauf fuer denselben
-- Zeitraum ersetzt die Partition statt Duplikate anzuhaengen (Checklisten-Punkt).

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['purchase_month']
    )
}}

with orders as (
    select
        *,
        date_trunc('month', order_purchase_timestamp)::date as purchase_month
    from {{ ref('int_orders_with_delay_flags') }}

    {% if is_incremental() %}
    where date_trunc('month', order_purchase_timestamp)::date
        >= (select coalesce(max(purchase_month), '1900-01-01'::date) from {{ this }})
    {% endif %}
)

select
    purchase_month,
    customer_state,
    product_category,
    count(*)                                              as total_orders,
    sum(case when is_late_delivery then 1 else 0 end)     as late_orders,
    round(
        sum(case when is_late_delivery then 1 else 0 end)::decimal
        / nullif(count(*), 0),
        4
    )                                                       as late_delivery_rate,
    round(avg(review_score), 2)                            as avg_review_score,
    round(
        avg(case when is_late_delivery then review_score end), 2
    )                                                       as avg_review_score_late,
    round(
        avg(case when not is_late_delivery then review_score end), 2
    )                                                       as avg_review_score_on_time
from orders
group by 1, 2, 3
