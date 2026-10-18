-- Staging-Layer: 1:1 zur Quelle, aber bereinigt.
-- - Normalisiert den Schema-Drift-Fall (order_status vs order_status_v2) auf eine Spalte
-- - Explizite Deduplizierung als eigener Schritt (Checklisten-Punkt), nicht implizit
--   irgendwo in einer Business-Logik-Query versteckt.

with source as (
    select * from {{ source('raw', 'orders') }}
),

normalized as (
    select
        order_id,
        customer_state,
        product_category,
        coalesce(order_status_v2, order_status) as order_status,
        order_purchase_timestamp,
        order_delivered_timestamp,
        order_estimated_delivery_days,
        review_score,
        updated_at,
        _loaded_at,
        _load_run_id,
        row_number() over (
            partition by order_id
            order by _loaded_at desc
        ) as _row_num
    from source
),

deduplicated as (
    -- explizite Dedup-Stufe: bei mehrfach geladenen order_id (z.B. durch
    -- ueberlappende inkrementelle Extraktionsfenster) gewinnt der neueste Load.
    select * exclude (_row_num)
    from normalized
    where _row_num = 1
)

select * from deduplicated
