{{ config(materialized='table') }}

select
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    processed_at
from {{ ref('mystagetable') }}
order by trade_date desc
