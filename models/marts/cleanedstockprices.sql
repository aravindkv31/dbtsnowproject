{{ config(
    materialized='incremental',
    unique_key='trade_date'
) }}

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
{% if is_incremental() %}
where trade_date > (select max(trade_date) from {{ this }})
{% endif %}
order by trade_date desc

