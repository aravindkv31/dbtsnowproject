with base as (
    select
        symbol,
        cast(date as date) as trade_date,
        cast(open as float) as open_price,
        cast(high as float) as high_price,
        cast(low as float) as low_price,
        cast(close as float) as close_price,
        cast(volume as bigint) as volume,
        current_timestamp() as processed_at
    from {{ source('raw', 'STOCK_PRICES') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by symbol, trade_date
            order by processed_at desc
        ) as rn
    from base
)

select *
from deduplicated
where rn = 1

