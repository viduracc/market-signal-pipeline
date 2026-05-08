{{config(materialized="table")}}

with source as (
    select *
    from {{source('bronze','bronze_raw')}}
),

deduped as (
    select
        ticker,
        bar_date,
        open::numeric(18,6) as open,
        high::numeric(18,6) as high,
        low::numeric(18,6) as low,
        close::numeric(18,6) as close,
        volume::bigint as volume,
        source,
        loaded_at,
        row_number() over (
            partition by ticker, bar_date
            order by loaded_at desc
        ) as row_num
    from source
)

select
    ticker,
    bar_date,
    open,
    high,
    low,
    close,
    volume,
    source,
    loaded_at
from deduped
where row_num = 1
