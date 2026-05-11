{{ config(materialized="table") }}

with daily as (
    select
        ticker,
        bar_date,
        close,
        volume,
        lag(close, 1)  over (partition by ticker order by bar_date) as prev_close_1d,
        lag(close, 5)  over (partition by ticker order by bar_date) as prev_close_5d,
        lag(close, 20) over (partition by ticker order by bar_date) as prev_close_20d
    from {{ ref('silver_daily_bars') }}
),

returns as (
    select
        *,
        (close - prev_close_1d)  / nullif(prev_close_1d, 0)  as return_1d,
        (close - prev_close_5d)  / nullif(prev_close_5d, 0)  as return_5d,
        (close - prev_close_20d) / nullif(prev_close_20d, 0) as return_20d
    from daily
),

mas as (
    select
        ticker,
        bar_date,
        close,
        volume,
        return_1d,
        return_5d,
        return_20d,
        avg(close) over (
            partition by ticker order by bar_date
            rows between 4 preceding and current row
        ) as ma_5d,
        avg(close) over (
            partition by ticker order by bar_date
            rows between 19 preceding and current row
        ) as ma_20d,
        avg(close) over (
            partition by ticker order by bar_date
            rows between 49 preceding and current row
        ) as ma_50d,
        stddev(return_1d) over (
            partition by ticker order by bar_date
            rows between 19 preceding and current row
        ) as volatility_20d,
        avg(volume) over (
            partition by ticker order by bar_date
            rows between 19 preceding and current row
        ) as avg_volume_20d
    from returns
),

rsi_prep as (
    select
        ticker,
        bar_date,
        close,
        volume,
        return_1d,
        return_5d,
        return_20d,
        ma_5d,
        ma_20d,
        ma_50d,
        volatility_20d,
        volume / nullif(avg_volume_20d, 0)              as volume_ratio_20d,
        case when return_1d > 0 then return_1d else 0 end as gain,
        case when return_1d < 0 then abs(return_1d) else 0 end as loss
    from mas
),

rsi as (
    select
        *,
        avg(gain) over (
            partition by ticker order by bar_date
            rows between 13 preceding and current row
        ) as avg_gain_14d,
        avg(loss) over (
            partition by ticker order by bar_date
            rows between 13 preceding and current row
        ) as avg_loss_14d
    from rsi_prep
)

select
    ticker,
    bar_date,
    close,
    volume,
    return_1d,
    return_5d,
    return_20d,
    ma_5d,
    ma_20d,
    ma_50d,
    volatility_20d,
    volume_ratio_20d,
    case
        when avg_loss_14d = 0 then 100
        else 100 - (100 / (1 + (avg_gain_14d / nullif(avg_loss_14d, 0))))
    end as rsi_14
from rsi
