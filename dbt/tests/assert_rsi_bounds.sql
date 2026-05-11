-- RSI-14 must be between 0 and 100 where not null
select *
from {{ ref('gold_features') }}
where rsi_14 is not null
  and (rsi_14 < 0 or rsi_14 > 100)
