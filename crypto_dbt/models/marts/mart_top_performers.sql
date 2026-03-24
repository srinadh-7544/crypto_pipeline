-- Best performing coins across all time

WITH staging AS (
    SELECT * FROM {{ ref('stg_crypto_prices') }}
),

performance AS (
    SELECT
        coin_name,
        symbol,
        price_category,

        ROUND(AVG(price_usd), 2)         AS avg_price,
        ROUND(MAX(price_usd), 2)         AS peak_price,
        ROUND(MIN(price_usd), 2)         AS lowest_price,
        ROUND(AVG(price_change), 2)      AS avg_daily_change,
        ROUND(STDDEV(price_usd), 2)      AS volatility,
        ROUND(AVG(market_cap_million), 2) AS avg_market_cap,

        -- Overall sentiment score
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE is_positive = TRUE) / COUNT(*),
        2) AS bullish_score,

        COUNT(*) AS total_data_points

    FROM staging
    GROUP BY coin_name, symbol, price_category
)

SELECT
    *,
    RANK() OVER (ORDER BY avg_daily_change DESC) AS growth_rank,
    RANK() OVER (ORDER BY bullish_score DESC)    AS sentiment_rank,
    RANK() OVER (ORDER BY avg_market_cap DESC)   AS market_cap_rank
FROM performance
ORDER BY growth_rank