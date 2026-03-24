-- Daily summary for each coin

WITH staging AS (
    SELECT * FROM {{ ref('stg_crypto_prices') }}
),

daily_summary AS (
    SELECT
        coin_name,
        symbol,
        date,

        -- Price stats
        ROUND(AVG(price_usd), 2)    AS avg_price,
        ROUND(MAX(price_usd), 2)    AS highest_price,
        ROUND(MIN(price_usd), 2)    AS lowest_price,
        ROUND(MAX(price_usd) - MIN(price_usd), 2) AS price_range,

        -- Market stats
        ROUND(AVG(market_cap_million), 2) AS avg_market_cap,
        ROUND(AVG(volume_million), 2)     AS avg_volume,

        -- Sentiment
        COUNT(*) FILTER (WHERE is_positive = TRUE)  AS positive_hours,
        COUNT(*) FILTER (WHERE is_positive = FALSE) AS negative_hours,
        COUNT(*) AS total_records,

        -- Positive percentage
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE is_positive = TRUE) / COUNT(*),
        2) AS positive_pct

    FROM staging
    GROUP BY coin_name, symbol, date
)

SELECT * FROM daily_summary
ORDER BY date DESC, avg_market_cap DESC