-- This model cleans your raw crypto data

WITH raw_data AS (
    SELECT
        id,
        coin_name,
        symbol,
        price_usd,
        price_change,
        market_cap_million,
        volume_million,
        is_positive,
        price_category,
        transformed_at

    FROM crypto_transformed
),

cleaned AS (
    SELECT
        id,

        -- Standardize text
        UPPER(coin_name)   AS coin_name,
        UPPER(symbol)      AS symbol,

        -- Clean numbers
        ROUND(price_usd::NUMERIC, 2)           AS price_usd,
        ROUND(price_change::NUMERIC, 2)        AS price_change,
        ROUND(market_cap_million::NUMERIC, 2)  AS market_cap_million,
        ROUND(volume_million::NUMERIC, 2)      AS volume_million,

        -- Booleans
        is_positive,
        price_category,

        -- Timestamps
        transformed_at,
        DATE(transformed_at)                   AS date,
        EXTRACT(HOUR FROM transformed_at)      AS hour

    FROM raw_data
)

SELECT * FROM cleaned