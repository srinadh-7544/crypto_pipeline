import requests
import psycopg2
import pandas as pd
from datetime import datetime

# ---------- STEP 1: FETCH RAW DATA ----------
def fetch_raw_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": "bitcoin,ethereum,solana,dogecoin,ripple",
    }
    response = requests.get(url, params=params, timeout=10)
    return response.json()

# Fetch and print RAW data first
raw_data = fetch_raw_data()

# See what raw data looks like
for coin in raw_data:
    print("--- RAW COIN DATA ---")
    print(f"Name:         {coin['name']}")
    print(f"Price:        {coin['current_price']}")
    print(f"Market Cap:   {coin['market_cap']}")
    print(f"Change 24h:   {coin['price_change_percentage_24h']}")
    print(f"Volume:       {coin['total_volume']}")
    print()
# ---------- STEP 2: TRANSFORM DATA ----------
def transform_data(raw_coins):
    transformed = []

    for coin in raw_coins:

        # Handle missing/null values safely
        price_change = coin.get("price_change_percentage_24h") or 0
        market_cap = coin.get("market_cap") or 0
        volume = coin.get("total_volume") or 0

        transformed_coin = {
            # Standardize text
            "coin_name":     coin["name"].upper(),
            "symbol":        coin["symbol"].upper(),

            # Round numbers cleanly
            "price_usd":     round(coin["current_price"], 2),
            "price_change":  round(price_change, 2),

            # Convert large numbers to readable millions
            "market_cap_million": round(market_cap / 1_000_000, 2),
            "volume_million":     round(volume / 1_000_000, 2),

            # Create new useful columns
            "is_positive":   price_change > 0,   # True or False
            "price_category": categorize_price(coin["current_price"]),

            # Timestamp
            "transformed_at": datetime.now()
        }
        transformed.append(transformed_coin)

    return transformed


# New column we create from scratch
def categorize_price(price):
    if price > 10000:
        return "HIGH VALUE"
    elif price > 100:
        return "MID VALUE"
    else:
        return "LOW VALUE"


raw_data = fetch_raw_data()
clean_data = transform_data(raw_data)
# Print clean transformed data
print("=== TRANSFORMED DATA ===")
for coin in clean_data:
    print(f"""
    Coin:          {coin['coin_name']} ({coin['symbol']})
    Price:         ${coin['price_usd']}
    Change 24h:    {coin['price_change']}%
    Market Cap:    ${coin['market_cap_million']} Million
    Volume:        ${coin['volume_million']} Million
    Trending Up:   {coin['is_positive']}
    Category:      {coin['price_category']}
    """)
# ---------- STEP 3: SAVE TO DATABASE ----------
def save_transformed_data(clean_data):
    conn = psycopg2.connect(
        host="localhost",
        database="crypto_pipeline",
        user="postgres",
        password="7544"  # ← change this
    )
    cursor = conn.cursor()

    # Create new clean table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crypto_transformed (
            id                  SERIAL PRIMARY KEY,
            coin_name           VARCHAR(50),
            symbol              VARCHAR(10),
            price_usd           NUMERIC,
            price_change        NUMERIC,
            market_cap_million  NUMERIC,
            volume_million      NUMERIC,
            is_positive         BOOLEAN,
            price_category      VARCHAR(20),
            transformed_at      TIMESTAMP
        )
    """)

    # Insert clean data
    for coin in clean_data:
        cursor.execute("""
            INSERT INTO crypto_transformed VALUES (
                DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            coin["coin_name"],
            coin["symbol"],
            coin["price_usd"],
            coin["price_change"],
            coin["market_cap_million"],
            coin["volume_million"],
            coin["is_positive"],
            coin["price_category"],
            coin["transformed_at"]
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Transformed data saved to PostgreSQL!")


# ---------- RUN FULL PIPELINE ----------
raw_data  = fetch_raw_data()
clean_data = transform_data(raw_data)
save_transformed_data(clean_data)
