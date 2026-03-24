import schedule
import time
import requests
import psycopg2
from datetime import datetime

#DATABASE CONNECTION

def connect_db():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="crypto_pipeline",
            user="postgres",
            password="7544"  # ← change this
        )
        return conn
    except Exception as e:
        print(f"❌ DB Connection failed: {e}")
        return None



#           FETCH RAW DATA

def fetch_crypto_data():
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": "bitcoin,ethereum,solana,dogecoin,ripple",
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ API fetch failed: {e}")
        return None



#TRANSFORM DATA

def transform_data(raw_coins):
    transformed = []
    for coin in raw_coins:
        price_change = coin.get("price_change_percentage_24h") or 0
        market_cap   = coin.get("market_cap") or 0
        volume       = coin.get("total_volume") or 0

        transformed.append({
            "coin_name":          coin["name"].upper(),
            "symbol":             coin["symbol"].upper(),
            "price_usd":          round(coin["current_price"], 2),
            "price_change":       round(price_change, 2),
            "market_cap_million": round(market_cap / 1_000_000, 2),
            "volume_million":     round(volume / 1_000_000, 2),
            "is_positive":        price_change > 0,
            "price_category":     categorize_price(coin["current_price"]),
            "transformed_at":     datetime.now()
        })
    return transformed

def categorize_price(price):
    if price > 10000:   return "HIGH VALUE"
    elif price > 100:   return "MID VALUE"
    else:               return "LOW VALUE"



#SAVE TO DATABASE

def save_data(clean_data):
    conn = connect_db()
    if not conn:
        return

    try:
        cursor = conn.cursor()
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
        print(f" {len(clean_data)} coins saved at {datetime.now()}")
    except Exception as e:
        conn.rollback()
        print(f"❌ Save failed: {e}")
    finally:
        cursor.close()
        conn.close()



#FULL PIPELINE (1 function)

def run_pipeline():
    print(f"\n Pipeline started at {datetime.now()}")
    print("-" * 40)

    # Step 1 - Fetch
    raw_data = fetch_crypto_data()
    if not raw_data:
        print("Skipping this run — no data fetched")
        return

    # Step 2 - Transform
    clean_data = transform_data(raw_data)
    print(f"Transformed {len(clean_data)} coins")

    # Step 3 - Save
    save_data(clean_data)

    print("-" * 40)
    print("Pipeline completed successfully!\n")



#SCHEDULE SETUP


# Run immediately once when script starts
run_pipeline()

# Then schedule it automatically
schedule.every(1).hours.do(run_pipeline)       # every 1 hour
# schedule.every(30).minutes.do(run_pipeline)  # every 30 min
# schedule.every().day.at("09:00").do(run_pipeline)  # every day 9am
# schedule.every().monday.at("08:00").do(run_pipeline) # every monday

print("Scheduler is running... Press CTRL+C to stop")
print("Pipeline will run every 1 hour automatically")
print("-" * 40)

# Keep the script alive forever
while True:
    schedule.run_pending()
    time.sleep(60)  # check every 60 seconds
