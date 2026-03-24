import requests
import psycopg2
from datetime import datetime

def connect_db():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="crypto_pipeline",
            user="postgres",
            password="7544"
        )
        print("✅ Database connected successfully")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def fetch_crypto_data():
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": "bitcoin,ethereum,solana,dogecoin,ripple",
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # catches bad responses
        print("✅ Data fetched from API successfully")
        return response.json()
    except requests.exceptions.Timeout:
        print("❌ API request timed out")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ API error: {e}")
        return None

def insert_data(conn, coins):
    try:
        cursor = conn.cursor()
        for coin in coins:
            cursor.execute("""
                INSERT INTO crypto_prices 
                (coin_name, symbol, current_price_usd, 
                 market_cap, price_change_24h, fetched_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                coin["name"],
                coin["symbol"],
                coin["current_price"],
                coin["market_cap"],
                coin["price_change_percentage_24h"],
                datetime.now()
            ))
        conn.commit()
        print(f"✅ {len(coins)} coins inserted into database")
    except Exception as e:
        conn.rollback()  # undo if something goes wrong
        print(f"❌ Insert failed: {e}")
    finally:
        cursor.close()

# ---------- MAIN ----------
if __name__ == "__main__":
    conn = connect_db()
    if conn:
        coins = fetch_crypto_data()
        if coins:
            insert_data(conn, coins)
        conn.close()