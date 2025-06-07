import snowflake.connector
import csv
import os
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)

def ingest_reddit_sentiment(csv_file="reddit_sentiment.csv"):
    cursor = conn.cursor()
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        rows = [tuple(row) for row in reader]

    insert_query = """
        INSERT INTO reddit_comments (timestamp, sentiment, comment)
        VALUES (%s, %s, %s)
    """
    cursor.executemany(insert_query, rows)
    conn.commit()
    print(f"Ingested {cursor.rowcount} rows into reddit_comments")
    cursor.close()

def ingest_btc_prices(csv_file="btc_prices.csv"):
    cursor = conn.cursor()
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        rows = []
        for row in reader:
            # Assuming CSV columns: timestamp, open, high, low, close, volume
            timestamp = row[0]
            open_price = float(row[1])
            high_price = float(row[2])
            low_price = float(row[3])
            close_price = float(row[4])
            volume = float(row[5])
            rows.append((timestamp, open_price, high_price, low_price, close_price, volume))

    insert_query = """
        INSERT INTO btc_prices (timestamp, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, rows)
    conn.commit()
    print(f"Ingested {cursor.rowcount} rows into btc_prices")
    cursor.close()

if __name__ == "__main__":
    ingest_reddit_sentiment()
    ingest_btc_prices()
    conn.close()
