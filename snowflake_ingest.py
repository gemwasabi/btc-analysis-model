import os
import csv
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def ingest_csv(file_path):
    ctx = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="REDDIT_WH",
        database="CRYPTO_ANALYTICS",
        schema="REDDIT_DATA"
    )
    cs = ctx.cursor()

    with open(file_path, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = [(row['timestamp'], float(row['sentiment']), row['comment']) for row in reader]

    insert_sql = """
        INSERT INTO reddit_comments (timestamp, sentiment, comment)
        VALUES (%s, %s, %s)
    """
    cs.executemany(insert_sql, rows)
    ctx.commit()
    cs.close()
    ctx.close()
    print(f"Ingested {len(rows)} rows into Snowflake")
