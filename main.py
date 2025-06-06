import nest_asyncio
nest_asyncio.apply()

import asyncio
import asyncpraw
import csv
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os
from dotenv import load_dotenv
import sys

from snowflake_ingest import ingest_csv  # import your ingest function here

load_dotenv()

analyzer = SentimentIntensityAnalyzer()

REDDIT_CLIENT_ID = "7kAL6M1Nr4QtAcNLnKmX8w"
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_SECRET")
REDDIT_USER_AGENT = "crypto-analytics"

if not REDDIT_CLIENT_SECRET:
    print("Error: REDDIT_SECRET is not set in environment variables or .env file.")
    sys.exit(1)

KEYWORDS = {"btc", "bitcoin", "crypto", "buy", "sell"}

async def fetch_comments():
    async with asyncpraw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    ) as reddit:

        subreddits = "Bitcoin+CryptoCurrency+CryptoMarkets+btc+BitcoinMarkets"
        subreddit = await reddit.subreddit(subreddits)

        today = datetime.now().date()

        comments = []

        try:
            async for comment in subreddit.comments(limit=10000):
                text = comment.body.lower()

                if any(keyword in text for keyword in KEYWORDS):
                    timestamp = datetime.fromtimestamp(comment.created_utc)
                    if timestamp.date() == today:
                        sentiment_score = analyzer.polarity_scores(text)["compound"]
                        comments.append((timestamp.isoformat(), sentiment_score, comment.body))
                        print("Fetched a comment!")

        except Exception as e:
            print(f"Error while fetching comments: {e}")

        return comments

async def scrape_and_save(csv_filename="reddit_sentiment.csv"):
    comments = await fetch_comments()
    with open(csv_filename, mode="w", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["timestamp", "sentiment", "comment"])
        writer.writerows(comments)
    print(f"Saved {len(comments)} comments to {csv_filename}")

def main():
    asyncio.run(scrape_and_save())
    ingest_csv("reddit_sentiment.csv")

if __name__ == "__main__":
    main()
