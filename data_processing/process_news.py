# EquiLake/data_processing/process_news.py

import os
import json
import pandas as pd
from datetime import datetime
from textblob import TextBlob
import boto3
import logging
import io
from dotenv import load_dotenv
import streamlit as st

# === Load environment variables or secrets ===
if "S3_BUCKET" in st.secrets:
    S3_BUCKET = st.secrets["S3_BUCKET"]
    AWS_REGION = st.secrets["AWS_REGION"]
else:
    load_dotenv()
    S3_BUCKET = os.getenv("S3_BUCKET")
    AWS_REGION = os.getenv("AWS_REGION")

# === Config ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
TODAY = datetime.today().strftime("%Y-%m-%d")
STOCK_SYMBOLS = ["AAPL", "TSLA", "GOOGL", "AMZN", "MSFT"]

# === Utilities ===
def safe_strip(value):
    return value.strip() if isinstance(value, str) else ""

def analyze_sentiment(text):
    try:
        if not text or not isinstance(text, str) or text.strip() == "":
            return 0.0
        return TextBlob(text).sentiment.polarity
    except Exception as e:
        logging.warning(f"⚠️ Sentiment error: {e}")
        return 0.0

def get_sentiment_label(score):
    if score > 0.1:
        return "positive"
    elif score < -0.1:
        return "negative"
    return "neutral"

def download_json_from_s3(key):
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception as e:
        logging.error(f"❌ Failed to download {key} from S3: {e}")
        return []

def upload_df_to_s3(df, symbol):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    try:
        # Save as parquet
        parquet_key = f"processed/news/{symbol}_{TODAY}.parquet"
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3.upload_fileobj(buffer, S3_BUCKET, parquet_key)
        logging.info(f"✅ Uploaded to s3://{S3_BUCKET}/{parquet_key}")
    except Exception as e:
        logging.error(f"❌ Upload failed for {symbol}: {e}")

# === Main ===
def process_news():
    for symbol in STOCK_SYMBOLS:
        logging.info(f"🔍 Processing news for {symbol}...")
        raw_key = f"raw/news/{symbol}_{TODAY}.json"
        articles = download_json_from_s3(raw_key)

        if not articles:
            logging.warning(f"⚠ No articles found for {symbol}")
            continue

        rows = []
        for article in articles:
            title = safe_strip(article.get("title"))
            content = safe_strip(article.get("content")) or title
            if not content:
                continue

            score = analyze_sentiment(content)
            label = get_sentiment_label(score)

            rows.append({
                "symbol": symbol,
                "published_at": article.get("publishedAt", ""),
                "title": title,
                "source": safe_strip(article.get("source", "Unknown")),
                "content": content,
                "url": safe_strip(article.get("url")),
                "urlToImage": safe_strip(article.get("urlToImage")),
                "sentiment_score": score,
                "sentiment_label": label
            })

        if not rows:
            logging.warning(f"⚠ No valid records for {symbol}")
            continue

        df = pd.DataFrame(rows)
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
        df.dropna(subset=["published_at", "title"], inplace=True)
        upload_df_to_s3(df, symbol)

# === Entry Point ===
if __name__ == "__main__":
    process_news()
