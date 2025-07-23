import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import boto3
import io
import streamlit as st

# === Load Secrets or .env ===
if "S3_BUCKET" in st.secrets:
    S3_BUCKET = st.secrets["S3_BUCKET"]
    AWS_REGION = st.secrets["AWS_REGION"]
    NEWS_API_KEY = st.secrets["NEWS_API_KEY"]
else:
    load_dotenv()
    S3_BUCKET = os.getenv("S3_BUCKET")
    AWS_REGION = os.getenv("AWS_REGION")
    NEWS_API_KEY = os.getenv("NEWS_API_KEY")

# === Config ===
STOCK_SYMBOLS = ["AAPL", "TSLA", "GOOGL", "AMZN", "MSFT"]
TODAY = datetime.today().strftime("%Y-%m-%d")

# === S3 Upload Helper ===
def upload_json_to_s3(data, s3_key):
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        json_buffer = io.BytesIO(json.dumps(data, indent=2).encode("utf-8"))
        s3.upload_fileobj(json_buffer, S3_BUCKET, s3_key)
        print(f"✅ Uploaded to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌ Failed to upload to S3: {e}")

# === News Fetching ===
def fetch_news_for_symbol(symbol):
    url = f"https://newsapi.org/v2/everything?q={symbol}&sortBy=publishedAt&language=en&pageSize=25&apiKey={NEWS_API_KEY}"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"❌ Failed to fetch news for {symbol}. Status: {response.status_code}")
        return []

    data = response.json()
    articles = data.get("articles", [])
    for article in articles:
        article["symbol"] = symbol
    return articles

# ✅ Importable function for Streamlit and CLI
def fetch_news():
    for symbol in STOCK_SYMBOLS:
        print(f"🔍 {symbol}")
        articles = fetch_news_for_symbol(symbol)

        if articles:
            s3_key = f"raw/news/{symbol}_{TODAY}.json"
            upload_json_to_s3(articles, s3_key)
        else:
            print(f"⚠️ No articles found for {symbol}")

# === CLI Entry Point ===
if __name__ == "__main__":
    print("📰 Fetching news per stock...")
    fetch_news()
