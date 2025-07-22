import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import boto3
import io
import streamlit as st

if "S3_BUCKET" in st.secrets:
    S3_BUCKET = st.secrets["S3_BUCKET"]
    AWS_REGION = st.secrets["AWS_REGION"]
    NEWS_API_KEY = st.secrets["NEWS_API_KEY"]
    ALPHA_VANTAGE_API_KEY = st.secrets["ALPHA_VANTAGE_API_KEY"]
else:
    load_dotenv()
    S3_BUCKET = os.getenv("S3_BUCKET")
    AWS_REGION = os.getenv("AWS_REGION")
    NEWS_API_KEY = os.getenv("NEWS_API_KEY")
    ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")



# === Config ===
QUERY = "stocks"
S3_KEY = f"raw/news/{QUERY}_{datetime.today().strftime('%Y-%m-%d')}.json"

def fetch_news(query):
    url = f"https://newsapi.org/v2/everything?q={query}&sortBy=publishedAt&language=en&pageSize=50&apiKey={NEWS_API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Failed to fetch news. Status code: {response.status_code}")
        return {}

def upload_json_to_s3(data, s3_key):
    if not S3_BUCKET:
        raise ValueError("❌ S3_BUCKET is not set in the .env file.")

    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        json_buffer = io.BytesIO(json.dumps(data, indent=2).encode("utf-8"))
        s3.upload_fileobj(json_buffer, S3_BUCKET, s3_key)
        print(f"✅ Uploaded to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌ Failed to upload to S3: {e}")

if __name__ == "__main__":
    print("📰 Fetching news data...")
    data = fetch_news(QUERY)
    if data:
        upload_json_to_s3(data, S3_KEY)
