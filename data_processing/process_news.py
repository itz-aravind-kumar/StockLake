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




# === Setup ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# === S3 Keys ===
TODAY = datetime.today().strftime("%Y-%m-%d")
RAW_S3_KEY = f"raw/news/stocks_{TODAY}.json"  # Adjust if your file is named differently
CSV_S3_KEY = f"processed/news/processed_news_{TODAY}.csv"
PARQUET_S3_KEY = f"processed/news/processed_news_{TODAY}.parquet"

# === Utilities ===
def safe_strip(value):
    return value.strip() if isinstance(value, str) else ""

def analyze_sentiment(text):
    try:
        if not text or not isinstance(text, str) or text.strip() == "":
            return 0.0
        blob = TextBlob(text)
        return blob.sentiment.polarity
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
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        logging.error(f"❌ Failed to download {key} from S3: {e}")
        return {}

def upload_df_to_s3(df):
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)

        # --- Upload CSV ---
        csv_buffer = io.BytesIO()
        csv_data = df.to_csv(index=False).encode("utf-8")
        csv_buffer.write(csv_data)
        csv_buffer.seek(0)
        s3.upload_fileobj(csv_buffer, S3_BUCKET, CSV_S3_KEY)
        logging.info(f"✅ Uploaded CSV to s3://{S3_BUCKET}/{CSV_S3_KEY}")

        # --- Upload Parquet ---
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        s3.upload_fileobj(parquet_buffer, S3_BUCKET, PARQUET_S3_KEY)
        logging.info(f"✅ Uploaded Parquet to s3://{S3_BUCKET}/{PARQUET_S3_KEY}")

    except Exception as e:
        logging.error(f"❌ S3 upload failed: {e}")

# === Main Processing ===
def process_news():
    logging.info("🚀 Downloading and processing news from S3...")
    data = download_json_from_s3(RAW_S3_KEY)
    if not data:
        return

    articles = data.get("articles", [])
    if not articles:
        logging.warning("⚠ No articles found in the raw data.")
        return

    records = []
    for article in articles:
        title = safe_strip(article.get("title"))
        content = safe_strip(article.get("content")) or title
        if not content:
            continue

        score = analyze_sentiment(content)
        label = get_sentiment_label(score)

        records.append({
            "published_at": article.get("publishedAt", ""),
            "title": title,
            "source": safe_strip(article.get("source", {}).get("name")),
            "content": content,
            "url": safe_strip(article.get("url")),
            "urlToImage": safe_strip(article.get("urlToImage")),
            "sentiment_score": score,
            "sentiment_label": label
        })

    if not records:
        logging.warning("⚠ No valid records for sentiment analysis.")
        return

    df = pd.DataFrame(records)
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    df.dropna(subset=["title", "published_at"], inplace=True)

    upload_df_to_s3(df)

# === Entry Point ===
if __name__ == "__main__":
    process_news()
