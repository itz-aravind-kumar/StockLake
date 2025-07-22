# EquiLake/data_processing/process_stocks.py

import os
import json
import pandas as pd
from datetime import datetime
import boto3
import io
import logging
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


STOCK_SYMBOLS = ["AAPL", "TSLA", "GOOGL", "AMZN", "MSFT"]
TODAY = datetime.today().strftime("%Y-%m-%d")

# === S3 Clients ===
s3 = boto3.client("s3", region_name=AWS_REGION)

# === Download JSON from S3 ===
def download_json_from_s3(s3_key):
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except Exception as e:
        logging.error(f"❌ Failed to download {s3_key}: {e}")
        return {}

# === Upload to S3 (in-memory) ===
def upload_df_to_s3(df, s3_key):
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3.upload_fileobj(buffer, S3_BUCKET, s3_key)
        logging.info(f"✅ Uploaded to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        logging.error(f"❌ Failed to upload to S3: {e}")

# === Main Processing ===
def process_stock_file(symbol):
    s3_key_raw = f"raw/stocks/{symbol}_{TODAY}.json"
    s3_key_processed = f"processed/stocks/processed_stocks_{symbol}_{TODAY}.parquet"

    data = download_json_from_s3(s3_key_raw)
    if not data:
        logging.warning(f"⚠️ No data found for {symbol}")
        return

    # ✅ Debug: Print keys to detect rate limit
    logging.debug(f"🔍 Keys in JSON for {symbol}: {list(data.keys())}")

    # ✅ Detect rate-limiting or API error
    if "Note" in data:
        logging.warning(f"⚠️ API limit hit for {symbol}: {data['Note']}")
        return
    if "Error Message" in data:
        logging.warning(f"⚠️ API error for {symbol}: {data['Error Message']}")
        return

    time_series = data.get("Time Series (Daily)", {})
    if not time_series:
        logging.warning(f"⚠️ No 'Time Series (Daily)' found for {symbol}")
        return

    try:
        df = pd.DataFrame.from_dict(time_series, orient="index").reset_index()
        df.columns = ["date", "open", "high", "low", "close", "volume"]

        df = df.astype({
            "open": "float",
            "high": "float",
            "low": "float",
            "close": "float",
            "volume": "int64"
        })

        df["date"] = pd.to_datetime(df["date"])
        df["symbol"] = symbol.upper()
        df = df[["date", "symbol", "open", "high", "low", "close", "volume"]]

        upload_df_to_s3(df, s3_key_processed)

    except Exception as e:
        logging.error(f"❌ Error processing {symbol}: {e}")

# === Entry Point ===
if __name__ == "__main__":
    logging.info("📊 Starting stock processing for all symbols from S3...")
    for symbol in STOCK_SYMBOLS:
        process_stock_file(symbol)
