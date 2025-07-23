import os
import json
import pandas as pd
import boto3
import io
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from PIL import Image
from io import BytesIO
import requests
from dotenv import load_dotenv
from datetime import datetime

# Load fetch + process functions
from data_ingestion.fetch_news import fetch_news_for_symbol, upload_json_to_s3 as upload_news_to_s3
from data_ingestion.fetch_stocks import fetch_stock_data, upload_json_to_s3 as upload_stocks_to_s3
from data_processing.process_news import process_news
import data_processing.process_stocks as ps

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

st.set_page_config(page_title="📈 Stock Market Dashboard", layout="wide", initial_sidebar_state="expanded")

@st.cache_data
def load_parquet(bucket, key):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    buffer = io.BytesIO()
    s3.download_fileobj(bucket, key, buffer)
    buffer.seek(0)
    return pd.read_parquet(buffer)

@st.cache_data
def list_s3_keys(prefix):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")]

@st.cache_data
def load_all_stocks():
    keys = list_s3_keys("processed/stocks/")
    dfs = []
    for key in keys:
        try:
            df = load_parquet(S3_BUCKET, key)
            dfs.append(df)
        except Exception:
            pass
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

@st.cache_data
def load_all_news():
    keys = list_s3_keys("processed/news/")
    dfs = []
    for key in keys:
        try:
            df = load_parquet(S3_BUCKET, key)
            dfs.append(df)
        except Exception:
            pass
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

STOCK_SYMBOLS = ["AAPL", "TSLA", "GOOGL", "AMZN", "MSFT"]
TODAY = datetime.today().strftime("%Y-%m-%d")

st.sidebar.markdown("---")
st.sidebar.subheader("🔹️ Manual Trigger")

if st.sidebar.button("📰 Fetch & Process News"):
    with st.spinner("Fetching and processing news..."):
        for symbol in STOCK_SYMBOLS:
            articles = fetch_news_for_symbol(symbol)
            if articles:
                s3_key = f"raw/news/{symbol}_{TODAY}.json"
                upload_news_to_s3(articles, s3_key)
        process_news()
    st.sidebar.success("✅ News Updated!")

if st.sidebar.button("📈 Fetch & Process Stocks"):
    with st.spinner("Fetching and processing stocks..."):
        for symbol in STOCK_SYMBOLS:
            data = fetch_stock_data(symbol)
            if data:
                s3_key = f"raw/stocks/{symbol}_{TODAY}.json"
                upload_stocks_to_s3(data, s3_key)
        ps.process_stocks()
    st.sidebar.success("✅ Stocks Updated!")

stock_df = load_all_stocks()
news_df = load_all_news()

stock_df["date"] = pd.to_datetime(stock_df["date"]).dt.date
symbols = sorted(stock_df["symbol"].dropna().unique())

if not stock_df.empty:
    news_df["published_at"] = pd.to_datetime(news_df["published_at"], errors="coerce")
    news_df.dropna(subset=["published_at", "sentiment_score"], inplace=True)
    news_df["published_date"] = news_df["published_at"].dt.date

    sentiment_daily = (
        news_df.groupby(["symbol", "published_date"])
        .agg(avg_sentiment=("sentiment_score", "mean"))
        .reset_index()
        .rename(columns={"published_date": "date"})
    )

    merged_df = pd.merge(stock_df, sentiment_daily, on=["symbol", "date"], how="left")
    merged_df["date"] = pd.to_datetime(merged_df["date"])

st.sidebar.header("📊 Filters")
selected_symbol = st.sidebar.selectbox("Select Stock Symbol", symbols)
date_min = stock_df["date"].min()
date_max = stock_df["date"].max()
selected_range = st.sidebar.date_input("Date Range", [date_min, date_max])

filtered_df = merged_df[
    (merged_df["symbol"] == selected_symbol) &
    (merged_df["date"] >= pd.to_datetime(selected_range[0])) &
    (merged_df["date"] <= pd.to_datetime(selected_range[1]))
]

st.markdown("""
    <h1 style='font-family: Inter;'>📈 Stock Market Intelligence</h1>
    <p style='color: #999;'>AI-powered insights using stock + news sentiment</p>
""", unsafe_allow_html=True)

if filtered_df.empty:
    st.warning("No data available for selected symbol and range.")
    st.stop()

latest = filtered_df.sort_values("date").iloc[-1]
col1, col2, col3 = st.columns(3)
col1.metric("📅 Latest Date", latest["date"].strftime("%Y-%m-%d"))
col2.metric("💵 Close Price", f"${latest['close']:.2f}")
col3.metric("🧐 Sentiment", f"{latest['avg_sentiment']:.2f}" if pd.notna(latest["avg_sentiment"]) else "N/A")

st.markdown("## 📊 Charts")
col4, col5 = st.columns(2)

with col4:
    st.subheader("📉 Candlestick Chart")
    candle_df = filtered_df.sort_values("date")
    fig_candle = go.Figure(data=[go.Candlestick(
        x=candle_df["date"],
        open=candle_df["open"],
        high=candle_df["high"],
        low=candle_df["low"],
        close=candle_df["close"],
        increasing_line_color='green',
        decreasing_line_color='red'
    )])
    fig_candle.update_layout(template="plotly_dark", margin=dict(t=10))
    st.plotly_chart(fig_candle, use_container_width=True)

with col5:
    st.subheader("🧐 Sentiment Over Time")
    fig_sent = px.line(filtered_df, x="date", y="avg_sentiment", template="plotly_dark",
                       labels={"avg_sentiment": "Sentiment Score", "date": "Date"})
    st.plotly_chart(fig_sent, use_container_width=True)

st.subheader("📈 Volume Traded")
fig_vol = px.bar(filtered_df, x="date", y="volume", template="plotly_dark",
                 labels={"volume": "Volume", "date": "Date"})
st.plotly_chart(fig_vol, use_container_width=True)

PLACEHOLDER_IMAGE = os.path.join("dashboard", "imagee.jpg")
fallback_image = Image.open(PLACEHOLDER_IMAGE)

def fetch_image_safe(url):
    try:
        if not url or not isinstance(url, str) or not url.startswith("http"):
            return fallback_image
        response = requests.get(url, timeout=2)
        if response.status_code == 200:
            return Image.open(BytesIO(response.content))
        else:
            return fallback_image
    except Exception:
        return fallback_image

if not news_df.empty and "title" in news_df.columns:
    st.subheader("📰 Top News Headlines")

    latest_news = news_df[
        news_df["symbol"] == selected_symbol
    ].dropna(subset=["title", "url", "published_at"]).sort_values("published_at", ascending=False).head(5)

    for _, row in latest_news.iterrows():
        title = row["title"]
        url = row["url"]
        source = row.get("source", "Unknown")
        published_at = pd.to_datetime(row["published_at"]).strftime("%b %d, %Y %I:%M %p")
        image_url = row.get("urlToImage", "")

        final_image = fetch_image_safe(image_url)

        cols = st.columns([1, 6])
        with cols[0]:
            st.image(final_image, width=90)
        with cols[1]:
            st.markdown(f"""
                <div style="line-height: 1.5">
                    <a href="{url}" target="_blank" style="text-decoration:none;">
                        <h4 style="margin-bottom:5px; color:#ffffff;">{title}</h4>
                    </a>
                    <p style="font-size:14px; color:#ccc;">{source} • {published_at}</p>
                </div>
            """, unsafe_allow_html=True)

st.subheader("📄 Full Merged Data")
st.dataframe(filtered_df.drop_duplicates().sort_values("date", ascending=False), use_container_width=True)

st.markdown("---")
st.caption("🔍 Built with ❤️ using Streamlit, AWS, NLP, and Python.")
