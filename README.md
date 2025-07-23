# 📊 EquiLake – Stock Market Intelligence Data Lake

EquiLake is a modern data lake solution that combines real-time stock prices with AI-powered news sentiment analysis. Built with Python, AWS, NLP, and Streamlit — it empowers users with actionable market insights in a visual, interactive dashboard.

---

## 🌐 Live Website

[https://your-website-link.com](https://your-website-link.com)

---

## 🔍 Features

- 📰 **News Sentiment Analysis**  
  Real-time news fetched for each stock symbol and analyzed with NLP (TextBlob) for sentiment (positive/neutral/negative).

- 📈 **Stock Price Ingestion**  
  Daily stock prices from Alpha Vantage for top companies like AAPL, MSFT, GOOGL, TSLA, AMZN.

- 🧠 **Merged Insights**  
  Stock prices and news sentiment are combined by date & symbol to show how news may impact market movement.

- 🎨 **Interactive Streamlit Dashboard**  
  Visualize candlestick charts, volume trends, sentiment timelines, and recent news headlines per stock.

- ☁️ **Cloud-Native Architecture**  
  All raw and processed data is stored on **Amazon S3** using a structured data lake format.

---

## 📂 Project Structure

```markdown
EquiLake/
├── data_ingestion/
│   ├── fetch_stocks.py        # Fetch daily stock data from Alpha Vantage
│   └── fetch_news.py          # Fetch news articles per stock from News API
│
├── data_processing/
│   ├── process_stocks.py      # Process and clean stock data
│   └── process_news.py        # Perform sentiment analysis on news
│
├── dashboard/
│   ├── streamlit_app.py       # Streamlit UI with manual triggers + insights
│   └── imagee.jpg             # Placeholder for fallback news images
│
├── .streamlit/
│   └── secrets.toml           # API keys & AWS secrets (for Streamlit Cloud)
│
├── .env                       # Local env file for dev (gitignored)
├── requirements.txt           # Python dependencies
└── README.md                  # You're here!
```


---

## 🔧 Technologies Used

- 🐍 Python (Pandas, Requests, TextBlob, Boto3)
- 📦 Streamlit for dashboard UI
- ☁️ AWS S3 as the storage backend (raw + processed)
- 🔑 News API + Alpha Vantage API
- 📈 Plotly for interactive visualizations

---

## 🚀 How It Works

1. **Manual Data Fetching**
   - Run from Streamlit sidebar:
     - `📰 Fetch & Process News`
     - `📈 Fetch & Process Stocks`

2. **Data Pipeline Flow**
   - Raw data is stored in `s3://<bucket>/raw/`
   - Processed data (sentiment, cleaned stock prices) → `s3://<bucket>/processed/`

3. **Dashboard Visuals**
   - Filter by stock symbol and date range
   - View candlestick chart + sentiment overlay
   - See volume traded and news headlines with thumbnails

---

## 🔐 Setup (Local)

### 1. Clone the repo

```markdown
git clone https://github.com/your-username/EquiLake.git
cd EquiLake
```

### 2. Install dependencies

```markdown
pip install -r requirements.txt
```

### 3. Add `.env` file

Create a `.env` file in the root directory with the following:

```markdown
NEWS_API_KEY=your_newsapi_key
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_REGION=your_region
S3_BUCKET=your_bucket_name
```

### 4. Run the Streamlit app

```markdown
streamlit run dashboard/streamlit_app.py
```

---

## 🌐 Deployment (Streamlit Cloud)

1. Push code to GitHub  
2. Go to [streamlit.io/cloud](https://streamlit.io/cloud)  
3. Connect your GitHub repo  
4. Add your API keys in **secrets.toml** format:

```markdown
NEWS_API_KEY = "your_newsapi_key"
ALPHA_VANTAGE_API_KEY = "your_alpha_vantage_key"
AWS_ACCESS_KEY_ID = "your_aws_key"
AWS_SECRET_ACCESS_KEY = "your_aws_secret"
AWS_REGION = "your_region"
S3_BUCKET = "your_bucket_name"
```

---

## 📌 To-Do / Future Enhancements

- 📊 Integrate AWS Athena for SQL-like queries over S3  
- 🗓️ Automate crawlers via AWS Lambda / Cron  
- 🧠 Use a more powerful LLM (e.g. GPT-4) for better sentiment/context analysis  
- 📲 Add notifications or alerts for sentiment/price spikes  

---

## 🙋‍♂️ Author

**Aravind Kumar Muddana**  

---

## 💡 Inspiration

Inspired by the need to track how news impacts stock prices, **StockLake** was built as a smart, extensible data lake project perfect for data-driven decision making and portfolio-building.

---

## ⭐️ Show Your Support
