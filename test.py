import pandas as pd
df = pd.read_parquet("s3_structure/processed/processed_news_2025-07-20.parquet")
print(df[["published_at", "sentiment_score"]].tail(5))
