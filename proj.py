from dotenv import load_dotenv
import os
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ============================================================
# Load environment variables
# ============================================================
load_dotenv()

BASE_URL = os.getenv("api_url")
API_KEY = os.getenv("api_key")
SYMBOL = os.getenv("symbol")

SNOWFLAKE_USER = os.getenv("snowflake_user")
SNOWFLAKE_PASSWORD = os.getenv("snowflake_password")
SNOWFLAKE_ACCOUNT = os.getenv("snowflake_account")
SNOWFLAKE_WAREHOUSE = os.getenv("snowflake_datawarehouse")
SNOWFLAKE_DATABASE = os.getenv("snowflake_database")
SNOWFLAKE_SCHEMA = os.getenv("snowflake_schema")

# ============================================================
# Fetch stock data from API
# ============================================================
url = f"{BASE_URL}query?function=TIME_SERIES_DAILY&symbol={SYMBOL}&apikey={API_KEY}"
response = requests.get(url)
response.raise_for_status()

data = response.json()
time_series = data.get("Time Series (Daily)")
if not time_series:
    raise ValueError("❌ No time series data found in API response!")

records = []
for date, values in time_series.items():
    records.append({
        "SYMBOL": SYMBOL,
        "DATE": date,
        "OPEN": float(values["1. open"]),
        "HIGH": float(values["2. high"]),
        "LOW": float(values["3. low"]),
        "CLOSE": float(values["4. close"]),
        "VOLUME": int(values["5. volume"])
    })

df = pd.DataFrame(records)
print(f"📊 Prepared {len(df)} records for {SYMBOL}")

# ============================================================
# Connect to Snowflake
# ============================================================
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)
print("✅ Connected to Snowflake successfully!")

# ============================================================
# Create table if not exists
# ============================================================
cur = conn.cursor()
cur.execute(f"""
    CREATE TABLE IF NOT EXISTS STOCK_PRICES (
        SYMBOL STRING,
        DATE DATE,
        OPEN FLOAT,
        HIGH FLOAT,
        LOW FLOAT,
        CLOSE FLOAT,
        VOLUME NUMBER
    )
""")
print("🧱 Table ready in Snowflake!")
cur.close()

# ============================================================
# Write data to Snowflake
# ============================================================
try:
    success, nchunks, nrows, _ = write_pandas(conn, df, 'STOCK_PRICES')
    print(f"✅ Write successful: {success}, inserted {nrows} rows.")
except Exception as e:
    print("❌ Error writing to Snowflake:", e)
finally:
    conn.close()
    print("🔒 Connection closed.")




