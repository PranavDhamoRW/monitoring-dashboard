import os
import sys
import logging
import requests
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- 1. ENVIRONMENT SETUP ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(BASE_DIR, "openai.env") # Keeping your specific env file

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    logging.warning(f"Config file not found at {dotenv_path}. Relying on System Environment.")

def get_env_or_fail(key, default=None):
    value = os.getenv(key, default)
    if value is None:
        logging.critical(f"❌ CRITICAL ERROR: Missing environment variable: {key}")
        sys.exit(1)
    return value

# Database Config (Now matches other scripts)
DB_PARAMS = {
    "dbname":   get_env_or_fail("DB_NAME"),
    "user":     get_env_or_fail("DB_USER"),
    "password": get_env_or_fail("DB_PASSWORD"),
    "host":     get_env_or_fail("DB_HOST", "127.0.0.1"),
    "port":     get_env_or_fail("DB_PORT", "5432")
}

OPENAI_API_KEY = get_env_or_fail("OPENAI_API_KEY")
USD_TO_INR_RATE = 90.97
MIN_COST_THRESHOLD = 0.0001 

# --- 2. FETCH LOGIC ---
def fetch_daily_costs():
    logging.info("🚀 Starting OpenAI Billing Sync...")
    
    # 1. Setup Dates (Last 30 days)
    now = datetime.now(timezone.utc)
    # End of today
    end_date = now.replace(hour=23, minute=59, second=59)
    # Start of 30 days ago
    start_date = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0)
    
    url = "https://api.openai.com/v1/organization/costs"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    
    # OpenAI requires Unix timestamps for this endpoint
    params = {
        "start_time": int(start_date.timestamp()),
        "end_time": int(end_date.timestamp()),
        "limit": 100,
        "group_by": "project_id" # Critical for splitting costs by project
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logging.error(f"❌ API Error: {e}")
        return []

    buckets = data.get("data", [])
    logging.info(f" -> Fetched {len(buckets)} daily buckets from OpenAI.")

    records = []

    for bucket in buckets:
        # OpenAI returns timestamps (e.g. 1704067200)
        ts_start = bucket.get("start_time")
        
        # Convert to YYYY-MM-DD for DB
        if ts_start:
            period_date = datetime.fromtimestamp(ts_start, timezone.utc).date()
        else:
            continue # Skip invalid data

        for result in bucket.get("results", []):
            try:
                amount_obj = result.get("amount", {})
                cost_usd = float(amount_obj.get("value", 0.0))
                
                if cost_usd < MIN_COST_THRESHOLD:
                    continue

                # Project Name Fallback
                p_name = result.get("project", {}).get("name") 
                if not p_name:
                     p_name = result.get("project_id", "Default Project")
                
                # Calculate INR
                final_amount = cost_usd * USD_TO_INR_RATE

                records.append((
                    "OpenAI",            # Provider
                    p_name,              # Project Name
                    "Model Inference",   # Resource Name (Generic for now)
                    "AI Service",        # Resource Type
                    final_amount,        # Amount
                    "INR",               # Currency
                    period_date,         # Period Start
                    period_date          # Period End
                ))

            except Exception as e:
                logging.warning(f"⚠️  Skipping malformed row: {e}")
                continue

    return records

# --- 3. DATABASE UPDATE ---
def main():
    records = fetch_daily_costs()
    
    if not records:
        logging.info("ℹ️  No usage found for the last 30 days.")
        return

    # Sort to find the earliest date we need to clear from DB
    records.sort(key=lambda x: x[6])
    earliest_date = records[0][6]

    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                # 1. Clean old data (Overlapping window)
                logging.info(f"🧹 Clearing OpenAI data since {earliest_date}...")
                cur.execute(
                    "DELETE FROM billing_metrics WHERE provider = 'OpenAI' AND period_start >= %s", 
                    (earliest_date,)
                )
                
                # 2. Insert fresh data
                logging.info(f"💾 Inserting {len(records)} records...")
                query = """
                    INSERT INTO billing_metrics 
                    (provider, project_name, resource_name, resource_type, amount, currency, period_start, period_end)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.executemany(query, records)
                
        logging.info("✅ Success: OpenAI Billing Synced.")

    except Exception as e:
        logging.error(f"❌ Database Transaction Failed: {e}")

if __name__ == "__main__":
    main()