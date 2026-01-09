import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from azure.identity import DefaultAzureCredential
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.costmanagement.models import (
    QueryDefinition, QueryTimePeriod, QueryDataset, 
    QueryGrouping, QueryAggregation
)

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- 1. ENVIRONMENT SETUP ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(BASE_DIR, "azure.env")

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

DB_PARAMS = {
    "dbname":   get_env_or_fail("DB_NAME"),
    "user":     get_env_or_fail("DB_USER"),
    "password": get_env_or_fail("DB_PASSWORD"),
    "host":     get_env_or_fail("DB_HOST", "127.0.0.1"),
    "port":     get_env_or_fail("DB_PORT", "5432")
}

SUBSCRIPTION_ID = get_env_or_fail("AZURE_SUBSCRIPTION_ID")
USD_TO_INR_RATE = 90.97
MIN_COST_THRESHOLD = 0.01 

# --- 2. FETCH LOGIC ---
def fetch_daily_costs():
    logging.info("🚀 Starting Azure Billing Sync...")
    
    try:
        credential = DefaultAzureCredential()
        cost_client = CostManagementClient(credential)
    except Exception as e:
        logging.critical(f"❌ Azure Auth Failed: {e}")
        sys.exit(1)

    # Date Range: Last 30 days
    now = datetime.now(timezone.utc)
    end_date = now
    start_date = now - timedelta(days=30)
    
    # Define Query: Group by Resource, Type, and Group
    query = QueryDefinition(
        type="Usage",
        timeframe="Custom",
        time_period=QueryTimePeriod(from_property=start_date, to=end_date),
        dataset=QueryDataset(
            granularity="Daily", 
            aggregation={"totalCost": QueryAggregation(name="Cost", function="Sum")},
            grouping=[
                QueryGrouping(type="Dimension", name="ResourceId"), 
                QueryGrouping(type="Dimension", name="ResourceType"), 
                QueryGrouping(type="Dimension", name="ResourceGroup")
            ]
        )
    )

    scope = f"/subscriptions/{SUBSCRIPTION_ID}"
    
    try:
        logging.info("⏳ Querying Azure Cost Management API (this takes a few seconds)...")
        result = cost_client.query.usage(scope, query)
    except Exception as e:
        logging.error(f"❌ API Query Failed: {e}")
        return []

    # Map columns dynamically (Protection against API changes)
    # result.columns example: [{'name': 'Cost', 'type': 'Number'}, {'name': 'UsageDate', ...}]
    col_map = {col.name: i for i, col in enumerate(result.columns)}
    
    idx_cost = col_map.get('Cost')
    idx_date = col_map.get('UsageDate')
    idx_rid = col_map.get('ResourceId')
    idx_type = col_map.get('ResourceType')
    idx_rg = col_map.get('ResourceGroup')
    idx_curr = col_map.get('Currency')

    records = []
    skipped_count = 0

    logging.info(f" -> Fetched {len(result.rows)} raw rows.")

    for row in result.rows:
        try:
            cost = float(row[idx_cost])
            if cost < MIN_COST_THRESHOLD:
                skipped_count += 1
                continue

            # Azure Date Handling (Can be int 20240101 or str "2024-01-01")
            raw_date = row[idx_date]
            if isinstance(raw_date, int):
                d_str = str(raw_date)
                date_fmt = f"{d_str[:4]}-{d_str[4:6]}-{d_str[6:]}"
            else:
                date_fmt = str(raw_date)[:10]

            # Resource ID Parsing
            r_id = row[idx_rid]
            r_name = r_id.split('/')[-1] if r_id else "Unknown"
            
            # Resource Type Parsing
            r_type_raw = row[idx_type]
            if r_type_raw and '/' in r_type_raw:
                clean_type = r_type_raw.split('/')[-1] # e.g. "virtualMachines"
            else:
                clean_type = r_type_raw

            # Currency Conversion
            currency = row[idx_curr]
            if currency == 'INR':
                final_amount = cost
            else:
                final_amount = cost * USD_TO_INR_RATE

            records.append((
                "Azure",
                row[idx_rg],   # Resource Group -> Project Name
                r_name,        # Resource Name
                clean_type,    # Resource Type
                round(final_amount, 2),
                "INR",
                date_fmt,
                date_fmt
            ))

        except Exception as e:
            logging.warning(f"⚠️  Skipping malformed row: {e}")
            continue

    logging.info(f" -> 🗑️ Skipped {skipped_count} micro-cost rows.")
    return records

# --- 3. DATABASE UPDATE ---
def main():
    records = fetch_daily_costs()
    
    if not records: 
        logging.info("ℹ️  No valid records to insert.")
        return

    # Find earliest date to clean DB
    records.sort(key=lambda x: x[6])
    earliest_date = records[0][6]

    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                # 1. Clean
                logging.info(f"🧹 Clearing Azure data since {earliest_date}...")
                cur.execute(
                    "DELETE FROM billing_metrics WHERE provider = 'Azure' AND period_start >= %s", 
                    (earliest_date,)
                )
                
                # 2. Insert
                logging.info(f"💾 Inserting {len(records)} records...")
                query = """
                INSERT INTO billing_metrics 
                (provider, project_name, resource_name, resource_type, amount, currency, period_start, period_end)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.executemany(query, records)
                
        logging.info("✅ Success: Azure Billing Synced.")

    except Exception as e:
        logging.error(f"❌ Database Transaction Failed: {e}")

if __name__ == "__main__":
    main()