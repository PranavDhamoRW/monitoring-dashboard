import os
import sys
import logging
import requests
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone, date

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- 1. ENVIRONMENT SETUP ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(BASE_DIR, "do.env")

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

DO_TOKEN = get_env_or_fail("DO_TOKEN")
HEADERS = {"Authorization": f"Bearer {DO_TOKEN}"}

# Hardcoded fallback, but ideally this comes from a live forex API
USD_TO_INR_RATE = 90.97 

# --- 2. HELPER FUNCTIONS ---

def fetch_all_pages(endpoint, key_override=None):
    """Generic helper to fetch all pages of a DO resource."""
    results = []
    url = f"https://api.digitalocean.com/v2/{endpoint}"
    if "?" not in url: url += "?per_page=100"
    
    while url:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code != 200: 
                logging.error(f"❌ API Error {endpoint}: {resp.status_code}")
                break
            
            data = resp.json()
            # If key not provided, guess it from endpoint (e.g. 'droplets' -> 'droplets')
            key = key_override or endpoint.split('?')[0].split('/')[-1]
            results.extend(data.get(key, []))
            
            url = data.get("links", {}).get("pages", {}).get("next")
        except Exception as e: 
            logging.error(f"⚠️  Fetch Error: {e}")
            break
    return results

def get_month_to_date_usage():
    """Gets the precise 'So Far' bill from the invoice API."""
    try:
        url = "https://api.digitalocean.com/v2/customers/my/balance"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return float(resp.json().get("month_to_date_usage", 0.0))
    except Exception as e:
        logging.error(f"❌ Failed to fetch balance: {e}")
    return 0.0

def estimate_db_price(size_slug):
    """Maps DB slugs to approximate monthly USD price for weighting."""
    if "1vcpu-1gb" in size_slug: return 15.0
    if "1vcpu-2gb" in size_slug: return 30.0
    if "2vcpu-4gb" in size_slug: return 60.0
    if "4vcpu-8gb" in size_slug: return 120.0
    return 15.0 # Fallback default

def build_inventory_weights():
    """
    Builds a list of active resources and assigns a 'Weight' (Price) to each.
    This allows us to distribute the total bill proportionally.
    """
    inv = {}
    
    # 1. Droplets
    droplets = fetch_all_pages("droplets")
    for d in droplets:
        try:
            price = float(d["size"]["price_monthly"])
            if "backups" in d.get("features", []): 
                price *= 1.20
            inv[f"Droplet: {d['name']}"] = {"price": price, "type": "Compute"}
        except: pass

    # 2. Databases
    dbs = fetch_all_pages("databases")
    for db in dbs:
        size = db.get("size", "db-s-1vcpu-1gb")
        inv[f"DB: {db['name']}"] = {"price": estimate_db_price(size), "type": "Database"}

    # 3. Load Balancers (Fixed $12/mo usually)
    lbs = fetch_all_pages("load_balancers")
    for lb in lbs:
        inv[f"LB: {lb['name']}"] = {"price": 12.0, "type": "Networking"}

    # 4. Volumes (Block Storage)
    vols = fetch_all_pages("volumes")
    for v in vols:
        size = float(v["size_gigabytes"])
        inv[f"Vol: {v['name']}"] = {"price": size * 0.10, "type": "Storage"}

    # 5. App Platform
    apps = fetch_all_pages("apps")
    for app in apps:
        try:
            spec = app.get("spec", {})
            name = spec.get("name", "App")
            # Heuristic: $5 base * number of containers
            count = len(spec.get("services", [])) + len(spec.get("workers", []))
            price = max(5.0, count * 5.0)
            inv[f"App: {name}"] = {"price": price, "type": "App Platform"}
        except: pass

    return inv

# --- 3. MAIN EXECUTION ---

def main():
    logging.info("🚀 Starting Billing Sync...")
    
    # Phase 1: Data Gathering (No DB connection yet)
    real_bill_usd = get_month_to_date_usage()
    inventory = build_inventory_weights()
    theoretical_total = sum(item['price'] for item in inventory.values())

    now = datetime.now(timezone.utc)
    # The first day of the current month
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    days_passed = now.day # If today is Jan 5, days_passed = 5
    
    logging.info(f"💰 Real Bill (MTD): ${real_bill_usd:.2f} | 📊 Theoretical Inventory: ${theoretical_total:.2f}")

    if theoretical_total == 0 and real_bill_usd == 0:
        logging.info("ℹ️  No usage and no bill. Exiting.")
        return

    records = []

    # Phase 2: Math (Distribute the Real Bill)
    if theoretical_total > 0:
        # Calculate the "Real" daily average so far
        daily_avg_usd = real_bill_usd / max(days_passed, 1)
        
        # We regenerate the ENTIRE month so far to keep charts accurate
        for day_num in range(1, days_passed + 1):
            loop_date = date(now.year, now.month, day_num)
            
            for r_name, r_data in inventory.items():
                # Weighted Split: (MyPrice / TotalPrice) * RealBill
                ratio = r_data['price'] / theoretical_total
                cost_inr = (daily_avg_usd * ratio) * USD_TO_INR_RATE
                
                clean_name = r_name.split(": ")[1] if ": " in r_name else r_name
                
                records.append((
                    "DigitalOcean", 
                    "Production", # Project Name (Can be dynamic if needed)
                    clean_name, 
                    r_data['type'],
                    round(cost_inr, 2), 
                    "INR", 
                    loop_date, 
                    loop_date
                ))
    elif real_bill_usd > 0:
        # Fallback: Bill exists but API found no resources (deleted?)
        cost_inr = real_bill_usd * USD_TO_INR_RATE
        records.append((
            "DigitalOcean", "Global", "Uncategorized", "Misc", 
            round(cost_inr, 2), "INR", now.date(), now.date()
        ))

    # Phase 3: Database Write (Atomic Transaction)
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                # 1. Clear current month data (Self-Correction Logic)
                logging.info(f"🧹 Clearing existing billing data for {month_start.date()} onwards...")
                cur.execute(
                    "DELETE FROM billing_metrics WHERE provider = 'DigitalOcean' AND period_start >= %s", 
                    (month_start.date(),)
                )
                
                # 2. Insert fresh reconstruction
                if records:
                    logging.info(f"💾 Inserting {len(records)} billing records...")
                    query = """
                        INSERT INTO billing_metrics 
                        (provider, project_name, resource_name, resource_type, amount, currency, period_start, period_end)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cur.executemany(query, records)
                    
        logging.info("✅ Success: Billing sync complete.")
        
    except Exception as e:
        logging.error(f"❌ Database Transaction Failed: {e}")

if __name__ == "__main__":
    main()