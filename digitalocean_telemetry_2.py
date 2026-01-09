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

# --- 2. HELPER FUNCTIONS ---

def fetch_do_data_paginated(endpoint):
    """Fetches all pages of a resource (Apps, DBs, Volumes)."""
    results = []
    url = f"https://api.digitalocean.com/v2/{endpoint}"
    if "per_page=" not in url:
        url += ("&" if "?" in url else "?") + "per_page=100"

    page_count = 0
    MAX_PAGES = 10

    while url and page_count < MAX_PAGES:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code == 401:
                logging.error("❌ Authentication Failed! Check DO_TOKEN.")
                break
            resp.raise_for_status()
            
            data = resp.json()
            # Auto-detect the key (e.g., 'apps', 'databases', 'volumes')
            key = endpoint.split('?')[0].split('/')[-1]
            results.extend(data.get(key, []))
            
            url = data.get("links", {}).get("pages", {}).get("next")
            page_count += 1
        except Exception as e:
            logging.error(f"Error fetching {endpoint}: {e}")
            break
    return results

def get_app_metric(app_id, metric_name, params):
    """Fetches a specific metric for an App Platform instance."""
    url = f"https://api.digitalocean.com/v2/apps/{app_id}/metrics/{metric_name}"
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json().get("data", {}).get("result", [])
            if data and data[0].get("values"):
                # Return the last value in the series
                return float(data[0]["values"][-1][1])
    except Exception:
        pass
    return 0.0

# --- 3. MAIN EXECUTION ---

def main():
    logging.info("🚀 Starting Resource Sync (Apps, DBs, Storage)...")
    
    records = []
    ts = datetime.now(timezone.utc)
    
    # Time window for App Platform metrics
    m_params = {
        "start": str(int((ts - timedelta(minutes=5)).timestamp())), 
        "end": str(int(ts.timestamp()))
    }

    # --- 1. APP PLATFORM ---
    apps = fetch_do_data_paginated("apps")
    logging.info(f" -> Found {len(apps)} Apps. Scanning metrics...")
    
    for app in apps:
        try:
            app_id = app["id"]
            name = app["spec"]["name"]
            
            # Fetch usage safely
            cpu_pct = max(0.0, get_app_metric(app_id, "cpu_percentage", m_params))
            ram_pct = max(0.0, get_app_metric(app_id, "memory_percentage", m_params))
            
            # Note: We use "app_cpu" to distinguish from Droplet "cpu_usage"
            records.append((ts, "DigitalOcean", "App Platform", name, "app_cpu", cpu_pct, 100.0, cpu_pct, "percent"))
            records.append((ts, "DigitalOcean", "App Platform", name, "app_ram", ram_pct, 100.0, ram_pct, "percent"))
        except Exception as e:
            logging.warning(f"⚠️  Skipping App {app.get('spec', {}).get('name', 'Unknown')}: {e}")

    # --- 2. MANAGED DATABASES ---
    dbs = fetch_do_data_paginated("databases")
    logging.info(f" -> Found {len(dbs)} Databases.")
    
    for db in dbs:
        # Simple Status Check: 1 = Online, 0 = Offline/Maintenance
        status = 1.0 if db.get("status") == "online" else 0.0
        records.append((ts, "DigitalOcean", "Databases", db["name"], "db_status", status, 1.0, status * 100, "boolean"))

    # --- 3. VOLUMES (BLOCK STORAGE) ---
    vols = fetch_do_data_paginated("volumes")
    logging.info(f" -> Found {len(vols)} Volumes.")
    
    for v in vols:
        # Renamed metric to prevent pollution of server disk charts
        size_gb = float(v["size_gigabytes"])
        records.append((ts, "DigitalOcean", "Storage", v["name"], "volume_provisioned_gb", 0.0, size_gb, 0.0, "GiB"))

    if not records:
        logging.info("ℹ️  No records to save. Exiting.")
        return

    # --- 4. DATABASE WRITE (Atomic & Fast) ---
    try:
        logging.info(f"💾 Saving {len(records)} records to DB...")
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO live_telemetry 
                    (timestamp, provider, project_name, resource_name, metric_type, used_value, total_value, percent_usage, unit) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.executemany(query, records)
        logging.info("✅ Success: Sync complete.")

    except Exception as e:
        logging.error(f"❌ Database Transaction Failed: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Script stopped by user.")