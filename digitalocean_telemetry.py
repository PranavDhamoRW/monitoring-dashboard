import os
import sys
import logging
import requests
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# --- LOGGING CONFIGURATION ---
# Sets up professional logging with timestamps.
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- 1. ENVIRONMENT SETUP ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(BASE_DIR, "do.env")

# Robust Config Loading
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path, override=True)
else:
    logging.warning(f"Config file not found at {dotenv_path}. Relying on System Environment.")

def get_env_or_fail(key, default=None):
    """Retrieves env var or crashes intentionally if missing."""
    value = os.getenv(key, default)
    if value is None:
        logging.critical(f"❌ CRITICAL ERROR: Missing environment variable: {key}")
        sys.exit(1)
    return value

# Database & API Config
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

def get_inventory():
    """
    Fetches active droplets. 
    Includes circuit breaker for infinite loops and filters bad data.
    """
    droplets = {}
    url = "https://api.digitalocean.com/v2/droplets?per_page=100"
    
    page_count = 0
    MAX_PAGES = 10  # Prevent infinite loops

    while url and page_count < MAX_PAGES:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            
            if resp.status_code == 401:
                logging.error("❌ Authentication Failed! Check your DO_TOKEN.")
                break 
            
            resp.raise_for_status()
            data = resp.json()
            
            for d in data.get("droplets", []):
                if d.get("status") != "active": 
                    continue
                
                size_info = d.get("size", {})
                ram_mb = float(size_info.get("memory", 0))
                
                # CRITICAL SAFETY: Filter out invalid specs to prevent DivByZero later
                if ram_mb <= 0:
                    logging.warning(f"⚠️  Skipping droplet {d.get('name')}: Invalid RAM reported.")
                    continue

                droplets[d["id"]] = {
                    "name": d["name"],
                    "project": (d.get("tags") or ["Uncategorized"])[0],
                    "cpu_total": float(size_info.get("vcpus", 1)), 
                    "ram_total": ram_mb / 1024.0, # Convert MB to GB
                    "disk_total": float(size_info.get("disk", 0))
                }
            
            url = data.get("links", {}).get("pages", {}).get("next")
            page_count += 1
            
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Inventory API Error: {e}")
            break
            
    return droplets

def fetch_series_data(url, params):
    """Fetches raw metric series data with proper error handling."""
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
        
        if resp.status_code != 200:
            logging.warning(f"⚠️  Metric Fetch Failed [{resp.status_code}]: {resp.text[:50]}...")
            return []
            
        return resp.json().get("data", {}).get("result", [])
        
    except requests.exceptions.Timeout:
        logging.error(f"⏳ Timeout fetching metrics from {url}")
        return []
    except Exception as e:
        logging.error(f"❌ Unexpected Fetch Error: {e}")
        return []

def calculate_rate(values):
    """Calculates rate of change between the last two data points."""
    if not values or len(values) < 2: 
        return 0.0
    try:
        t_prev, v_prev = float(values[-2][0]), float(values[-2][1])
        t_curr, v_curr = float(values[-1][0]), float(values[-1][1])
        
        time_diff = t_curr - t_prev
        val_diff = v_curr - v_prev
        
        if time_diff <= 0: return 0.0
        if val_diff < 0: return 0.0 # Ignore counter resets
        
        return val_diff / time_diff
    except (ValueError, IndexError):
        return 0.0

def process_droplet(d_id, specs):
    """
    Fetches and processes metrics for a single droplet.
    Protected against Divide-by-Zero errors.
    """
    GIB = 1024 ** 3
    MBPS_CONV = 8 / (1024 * 1024) # Bytes -> Bits -> Megabits
    metrics = []
    
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=5)
    params = {
        "host_id": str(d_id), 
        "start": str(int(start.timestamp())), 
        "end": str(int(end.timestamp()))
    }
    base = "https://api.digitalocean.com/v2/monitoring/metrics/droplet"

    def get_latest_val(data):
        if data and data[0].get("values"):
            try: return float(data[0]["values"][-1][1])
            except (IndexError, ValueError): pass
        return 0.0

    # --- 1. CPU ---
    cpu_res = fetch_series_data(f"{base}/cpu", params)
    total_idle_rate = 0.0
    found_cpu = False
    
    for res in cpu_res:
        if res.get("metric", {}).get("mode") == "idle":
            total_idle_rate += calculate_rate(res.get("values", []))
            found_cpu = True
    
    if found_cpu and specs["cpu_total"] > 0:
        idle_norm = max(0.0, min(1.0, total_idle_rate / specs["cpu_total"]))
        metrics.append((
            "cpu_usage", 
            (1.0 - idle_norm) * specs["cpu_total"], 
            specs["cpu_total"], 
            (1.0 - idle_norm) * 100, 
            "vCPU"
        ))

    # --- 2. RAM ---
    # Formula: Used = Total - (Free + Cached + Buffers)
    mem_free = get_latest_val(fetch_series_data(f"{base}/memory_free", params))
    mem_cached = get_latest_val(fetch_series_data(f"{base}/memory_cached", params))
    mem_buffers = get_latest_val(fetch_series_data(f"{base}/memory_buffers", params))
    
    if specs["ram_total"] > 0:
        free_gb = (mem_free + mem_cached + mem_buffers) / GIB
        used_gb = max(0.0, specs["ram_total"] - free_gb)
        metrics.append(("ram_usage", used_gb, specs["ram_total"], (used_gb / specs["ram_total"]) * 100, "GiB"))

    # --- 3. Disk ---
    disk_data = fetch_series_data(f"{base}/filesystem_free", params)
    root_free_gb = 0.0
    found_disk = False
    
    for res in disk_data:
        m = res.get("metric", {})
        if m.get("fstype") in ["tmpfs", "devtmpfs", "overlay"]: continue
        if m.get("mountpoint") == "/" or any(x in m.get("device", "") for x in ["vda", "sda", "nvme"]):
            root_free_gb = get_latest_val([res]) / GIB
            found_disk = True
            if m.get("mountpoint") == "/": break
            
    if found_disk and specs["disk_total"] > 0:
        used_disk = max(0.0, specs["disk_total"] - root_free_gb)
        metrics.append(("disk_usage", used_disk, specs["disk_total"], (used_disk / specs["disk_total"]) * 100, "GiB"))

    # --- 4. Bandwidth ---
    for dr in ["inbound", "outbound"]:
        net = fetch_series_data(f"{base}/bandwidth", {**params, "interface": "public", "direction": dr})
        if net and net[0].get("values"):
            mbps = (calculate_rate(net[0]["values"]) * 8) / (1024 * 1024)
            metrics.append((f"net_{dr}", mbps, 0.0, 0.0, "Mbps"))

    return metrics

# --- 3. MAIN EXECUTION ---

def main():
    logging.info("🚀 Starting telemetry collection...")
    
    # Phase 1: Fetch all data (Long duration, no DB connection)
    inventory = get_inventory()
    if not inventory: 
        logging.info("ℹ️  No active droplets found. Exiting.")
        return
    
    records = []
    ts = datetime.now(timezone.utc)
    
    for d_id, specs in inventory.items():
        try:
            for m in process_droplet(d_id, specs):
                # m = (type, used, total, pct, unit)
                records.append((
                    ts, "DigitalOcean", specs["project"], specs["name"], 
                    m[0], round(m[1], 4), round(m[2], 4), round(m[3], 2), m[4]
                ))
        except Exception as e: 
            logging.error(f"⚠️  Error processing {specs['name']}: {e}")

    if not records:
        logging.info("ℹ️  No metrics collected. Exiting.")
        return

    # Phase 2: Save to DB (Short duration, atomic transaction)
    try:
        logging.info(f"💾 Connecting to DB to save {len(records)} records...")
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO live_telemetry 
                    (timestamp, provider, project_name, resource_name, metric_type, used_value, total_value, percent_usage, unit) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.executemany(query, records)
        logging.info(f"✅ Success: Data saved successfully.")
        
    except psycopg2.OperationalError as e:
        logging.error(f"❌ Database Connection Failed: {e}")
    except Exception as e:
        logging.error(f"❌ Database Transaction Failed: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n🛑 Script stopped by user.")