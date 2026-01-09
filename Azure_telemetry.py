import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.web import WebSiteManagementClient
from azure.mgmt.rdbms.postgresql_flexibleservers import PostgreSQLManagementClient
from azure.mgmt.storage import StorageManagementClient

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

# Database Config
DB_PARAMS = {
    "dbname":   get_env_or_fail("DB_NAME"),
    "user":     get_env_or_fail("DB_USER"),
    "password": get_env_or_fail("DB_PASSWORD"),
    "host":     get_env_or_fail("DB_HOST", "127.0.0.1"),
    "port":     get_env_or_fail("DB_PORT", "5432")
}

SUBSCRIPTION_ID = get_env_or_fail("AZURE_SUBSCRIPTION_ID")

# --- 2. AZURE AUTHENTICATION ---
logging.info(f"🔑 Authenticating with Azure (Sub: {SUBSCRIPTION_ID})...")
try:
    credential = DefaultAzureCredential()
    # Initialize Clients
    compute_client = ComputeManagementClient(credential, SUBSCRIPTION_ID)
    monitor_client = MonitorManagementClient(credential, SUBSCRIPTION_ID)
    network_client = NetworkManagementClient(credential, SUBSCRIPTION_ID)
    web_client = WebSiteManagementClient(credential, SUBSCRIPTION_ID)
    postgres_client = PostgreSQLManagementClient(credential, SUBSCRIPTION_ID)
    storage_client = StorageManagementClient(credential, SUBSCRIPTION_ID)
except Exception as e:
    logging.critical(f"❌ Azure Auth Failed: {e}")
    sys.exit(1)

# --- 3. HELPER FUNCTIONS ---

def get_azure_metric(resource_id, metric_name, timespan):
    """Fetches the latest metric value from Azure Monitor."""
    try:
        metrics_data = monitor_client.metrics.list(
            resource_uri=resource_id,
            timespan=timespan,
            interval='PT5M',  # 5-minute granularity
            metricnames=metric_name,
            aggregation='Average'
        )
        
        if metrics_data.value and metrics_data.value[0].timeseries:
            data_points = metrics_data.value[0].timeseries[0].data
            # Extract valid values
            values = [d.average for d in data_points if d.average is not None]
            
            # Return the last known value
            return max(0.0, values[-1]) if values else 0.0
            
    except Exception as e:
        # Debug log only to prevent spamming
        logging.debug(f"Metric fetch failed for {resource_id.split('/')[-1]}: {e}")
    return 0.0

def parse_rg(resource_id):
    """Extracts Resource Group Name from ID."""
    try:
        parts = resource_id.split('/')
        if 'resourceGroups' in parts:
            return parts[parts.index('resourceGroups') + 1]
    except: pass
    return "Unknown-RG"

# --- 4. MAIN LOOP ---

def main():
    logging.info("🚀 Starting Azure Telemetry Sync...")
    all_metrics = []
    
    now = datetime.now(timezone.utc)
    # Azure API expects strict ISO8601 strings
    start_time = (now - timedelta(minutes=15)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = now.strftime('%Y-%m-%dT%H:%M:%SZ')
    t_span = f"{start_time}/{end_time}"

    # 1. Virtual Machines
    logging.info("📡 Scanning Virtual Machines...")
    try:
        vms = list(compute_client.virtual_machines.list_all())
        logging.info(f"   -> Found {len(vms)} VMs.")
        for vm in vms:
            rg = parse_rg(vm.id)
            cpu = get_azure_metric(vm.id, 'Percentage CPU', t_span)
            
            # Use 'cpu_usage' to match DigitalOcean Droplets
            all_metrics.append((now, "Azure", rg, vm.name, 'cpu_usage', cpu, 100.0, cpu, 'percent'))
            
            # Simple status check
            status = 1.0 if cpu > 0 else 0.0
            all_metrics.append((now, "Azure", rg, vm.name, 'vm_status', status, 1.0, status * 100, 'boolean'))
    except Exception as e:
        logging.error(f"⚠️  VM Scan Failed: {e}")

    # 2. PostgreSQL Flexible
    logging.info("📡 Scanning PostgreSQL...")
    try:
        dbs = list(postgres_client.servers.list())
        for server in dbs:
            rg = parse_rg(server.id)
            cpu = get_azure_metric(server.id, 'cpu_percent', t_span)
            
            all_metrics.append((now, "Azure", rg, server.name, 'db_cpu', cpu, 100.0, cpu, 'percent'))
            
            is_online = 1.0 if server.state == 'Ready' else 0.0
            all_metrics.append((now, "Azure", rg, server.name, 'db_status', is_online, 1.0, is_online * 100, 'boolean'))
    except Exception as e:
        logging.error(f"⚠️  Postgres Scan Failed: {e}")

    # 3. Storage Accounts
    logging.info("📡 Scanning Storage Accounts...")
    try:
        sas = list(storage_client.storage_accounts.list())
        for sa in sas:
            rg = parse_rg(sa.id)
            used_bytes = get_azure_metric(sa.id, 'UsedCapacity', t_span)
            used_gb = used_bytes / (1024**3)
            
            all_metrics.append((now, "Azure", rg, sa.name, 'storage_used', used_gb, 0.0, 0.0, 'GB'))
    except Exception as e:
        logging.error(f"⚠️  Storage Scan Failed: {e}")

    # 4. App Services (Web Apps)
    logging.info("📡 Scanning App Services...")
    try:
        apps = list(web_client.web_apps.list())
        for site in apps:
            rg = site.resource_group
            cpu = get_azure_metric(site.id, 'CpuPercentage', t_span)
            
            # Use 'app_cpu' to match DO App Platform
            all_metrics.append((now, "Azure", rg, site.name, 'app_cpu', cpu, 100.0, cpu, 'percent'))
            
            status = 1.0 if site.state == 'Running' else 0.0
            all_metrics.append((now, "Azure", rg, site.name, 'app_status', status, 1.0, status * 100, 'boolean'))
    except Exception as e:
        logging.error(f"⚠️  App Service Scan Failed: {e}")

    # 5. DB Insert
    if not all_metrics:
        logging.info("ℹ️  No metrics found. Exiting.")
        return

    try:
        logging.info(f"💾 Saving {len(all_metrics)} metrics to DB...")
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                query = """
                INSERT INTO live_telemetry 
                (timestamp, provider, project_name, resource_name, metric_type, used_value, total_value, percent_usage, unit) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cur.executemany(query, all_metrics)
                conn.commit()
        logging.info("✅ Success: Azure Telemetry Synced.")
    except Exception as e:
        logging.error(f"❌ Database Error: {e}")

if __name__ == "__main__":
    main()