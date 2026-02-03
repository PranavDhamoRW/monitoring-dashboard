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

# --- 2. VM SIZE SPECS LOOKUP ---
# Maps Azure VM sizes to their specs (vCPUs, RAM in GB)
# Source: https://learn.microsoft.com/en-us/azure/virtual-machines/sizes
VM_SIZE_SPECS = {
    # B-Series (Burstable)
    "Standard_B1s": (1, 1), "Standard_B1ms": (1, 2), "Standard_B2s": (2, 4),
    "Standard_B2ms": (2, 8), "Standard_B4ms": (4, 16), "Standard_B8ms": (8, 32),
    "Standard_B12ms": (12, 48), "Standard_B16ms": (16, 64), "Standard_B20ms": (20, 80),
    
    # D-Series (General Purpose v3)
    "Standard_D2s_v3": (2, 8), "Standard_D4s_v3": (4, 16), "Standard_D8s_v3": (8, 32),
    "Standard_D16s_v3": (16, 64), "Standard_D32s_v3": (32, 128), "Standard_D48s_v3": (48, 192),
    "Standard_D64s_v3": (64, 256),
    
    # D-Series (General Purpose v4)
    "Standard_D2s_v4": (2, 8), "Standard_D4s_v4": (4, 16), "Standard_D8s_v4": (8, 32),
    "Standard_D16s_v4": (16, 64), "Standard_D32s_v4": (32, 128), "Standard_D48s_v4": (48, 192),
    
    # D-Series (General Purpose v5)
    "Standard_D2s_v5": (2, 8), "Standard_D4s_v5": (4, 16), "Standard_D8s_v5": (8, 32),
    "Standard_D16s_v5": (16, 64), "Standard_D32s_v5": (32, 128),
    
    # E-Series (Memory Optimized v3)
    "Standard_E2s_v3": (2, 16), "Standard_E4s_v3": (4, 32), "Standard_E8s_v3": (8, 64),
    "Standard_E16s_v3": (16, 128), "Standard_E20s_v3": (20, 160), "Standard_E32s_v3": (32, 256),
    "Standard_E48s_v3": (48, 384), "Standard_E64s_v3": (64, 432),
    
    # E-Series (Memory Optimized v4)
    "Standard_E2s_v4": (2, 16), "Standard_E4s_v4": (4, 32), "Standard_E8s_v4": (8, 64),
    "Standard_E16s_v4": (16, 128), "Standard_E32s_v4": (32, 256),
    
    # F-Series (Compute Optimized v2)
    "Standard_F2s_v2": (2, 4), "Standard_F4s_v2": (4, 8), "Standard_F8s_v2": (8, 16),
    "Standard_F16s_v2": (16, 32), "Standard_F32s_v2": (32, 64), "Standard_F48s_v2": (48, 96),
    "Standard_F64s_v2": (64, 128), "Standard_F72s_v2": (72, 144),
    
    # L-Series (Storage Optimized v2)
    "Standard_L8s_v2": (8, 64), "Standard_L16s_v2": (16, 128), "Standard_L32s_v2": (32, 256),
    
    # M-Series (Large Memory)
    "Standard_M8ms": (8, 218), "Standard_M16ms": (16, 437), "Standard_M32ms": (32, 875),
    "Standard_M64ms": (64, 1750), "Standard_M128ms": (128, 3800),
}

def get_vm_specs(vm_size):
    """
    Returns (vCPUs, RAM_GB) for a given VM size.
    Falls back to estimation if size not in lookup table.
    """
    if vm_size in VM_SIZE_SPECS:
        return VM_SIZE_SPECS[vm_size]
    
    # Fallback: Try to parse from size name (e.g., Standard_D4s_v3 → 4 vCPUs)
    # This is a heuristic and may not always be accurate
    try:
        parts = vm_size.split('_')
        if len(parts) >= 2:
            # Extract number from size (D4 → 4, E8 → 8, etc.)
            size_str = parts[1]
            num = int(''.join(filter(str.isdigit, size_str)))
            
            # Estimate RAM based on series
            if 'E' in size_str:  # Memory optimized: 8GB per vCPU
                ram = num * 8
            elif 'F' in size_str:  # Compute optimized: 2GB per vCPU
                ram = num * 2
            elif 'B' in size_str:  # Burstable: 4GB per vCPU
                ram = num * 4
            else:  # Default (D-series): 4GB per vCPU
                ram = num * 4
                
            logging.warning(f"⚠️  VM size '{vm_size}' not in lookup table. Estimated: {num} vCPUs, {ram} GB RAM")
            return (num, ram)
    except:
        pass
    
    # Last resort fallback
    logging.warning(f"⚠️  Could not determine specs for VM size '{vm_size}'. Using defaults: 2 vCPUs, 8 GB RAM")
    return (2, 8)

# --- 3. AZURE AUTHENTICATION ---
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

# --- 4. HELPER FUNCTIONS ---

def get_azure_metric(resource_id, metric_name, timespan, aggregation='Average'):
    """
    Fetches the latest metric value from Azure Monitor.
    
    FIXED: Now supports different aggregation types:
    - Average: For percentages and rates (CPU %, Memory %)
    - Total: For cumulative counters (Network bytes)
    - Maximum: For capacity gauges (Storage capacity)
    """
    try:
        metrics_data = monitor_client.metrics.list(
            resource_uri=resource_id,
            timespan=timespan,
            interval='PT5M',  # 5-minute granularity
            metricnames=metric_name,
            aggregation=aggregation
        )
        
        if metrics_data.value and metrics_data.value[0].timeseries:
            data_points = metrics_data.value[0].timeseries[0].data
            
            # Extract values based on aggregation type
            if aggregation == 'Average':
                values = [d.average for d in data_points if d.average is not None]
            elif aggregation == 'Total':
                values = [d.total for d in data_points if d.total is not None]
            elif aggregation == 'Maximum':
                values = [d.maximum for d in data_points if d.maximum is not None]
            else:
                values = [d.average for d in data_points if d.average is not None]
            
            # Return the last known value
            return max(0.0, values[-1]) if values else 0.0
            
    except Exception as e:
        # Debug log only to prevent spamming
        logging.debug(f"Metric fetch failed for {resource_id.split('/')[-1]} ({metric_name}): {e}")
    return 0.0

def calculate_rate(current, previous, time_diff_seconds):
    """
    Calculates rate of change (e.g., bytes/sec).
    Handles counter resets gracefully.
    """
    if time_diff_seconds <= 0:
        return 0.0
    
    diff = current - previous
    if diff < 0:  # Counter reset
        return 0.0
    
    return diff / time_diff_seconds

def parse_rg(resource_id):
    """Extracts Resource Group Name from ID."""
    try:
        parts = resource_id.split('/')
        if 'resourceGroups' in parts:
            return parts[parts.index('resourceGroups') + 1]
    except: pass
    return "Unknown-RG"

# --- 5. MAIN LOOP ---

def main():
    logging.info("🚀 Starting Azure Telemetry Sync...")
    all_metrics = []
    
    now = datetime.now(timezone.utc)
    # Azure API expects strict ISO8601 strings
    start_time = (now - timedelta(minutes=15)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time = now.strftime('%Y-%m-%dT%H:%M:%SZ')
    t_span = f"{start_time}/{end_time}"

    # --- 1. VIRTUAL MACHINES (FIXED) ---
    logging.info("📡 Scanning Virtual Machines...")
    try:
        vms = list(compute_client.virtual_machines.list_all())
        logging.info(f"   -> Found {len(vms)} VMs.")
        
        for vm in vms:
            rg = parse_rg(vm.id)
            vm_size = vm.hardware_profile.vm_size
            vcpu_total, ram_total_gb = get_vm_specs(vm_size)
            
            # CPU (FIXED: Now reports vCPU usage, not just percentage)
            cpu_percent = get_azure_metric(vm.id, 'Percentage CPU', t_span, 'Average')
            cpu_used_vcpu = (cpu_percent / 100.0) * vcpu_total
            
            all_metrics.append((
                now, "Azure", rg, vm.name, 
                'cpu_usage', 
                cpu_used_vcpu,  # e.g., 1.5 vCPUs
                vcpu_total,     # e.g., 2 vCPUs
                cpu_percent,    # e.g., 75%
                'vCPU'
            ))
            
            # RAM (NEW: Added memory metrics)
            # Azure provides "Available Memory Bytes" which matches Linux "available" memory
            mem_available_bytes = get_azure_metric(vm.id, 'Available Memory Bytes', t_span, 'Average')
            mem_available_gb = mem_available_bytes / (1024**3)
            mem_used_gb = max(0.0, ram_total_gb - mem_available_gb)
            mem_percent = (mem_used_gb / ram_total_gb) * 100 if ram_total_gb > 0 else 0.0
            
            all_metrics.append((
                now, "Azure", rg, vm.name,
                'ram_usage',
                mem_used_gb,    # e.g., 6.2 GB
                ram_total_gb,   # e.g., 8 GB
                mem_percent,    # e.g., 77.5%
                'GiB'
            ))
            
            # Network Bandwidth (NEW: Added network metrics)
            # Azure provides cumulative totals, we need to calculate rates
            net_in_total = get_azure_metric(vm.id, 'Network In Total', t_span, 'Total')
            net_out_total = get_azure_metric(vm.id, 'Network Out Total', t_span, 'Total')
            
            # Convert bytes to Mbps (assume data is over 15-minute window)
            time_window_sec = 15 * 60  # 15 minutes in seconds
            net_in_mbps = (net_in_total * 8) / (time_window_sec * 1024 * 1024) if net_in_total > 0 else 0.0
            net_out_mbps = (net_out_total * 8) / (time_window_sec * 1024 * 1024) if net_out_total > 0 else 0.0
            
            all_metrics.append((now, "Azure", rg, vm.name, 'net_inbound', net_in_mbps, 0.0, 0.0, 'Mbps'))
            all_metrics.append((now, "Azure", rg, vm.name, 'net_outbound', net_out_mbps, 0.0, 0.0, 'Mbps'))
            
            # Status (improved but still simple)
            status = 1.0 if cpu_percent > 0 else 0.0
            all_metrics.append((now, "Azure", rg, vm.name, 'vm_status', status, 1.0, status * 100, 'boolean'))
            
            # Debug logging
            logging.debug(
                f"{vm.name} ({vm_size}): CPU={cpu_used_vcpu:.2f}/{vcpu_total} vCPUs ({cpu_percent:.1f}%), "
                f"RAM={mem_used_gb:.2f}/{ram_total_gb} GB ({mem_percent:.1f}%)"
            )
            
    except Exception as e:
        logging.error(f"⚠️  VM Scan Failed: {e}")

    # --- 2. POSTGRESQL FLEXIBLE ---
    logging.info("📡 Scanning PostgreSQL...")
    try:
        dbs = list(postgres_client.servers.list())
        for server in dbs:
            rg = parse_rg(server.id)
            
            # CPU
            cpu = get_azure_metric(server.id, 'cpu_percent', t_span, 'Average')
            all_metrics.append((now, "Azure", rg, server.name, 'db_cpu', cpu, 100.0, cpu, 'percent'))
            
            # Memory (if available)
            mem_percent = get_azure_metric(server.id, 'memory_percent', t_span, 'Average')
            if mem_percent > 0:
                all_metrics.append((now, "Azure", rg, server.name, 'db_memory', mem_percent, 100.0, mem_percent, 'percent'))
            
            # Status
            is_online = 1.0 if server.state == 'Ready' else 0.0
            all_metrics.append((now, "Azure", rg, server.name, 'db_status', is_online, 1.0, is_online * 100, 'boolean'))
            
    except Exception as e:
        logging.error(f"⚠️  Postgres Scan Failed: {e}")

    # --- 3. STORAGE ACCOUNTS (FIXED) ---
    logging.info("📡 Scanning Storage Accounts...")
    try:
        sas = list(storage_client.storage_accounts.list())
        for sa in sas:
            rg = parse_rg(sa.id)
            
            # FIXED: Use 'Maximum' aggregation for capacity metrics
            used_bytes = get_azure_metric(sa.id, 'UsedCapacity', t_span, 'Maximum')
            used_gb = used_bytes / (1024**3)
            
            all_metrics.append((now, "Azure", rg, sa.name, 'storage_used', used_gb, 0.0, 0.0, 'GB'))
            
    except Exception as e:
        logging.error(f"⚠️  Storage Scan Failed: {e}")

    # --- 4. APP SERVICES (WEB APPS) ---
    logging.info("📡 Scanning App Services...")
    try:
        apps = list(web_client.web_apps.list())
        for site in apps:
            rg = site.resource_group
            
            # CPU
            cpu = get_azure_metric(site.id, 'CpuPercentage', t_span, 'Average')
            all_metrics.append((now, "Azure", rg, site.name, 'app_cpu', cpu, 100.0, cpu, 'percent'))
            
            # Memory (if available)
            mem_percent = get_azure_metric(site.id, 'MemoryPercentage', t_span, 'Average')
            if mem_percent > 0:
                all_metrics.append((now, "Azure", rg, site.name, 'app_memory', mem_percent, 100.0, mem_percent, 'percent'))
            
            # Status
            status = 1.0 if site.state == 'Running' else 0.0
            all_metrics.append((now, "Azure", rg, site.name, 'app_status', status, 1.0, status * 100, 'boolean'))
            
    except Exception as e:
        logging.error(f"⚠️  App Service Scan Failed: {e}")

    # --- 5. DATABASE INSERT ---
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