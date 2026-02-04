import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from google.cloud import monitoring_v3
from google.cloud import compute_v1
import time

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- 1. ENVIRONMENT SETUP ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(BASE_DIR, "gcp.env")

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

# GCP Config
GCP_PROJECT_ID = get_env_or_fail("GCP_PROJECT_ID")
GCP_CREDENTIALS_PATH = get_env_or_fail("GCP_CREDENTIALS_PATH", "gcp_credentials.json")

# Set GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS_PATH

# --- 2. GCP INSTANCE TYPE SPECS ---
# Maps GCP machine types to (vCPUs, RAM in GB)
# Source: https://cloud.google.com/compute/docs/machine-types

GCP_MACHINE_SPECS = {
    # E2 (Cost-optimized)
    "e2-micro": (2, 1), "e2-small": (2, 2), "e2-medium": (2, 4),
    "e2-standard-2": (2, 8), "e2-standard-4": (4, 16), "e2-standard-8": (8, 32),
    "e2-standard-16": (16, 64), "e2-standard-32": (32, 128),
    
    # N1 (First generation)
    "n1-standard-1": (1, 3.75), "n1-standard-2": (2, 7.5), "n1-standard-4": (4, 15),
    "n1-standard-8": (8, 30), "n1-standard-16": (16, 60), "n1-standard-32": (32, 120),
    "n1-highmem-2": (2, 13), "n1-highmem-4": (4, 26), "n1-highmem-8": (8, 52),
    "n1-highcpu-2": (2, 1.8), "n1-highcpu-4": (4, 3.6), "n1-highcpu-8": (8, 7.2),
    
    # N2 (Second generation)
    "n2-standard-2": (2, 8), "n2-standard-4": (4, 16), "n2-standard-8": (8, 32),
    "n2-standard-16": (16, 64), "n2-standard-32": (32, 128), "n2-standard-48": (48, 192),
    "n2-highmem-2": (2, 16), "n2-highmem-4": (4, 32), "n2-highmem-8": (8, 64),
    "n2-highcpu-2": (2, 2), "n2-highcpu-4": (4, 4), "n2-highcpu-8": (8, 8),
    
    # N2D (AMD)
    "n2d-standard-2": (2, 8), "n2d-standard-4": (4, 16), "n2d-standard-8": (8, 32),
    "n2d-standard-16": (16, 64), "n2d-standard-32": (32, 128),
    
    # C2 (Compute-optimized)
    "c2-standard-4": (4, 16), "c2-standard-8": (8, 32), "c2-standard-16": (16, 64),
    "c2-standard-30": (30, 120), "c2-standard-60": (60, 240),
    
    # M1 (Memory-optimized)
    "m1-ultramem-40": (40, 961), "m1-ultramem-80": (80, 1922), "m1-ultramem-160": (160, 3844),
    
    # M2 (Memory-optimized)
    "m2-ultramem-208": (208, 5888), "m2-ultramem-416": (416, 11776),
}

def get_machine_specs(machine_type):
    """
    Extracts (vCPUs, RAM_GB) from machine type.
    Handles both full URLs and short names.
    """
    # Extract machine type name from URL if needed
    if "/" in machine_type:
        machine_type = machine_type.split("/")[-1]
    
    if machine_type in GCP_MACHINE_SPECS:
        return GCP_MACHINE_SPECS[machine_type]
    
    # Try to parse custom machine types (e.g., custom-4-16384)
    if machine_type.startswith("custom-"):
        try:
            parts = machine_type.split("-")
            vcpus = int(parts[1])
            ram_mb = int(parts[2])
            ram_gb = ram_mb / 1024
            logging.info(f"Parsed custom machine type '{machine_type}': {vcpus} vCPUs, {ram_gb:.1f} GB RAM")
            return (vcpus, ram_gb)
        except:
            pass
    
    # Fallback estimation
    logging.warning(f"⚠️  Unknown machine type '{machine_type}'. Using defaults: 2 vCPUs, 8 GB RAM")
    return (2, 8)

# --- 3. HELPER FUNCTIONS ---

def get_gcp_metric(client, project_name, filter_str, minutes=5):
    """
    Fetches the latest metric value from GCP Cloud Monitoring.
    Returns a dict mapping resource_name → value
    """
    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10**9)
    
    interval = monitoring_v3.TimeInterval({
        "end_time": {"seconds": seconds, "nanos": nanos},
        "start_time": {"seconds": (seconds - minutes * 60), "nanos": nanos},
    })

    try:
        results = client.list_time_series(
            request={
                "name": project_name,
                "filter": filter_str,
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        )
        
        metrics = {}
        for result in results:
            # Extract resource identifier
            resource_labels = result.resource.labels
            resource_name = (
                resource_labels.get("instance_id") or 
                resource_labels.get("database_id") or 
                resource_labels.get("service_name") or 
                resource_labels.get("function_name") or 
                "Unknown"
            )
            
            # Get latest value
            if result.points:
                value = result.points[0].value.double_value
                metrics[resource_name] = value
        
        return metrics
        
    except Exception as e:
        logging.debug(f"Metric fetch failed for filter '{filter_str}': {e}")
        return {}

# --- 4. MAIN EXECUTION ---

def main():
    logging.info("🚀 Starting GCP Telemetry Sync...")
    all_metrics = []
    
    now = datetime.now(timezone.utc)
    project_name = f"projects/{GCP_PROJECT_ID}"
    
    try:
        monitoring_client = monitoring_v3.MetricServiceClient()
        compute_client = compute_v1.InstancesClient()
    except Exception as e:
        logging.critical(f"❌ GCP Auth Failed: {e}")
        logging.critical("   Check that GCP_CREDENTIALS_PATH points to valid service account JSON")
        sys.exit(1)
    
    # --- 1. COMPUTE ENGINE (VMs) ---
    logging.info("📡 Scanning Compute Engine instances...")
    
    try:
        # Get all instances across all zones
        aggregated_list = compute_client.aggregated_list(project=GCP_PROJECT_ID)
        
        instance_count = 0
        
        for zone, response in aggregated_list:
            if not hasattr(response, 'instances') or not response.instances:
                continue
            
            zone_name = zone.split("/")[-1]
            
            for instance in response.instances:
                # Only process running instances
                if instance.status != "RUNNING":
                    continue
                
                instance_count += 1
                instance_id = str(instance.id)
                instance_name = instance.name
                machine_type = instance.machine_type
                
                # Get machine specs
                vcpu_total, ram_total_gb = get_machine_specs(machine_type)
                
                # --- CPU ---
                cpu_metrics = get_gcp_metric(
                    monitoring_client, 
                    project_name,
                    f'metric.type="compute.googleapis.com/instance/cpu/utilization" AND resource.labels.instance_id="{instance_id}"'
                )
                
                cpu_utilization = cpu_metrics.get(instance_id, 0.0)
                cpu_percent = cpu_utilization * 100  # Convert 0.05 → 5%
                cpu_used_vcpu = (cpu_percent / 100.0) * vcpu_total
                
                all_metrics.append((
                    now, "GCP", zone_name, instance_name,
                    'cpu_usage',
                    cpu_used_vcpu,
                    vcpu_total,
                    cpu_percent,
                    'vCPU'
                ))
                
                # --- MEMORY ---
                # GCP provides memory utilization via Cloud Monitoring Agent
                mem_metrics = get_gcp_metric(
                    monitoring_client,
                    project_name,
                    f'metric.type="compute.googleapis.com/instance/memory/balloon/ram_used" AND resource.labels.instance_id="{instance_id}"'
                )
                
                if instance_id in mem_metrics:
                    # ram_used is in bytes
                    mem_used_bytes = mem_metrics[instance_id]
                    mem_used_gb = mem_used_bytes / (1024**3)
                    mem_percent = (mem_used_gb / ram_total_gb) * 100 if ram_total_gb > 0 else 0.0
                    
                    all_metrics.append((
                        now, "GCP", zone_name, instance_name,
                        'ram_usage',
                        mem_used_gb,
                        ram_total_gb,
                        mem_percent,
                        'GiB'
                    ))
                else:
                    logging.debug(f"{instance_name}: Memory metrics not available (Cloud Monitoring agent not installed)")
                
                # --- DISK ---
                disk_metrics = get_gcp_metric(
                    monitoring_client,
                    project_name,
                    f'metric.type="compute.googleapis.com/instance/disk/write_bytes_count" AND resource.labels.instance_id="{instance_id}"'
                )
                
                # Note: GCP doesn't provide disk usage % directly, only I/O metrics
                # Would need Cloud Monitoring agent for disk usage
                
                # --- NETWORK ---
                net_in_metrics = get_gcp_metric(
                    monitoring_client,
                    project_name,
                    f'metric.type="compute.googleapis.com/instance/network/received_bytes_count" AND resource.labels.instance_id="{instance_id}"'
                )
                
                net_out_metrics = get_gcp_metric(
                    monitoring_client,
                    project_name,
                    f'metric.type="compute.googleapis.com/instance/network/sent_bytes_count" AND resource.labels.instance_id="{instance_id}"'
                )
                
                # Network metrics are cumulative counters, we get rate over 5 minutes
                # Convert to Mbps: bytes/sec * 8 bits/byte / (1024*1024)
                if instance_id in net_in_metrics:
                    net_in_bytes_per_sec = net_in_metrics[instance_id] / 300  # 5 minutes = 300 seconds
                    net_in_mbps = (net_in_bytes_per_sec * 8) / (1024 * 1024)
                    all_metrics.append((now, "GCP", zone_name, instance_name, 'net_inbound', net_in_mbps, 0.0, 0.0, 'Mbps'))
                
                if instance_id in net_out_metrics:
                    net_out_bytes_per_sec = net_out_metrics[instance_id] / 300
                    net_out_mbps = (net_out_bytes_per_sec * 8) / (1024 * 1024)
                    all_metrics.append((now, "GCP", zone_name, instance_name, 'net_outbound', net_out_mbps, 0.0, 0.0, 'Mbps'))
                
                # --- STATUS ---
                status = 1.0 if instance.status == "RUNNING" else 0.0
                all_metrics.append((now, "GCP", zone_name, instance_name, 'instance_status', status, 1.0, status * 100, 'boolean'))
                
                logging.debug(
                    f"{instance_name} ({machine_type}): CPU={cpu_percent:.1f}%, "
                    f"vCPUs={vcpu_total}, RAM={ram_total_gb}GB"
                )
        
        if instance_count > 0:
            logging.info(f"   -> Found {instance_count} running instances")
        else:
            logging.info(f"   -> No running instances found")
            
    except Exception as e:
        logging.error(f"⚠️  Compute Engine scan failed: {e}")
    
    # --- 2. CLOUD SQL (Databases) ---
    logging.info("📡 Scanning Cloud SQL databases...")
    
    try:
        sql_cpu_metrics = get_gcp_metric(
            monitoring_client,
            project_name,
            'metric.type="cloudsql.googleapis.com/database/cpu/utilization"'
        )
        
        sql_mem_metrics = get_gcp_metric(
            monitoring_client,
            project_name,
            'metric.type="cloudsql.googleapis.com/database/memory/utilization"'
        )
        
        for db_id, cpu_util in sql_cpu_metrics.items():
            cpu_percent = cpu_util * 100
            all_metrics.append((now, "GCP", "CloudSQL", db_id, 'db_cpu', cpu_percent, 100.0, cpu_percent, 'percent'))
        
        for db_id, mem_util in sql_mem_metrics.items():
            mem_percent = mem_util * 100
            all_metrics.append((now, "GCP", "CloudSQL", db_id, 'db_memory', mem_percent, 100.0, mem_percent, 'percent'))
        
        if sql_cpu_metrics:
            logging.info(f"   -> Found {len(sql_cpu_metrics)} Cloud SQL instances")
            
    except Exception as e:
        logging.error(f"⚠️  Cloud SQL scan failed: {e}")
    
    # --- 3. CLOUD RUN (Serverless Containers) ---
    logging.info("📡 Scanning Cloud Run services...")
    
    try:
        run_cpu_metrics = get_gcp_metric(
            monitoring_client,
            project_name,
            'metric.type="run.googleapis.com/container/cpu/utilizations"'
        )
        
        run_mem_metrics = get_gcp_metric(
            monitoring_client,
            project_name,
            'metric.type="run.googleapis.com/container/memory/utilizations"'
        )
        
        for service_name, cpu_util in run_cpu_metrics.items():
            cpu_percent = cpu_util * 100
            all_metrics.append((now, "GCP", "CloudRun", service_name, 'app_cpu', cpu_percent, 100.0, cpu_percent, 'percent'))
        
        for service_name, mem_util in run_mem_metrics.items():
            mem_percent = mem_util * 100
            all_metrics.append((now, "GCP", "CloudRun", service_name, 'app_memory', mem_percent, 100.0, mem_percent, 'percent'))
        
        if run_cpu_metrics:
            logging.info(f"   -> Found {len(run_cpu_metrics)} Cloud Run services")
            
    except Exception as e:
        logging.error(f"⚠️  Cloud Run scan failed: {e}")
    
    # --- DATABASE INSERT ---
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
        logging.info("✅ Success: GCP Telemetry Synced.")
    except Exception as e:
        logging.error(f"❌ Database Error: {e}")

if __name__ == "__main__":
    main()