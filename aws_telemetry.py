import os
import sys
import logging
import psycopg2
import boto3
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from botocore.config import Config
from botocore.exceptions import ClientError

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- 1. ENVIRONMENT SETUP ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(BASE_DIR, "aws.env")

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

# AWS Config
AWS_ACCESS_KEY = get_env_or_fail("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = get_env_or_fail("AWS_SECRET_ACCESS_KEY")

# Fast timeout config
aws_config = Config(
    connect_timeout=10,
    read_timeout=30,
    retries={'max_attempts': 2}
)

# --- 2. EC2 INSTANCE TYPE SPECS ---
# Maps EC2 instance types to (vCPUs, RAM in GB)
# Source: https://aws.amazon.com/ec2/instance-types/

EC2_INSTANCE_SPECS = {
    # T3/T3a (Burstable)
    "t3.nano": (2, 0.5), "t3.micro": (2, 1), "t3.small": (2, 2), "t3.medium": (2, 4),
    "t3.large": (2, 8), "t3.xlarge": (4, 16), "t3.2xlarge": (8, 32),
    "t3a.nano": (2, 0.5), "t3a.micro": (2, 1), "t3a.small": (2, 2), "t3a.medium": (2, 4),
    "t3a.large": (2, 8), "t3a.xlarge": (4, 16), "t3a.2xlarge": (8, 32),
    
    # T2 (Burstable - older generation)
    "t2.nano": (1, 0.5), "t2.micro": (1, 1), "t2.small": (1, 2), "t2.medium": (2, 4),
    "t2.large": (2, 8), "t2.xlarge": (4, 16), "t2.2xlarge": (8, 32),
    
    # M5/M5a (General Purpose)
    "m5.large": (2, 8), "m5.xlarge": (4, 16), "m5.2xlarge": (8, 32), "m5.4xlarge": (16, 64),
    "m5.8xlarge": (32, 128), "m5.12xlarge": (48, 192), "m5.16xlarge": (64, 256),
    "m5a.large": (2, 8), "m5a.xlarge": (4, 16), "m5a.2xlarge": (8, 32), "m5a.4xlarge": (16, 64),
    
    # M6i/M6a (General Purpose - newer)
    "m6i.large": (2, 8), "m6i.xlarge": (4, 16), "m6i.2xlarge": (8, 32), "m6i.4xlarge": (16, 64),
    "m6a.large": (2, 8), "m6a.xlarge": (4, 16), "m6a.2xlarge": (8, 32),
    
    # C5/C5a (Compute Optimized)
    "c5.large": (2, 4), "c5.xlarge": (4, 8), "c5.2xlarge": (8, 16), "c5.4xlarge": (16, 32),
    "c5.9xlarge": (36, 72), "c5.12xlarge": (48, 96), "c5.18xlarge": (72, 144),
    "c5a.large": (2, 4), "c5a.xlarge": (4, 8), "c5a.2xlarge": (8, 16),
    
    # R5/R5a (Memory Optimized)
    "r5.large": (2, 16), "r5.xlarge": (4, 32), "r5.2xlarge": (8, 64), "r5.4xlarge": (16, 128),
    "r5.8xlarge": (32, 256), "r5.12xlarge": (48, 384), "r5.16xlarge": (64, 512),
    "r5a.large": (2, 16), "r5a.xlarge": (4, 32), "r5a.2xlarge": (8, 64),
}

def get_instance_specs(instance_type):
    """Returns (vCPUs, RAM_GB) for an instance type."""
    if instance_type in EC2_INSTANCE_SPECS:
        return EC2_INSTANCE_SPECS[instance_type]
    
    # Fallback: Try to parse from name (e.g., m5.2xlarge → 8 vCPUs)
    try:
        parts = instance_type.split('.')
        if len(parts) == 2:
            size = parts[1]
            
            # Parse multiplier
            if 'nano' in size:
                vcpu = 1
            elif 'micro' in size:
                vcpu = 1
            elif 'small' in size:
                vcpu = 2
            elif 'medium' in size:
                vcpu = 2
            elif 'large' in size and 'xlarge' not in size:
                vcpu = 2
            elif '24xlarge' in size:
                vcpu = 96
            elif '18xlarge' in size:
                vcpu = 72
            elif '16xlarge' in size:
                vcpu = 64
            elif '12xlarge' in size:
                vcpu = 48
            elif '9xlarge' in size:
                vcpu = 36
            elif '8xlarge' in size:
                vcpu = 32
            elif '4xlarge' in size:
                vcpu = 16
            elif '2xlarge' in size:
                vcpu = 8
            elif 'xlarge' in size:
                vcpu = 4
            else:
                vcpu = 2
            
            # Estimate RAM based on family
            if parts[0].startswith('r'):  # Memory optimized
                ram = vcpu * 8
            elif parts[0].startswith('c'):  # Compute optimized
                ram = vcpu * 2
            else:  # General purpose
                ram = vcpu * 4
            
            logging.warning(f"⚠️  Instance type '{instance_type}' not in lookup. Estimated: {vcpu} vCPUs, {ram} GB RAM")
            return (vcpu, ram)
    except:
        pass
    
    # Last resort
    logging.warning(f"⚠️  Could not determine specs for '{instance_type}'. Using defaults: 2 vCPUs, 4 GB RAM")
    return (2, 4)

# --- 3. HELPER FUNCTIONS ---

def get_tag_value(tags, key):
    """Extracts value from tag list."""
    if not tags:
        return None
    for tag in tags:
        if tag.get('Key') == key:
            return tag.get('Value')
    return None

def get_cloudwatch_metric(cw_client, namespace, metric_name, dimensions, stat='Average'):
    """
    Fetches latest CloudWatch metric value.
    
    Args:
        namespace: AWS/EC2, AWS/RDS, etc.
        metric_name: CPUUtilization, NetworkIn, etc.
        dimensions: [{'Name': 'InstanceId', 'Value': 'i-123'}]
        stat: Average, Maximum, Sum
    """
    try:
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=15)
        
        response = cw_client.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=start,
            EndTime=now,
            Period=300,  # 5-minute intervals
            Statistics=[stat]
        )
        
        datapoints = response.get('Datapoints', [])
        if datapoints:
            # Sort by timestamp and get latest
            datapoints.sort(key=lambda x: x['Timestamp'], reverse=True)
            return datapoints[0].get(stat, 0.0)
        
    except Exception as e:
        logging.debug(f"Metric fetch failed for {metric_name}: {e}")
    
    return 0.0

# --- 4. MAIN EXECUTION ---

def main():
    logging.info("🚀 Starting AWS Telemetry Sync...")
    all_metrics = []
    
    now = datetime.now(timezone.utc)
    
    # Regions to scan (from your test: us-east-1 has instances, ap-south-1 is empty)
    regions = ['us-east-1', 'ap-south-1']
    
    for region in regions:
        logging.info(f"📡 Scanning EC2 instances in {region}...")
        
        try:
            ec2 = boto3.client('ec2', region_name=region, config=aws_config)
            cw = boto3.client('cloudwatch', region_name=region, config=aws_config)
            
            # Fetch all running instances
            instances_response = ec2.describe_instances(
                Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
            )
            
            instance_count = 0
            
            for reservation in instances_response.get('Reservations', []):
                for instance in reservation.get('Instances', []):
                    instance_count += 1
                    
                    instance_id = instance['InstanceId']
                    instance_type = instance['InstanceType']
                    instance_name = get_tag_value(instance.get('Tags', []), 'Name') or instance_id
                    
                    # Get instance specs
                    vcpu_total, ram_total_gb = get_instance_specs(instance_type)
                    
                    # Dimensions for CloudWatch queries
                    dimensions = [{'Name': 'InstanceId', 'Value': instance_id}]
                    
                    # --- CPU ---
                    cpu_percent = get_cloudwatch_metric(cw, 'AWS/EC2', 'CPUUtilization', dimensions, 'Average')
                    cpu_used_vcpu = (cpu_percent / 100.0) * vcpu_total
                    
                    all_metrics.append((
                        now, "AWS", region, instance_name,
                        'cpu_usage',
                        cpu_used_vcpu,
                        vcpu_total,
                        cpu_percent,
                        'vCPU'
                    ))
                    
                    # --- MEMORY ---
                    # Note: Default CloudWatch doesn't provide memory metrics!
                    # You need CloudWatch Agent installed for this.
                    # We'll try to fetch it, but it will likely be 0.0
                    mem_percent = get_cloudwatch_metric(cw, 'CWAgent', 'mem_used_percent', dimensions, 'Average')
                    
                    if mem_percent > 0:
                        mem_used_gb = (mem_percent / 100.0) * ram_total_gb
                        all_metrics.append((
                            now, "AWS", region, instance_name,
                            'ram_usage',
                            mem_used_gb,
                            ram_total_gb,
                            mem_percent,
                            'GiB'
                        ))
                    else:
                        # If CloudWatch Agent not installed, we can't get memory
                        logging.debug(f"{instance_name}: Memory metrics not available (CloudWatch Agent not installed)")
                    
                    # --- DISK ---
                    # Similar to memory, disk metrics require CloudWatch Agent
                    disk_percent = get_cloudwatch_metric(cw, 'CWAgent', 'disk_used_percent', dimensions, 'Average')
                    
                    if disk_percent > 0:
                        all_metrics.append((
                            now, "AWS", region, instance_name,
                            'disk_usage',
                            0.0,  # Don't have total disk size easily
                            0.0,
                            disk_percent,
                            'percent'
                        ))
                    
                    # --- NETWORK ---
                    # Network metrics ARE available by default
                    net_in_bytes = get_cloudwatch_metric(cw, 'AWS/EC2', 'NetworkIn', dimensions, 'Sum')
                    net_out_bytes = get_cloudwatch_metric(cw, 'AWS/EC2', 'NetworkOut', dimensions, 'Sum')
                    
                    # Convert bytes over 5 minutes to Mbps
                    # bytes / 300 seconds * 8 bits / 1024^2 = Mbps
                    net_in_mbps = (net_in_bytes / 300 * 8) / (1024 * 1024) if net_in_bytes > 0 else 0.0
                    net_out_mbps = (net_out_bytes / 300 * 8) / (1024 * 1024) if net_out_bytes > 0 else 0.0
                    
                    all_metrics.append((now, "AWS", region, instance_name, 'net_inbound', net_in_mbps, 0.0, 0.0, 'Mbps'))
                    all_metrics.append((now, "AWS", region, instance_name, 'net_outbound', net_out_mbps, 0.0, 0.0, 'Mbps'))
                    
                    # --- STATUS ---
                    state = instance.get('State', {}).get('Name', 'unknown')
                    status = 1.0 if state == 'running' else 0.0
                    all_metrics.append((now, "AWS", region, instance_name, 'instance_status', status, 1.0, status * 100, 'boolean'))
                    
                    logging.debug(
                        f"{instance_name} ({instance_type}): CPU={cpu_percent:.1f}%, "
                        f"Net In={net_in_mbps:.2f} Mbps, Net Out={net_out_mbps:.2f} Mbps"
                    )
            
            if instance_count > 0:
                logging.info(f"   -> Found {instance_count} running instances in {region}")
            else:
                logging.info(f"   -> No running instances in {region}")
                
        except Exception as e:
            logging.error(f"⚠️  Error scanning {region}: {e}")
    
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
        logging.info("✅ Success: AWS Telemetry Synced.")
    except Exception as e:
        logging.error(f"❌ Database Error: {e}")

if __name__ == "__main__":
    main()