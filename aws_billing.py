import os
import sys
import logging
import psycopg2
import boto3
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone, date
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
USD_TO_INR_RATE = 90.97
MIN_COST_THRESHOLD = 0.01  # Skip costs below 1 cent

# Fast timeout config
aws_config = Config(
    connect_timeout=10,
    read_timeout=30,
    retries={'max_attempts': 2}
)

# --- 2. HELPER FUNCTIONS ---

def parse_resource_name(resource_id, service):
    """
    Extracts a clean resource name from AWS Resource ID.
    
    Examples:
    - arn:aws:ec2:us-east-1:123456:instance/i-abc123 → i-abc123
    - arn:aws:rds:us-east-1:123456:db:mydb → mydb
    - arn:aws:s3:::my-bucket → my-bucket
    """
    if not resource_id:
        return "Untagged"
    
    # Handle ARNs
    if resource_id.startswith("arn:"):
        parts = resource_id.split(":")
        if len(parts) >= 6:
            # Last part after the resource type
            resource_part = parts[-1]
            if "/" in resource_part:
                return resource_part.split("/")[-1]
            return resource_part
        return resource_id.split(":")[-1]
    
    # Handle plain resource IDs
    if "/" in resource_id:
        return resource_id.split("/")[-1]
    
    return resource_id

def get_tag_value(tags, key):
    """Extracts value from AWS tag list."""
    if not tags:
        return None
    for tag in tags:
        if tag.get("Key") == key:
            return tag.get("Value")
    return None

# --- 3. FETCH LOGIC ---

def fetch_daily_costs():
    """
    Fetches last 30 days of costs from AWS Cost Explorer.
    Groups by Service and Resource for detailed breakdown.
    """
    logging.info("🚀 Starting AWS Billing Sync...")
    
    try:
        ce = boto3.client('ce', region_name='us-east-1', config=aws_config)
    except Exception as e:
        logging.critical(f"❌ AWS Auth Failed: {e}")
        sys.exit(1)

    # Date Range: Last 30 days
    now = datetime.now(timezone.utc)
    end_date = now.date()
    start_date = end_date - timedelta(days=30)
    
    try:
        logging.info("⏳ Querying AWS Cost Explorer (this can take 10-20 seconds)...")
        
        # Query with grouping by Service and Resource
        response = ce.get_cost_and_usage(
            TimePeriod={
                'Start': str(start_date),
                'End': str(end_date)
            },
            Granularity='DAILY',
            Metrics=['UnblendedCost'],
            GroupBy=[
                {'Type': 'DIMENSION', 'Key': 'SERVICE'},
                {'Type': 'DIMENSION', 'Key': 'RESOURCE_ID'}
            ]
        )
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDeniedException':
            logging.error("❌ Access Denied: Cost Explorer requires billing permissions")
            logging.error("   Add 'ce:GetCostAndUsage' permission to your IAM user/role")
        else:
            logging.error(f"❌ AWS API Error: {e}")
        return []
    except Exception as e:
        logging.error(f"❌ Query Failed: {e}")
        return []

    # Parse response
    records = []
    skipped_count = 0
    
    for time_period in response.get('ResultsByTime', []):
        period_start = time_period['TimePeriod']['Start']
        period_end = time_period['TimePeriod']['End']
        
        for group in time_period.get('Groups', []):
            try:
                # Extract service and resource ID
                keys = group.get('Keys', [])
                if len(keys) < 2:
                    continue
                
                service = keys[0]
                resource_id = keys[1]
                
                # Extract cost
                cost_usd = float(group['Metrics']['UnblendedCost']['Amount'])
                
                # Skip negligible costs
                if cost_usd < MIN_COST_THRESHOLD:
                    skipped_count += 1
                    continue
                
                # Parse resource name
                resource_name = parse_resource_name(resource_id, service)
                
                # Map service to resource type
                resource_type = service.replace('Amazon ', '').replace('AWS ', '')
                
                # Convert to INR
                cost_inr = cost_usd * USD_TO_INR_RATE
                
                records.append((
                    "AWS",
                    "Production",      # Project name (can be enhanced with tags)
                    resource_name,
                    resource_type,
                    round(cost_inr, 2),
                    "INR",
                    period_start,
                    period_end
                ))
                
            except Exception as e:
                logging.warning(f"⚠️  Skipping malformed entry: {e}")
                continue
    
    logging.info(f" -> Processed {len(records)} cost records.")
    logging.info(f" -> 🗑️ Skipped {skipped_count} micro-cost entries (< ${MIN_COST_THRESHOLD}).")
    
    return records

# --- 4. ENHANCED VERSION: WITH EC2 NAME TAGS ---

def enrich_with_ec2_names(records):
    """
    Optional: Fetches EC2 instance names from tags to replace instance IDs.
    This makes the dashboard more readable.
    """
    logging.info("🔍 Enriching EC2 instance names from tags...")
    
    try:
        # Get all regions with EC2 instances
        ec2_us = boto3.client('ec2', region_name='us-east-1', config=aws_config)
        
        # Build instance ID → Name mapping
        name_map = {}
        
        try:
            instances = ec2_us.describe_instances()
            for reservation in instances.get('Reservations', []):
                for instance in reservation.get('Instances', []):
                    instance_id = instance['InstanceId']
                    name = get_tag_value(instance.get('Tags', []), 'Name')
                    if name:
                        name_map[instance_id] = name
        except:
            pass  # If EC2 access fails, skip enrichment
        
        # Apply mappings
        if name_map:
            enriched = []
            for record in records:
                provider, project, resource_name, res_type, amount, currency, start, end = record
                
                # If resource is an instance ID and we have a name, use it
                if resource_name in name_map:
                    resource_name = name_map[resource_name]
                
                enriched.append((provider, project, resource_name, res_type, amount, currency, start, end))
            
            logging.info(f" -> Enriched {len(name_map)} EC2 instance names.")
            return enriched
        
    except Exception as e:
        logging.debug(f"Name enrichment failed: {e}")
    
    return records

# --- 5. DATABASE UPDATE ---

def main():
    records = fetch_daily_costs()
    
    if not records:
        logging.info("ℹ️  No valid records to insert.")
        return
    
    # Optional: Enrich EC2 names
    records = enrich_with_ec2_names(records)
    
    # Find earliest date to clean DB
    records.sort(key=lambda x: x[6])
    earliest_date = records[0][6]

    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                # 1. Clean
                logging.info(f"🧹 Clearing AWS data since {earliest_date}...")
                cur.execute(
                    "DELETE FROM billing_metrics WHERE provider = 'AWS' AND period_start >= %s",
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
                
        logging.info("✅ Success: AWS Billing Synced.")

    except Exception as e:
        logging.error(f"❌ Database Transaction Failed: {e}")

if __name__ == "__main__":
    main()