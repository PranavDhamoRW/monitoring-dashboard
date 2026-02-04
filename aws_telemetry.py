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
EC2_INSTANCE_SPECS = {
    # T3/T3a (Burstable)
    "t3.nano": (2, 0.5), "t3.micro": (2, 1), "t3.small": (2, 2), "t3.medium": (2, 4),
    "t3.large": (2, 8), "t3.xlarge": (4, 16), "t3.2xlarge": (8, 32),
    "t3a.nano": (2, 0.5), "t3a.micro": (2, 1), "t3a.small": (2, 2), "t3a.medium": (2, 4),
    "t3a.large": (2, 8), "t3a.xlarge": (4, 16), "t3a.2xlarge": (8, 32),
    
    # T2 (Burstable)
    "t2.nano": (1, 0.5), "t2.micro": (1, 1), "t2.small": (1, 2), "t2.medium": (2, 4),
    "t2.large": (2, 8), "t2.xlarge": (4, 16), "t2.2xlarge": (8, 32),
    
    # M5/M5a (General Purpose)
    "m5.large": (2, 8), "m5.xlarge": (4, 16), "m5.2xlarge": (8, 32), "m5.4xlarge": (16, 64),
    "m5.8xlarge": (32, 128), "m5.12xlarge": (48, 192), "m5.16xlarge": (64, 256),
    "m5a.large": (2, 8), "m5a.xlarge": (4, 16), "m5a.2xlarge": (8, 32), "m5a.4xlarge": (16, 64),
    
    # M6i/M6a (General Purpose)
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
    
    # Fallback estimation
    try:
        parts = instance_type.split('.')
        if len(parts) == 2:
            size = parts[1]
            if 'nano' in size: vcpu = 1
            elif 'micro' in size: vcpu = 1
            elif 'small' in size: vcpu = 2
            elif 'medium' in size: vcpu = 2
            elif 'large' in size and 'xlarge' not in size: vcpu = 2
            elif '24xlarge' in size: vcpu = 96
            elif '18xlarge' in size: vcpu = 72
            elif '16xlarge' in size: vcpu = 64
            elif '12xlarge' in size: vcpu = 48
            elif '9xlarge' in size: vcpu = 36
            elif '8xlarge' in size: vcpu = 32
            elif '4xlarge' in size: vcpu = 16
            elif '2xlarge' in size: vcpu = 8
            elif 'xlarge' in size: vcpu = 4
            else: vcpu = 2
            
            if parts[0].startswith('r'): ram = vcpu * 8
            elif parts[0].startswith('c'): ram = vcpu * 2
            else: ram = vcpu * 4
            
            logging.warning(f"⚠️  Instance type '{instance_type}' not in lookup. Estimated: {vcpu} vCPUs, {ram} GB RAM")
            return (vcpu, ram)
    except: pass
    
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
    """Fetches latest CloudWatch metric value."""
    try:
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=15)
        
        response = cw_client.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=dimensions,
            StartTime=start,
            EndTime=now,
            Period=300,
            Statistics=[stat]
        )
        
        datapoints = response.get('Datapoints', [])
        if datapoints:
            datapoints.sort(key=lambda x: x['Timestamp'], reverse=True)
            return datapoints[0].get(stat, 0.0)
    except Exception as e:
        logging.debug(f"Metric fetch failed for {metric_name}: {e}")
    
    return 0.0

def discover_all_regions():
    """Discovers all enabled AWS regions."""
    try:
        ec2 = boto3.client('ec2', region_name='us-east-1', config=aws_config)
        response = ec2.describe_regions(AllRegions=False)
        regions = [r['RegionName'] for r in response['Regions']]
        logging.info(f"🌍 Discovered {len(regions)} enabled regions")
        return regions
    except Exception as e:
        logging.warning(f"⚠️  Region discovery failed: {e}. Using defaults.")
        return ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-south-1']

# --- 4. SERVICE SCANNERS ---

def scan_ec2_instances(region, cw_client, all_metrics, now):
    """Scans EC2 instances in a region."""
    try:
        ec2 = boto3.client('ec2', region_name=region, config=aws_config)
        instances_response = ec2.describe_instances(
            Filters=[{'Name': 'instance-state-name', 'Values': ['running']}]
        )
        
        count = 0
        for reservation in instances_response.get('Reservations', []):
            for instance in reservation.get('Instances', []):
                count += 1
                instance_id = instance['InstanceId']
                instance_type = instance['InstanceType']
                instance_name = get_tag_value(instance.get('Tags', []), 'Name') or instance_id
                
                vcpu_total, ram_total_gb = get_instance_specs(instance_type)
                dimensions = [{'Name': 'InstanceId', 'Value': instance_id}]
                
                # CPU
                cpu_percent = get_cloudwatch_metric(cw_client, 'AWS/EC2', 'CPUUtilization', dimensions, 'Average')
                cpu_used_vcpu = (cpu_percent / 100.0) * vcpu_total
                all_metrics.append((now, "AWS", region, instance_name, 'cpu_usage', cpu_used_vcpu, vcpu_total, cpu_percent, 'vCPU'))
                
                # Memory (if CloudWatch Agent installed)
                mem_percent = get_cloudwatch_metric(cw_client, 'CWAgent', 'mem_used_percent', dimensions, 'Average')
                if mem_percent > 0:
                    mem_used_gb = (mem_percent / 100.0) * ram_total_gb
                    all_metrics.append((now, "AWS", region, instance_name, 'ram_usage', mem_used_gb, ram_total_gb, mem_percent, 'GiB'))
                
                # Network
                net_in_bytes = get_cloudwatch_metric(cw_client, 'AWS/EC2', 'NetworkIn', dimensions, 'Sum')
                net_out_bytes = get_cloudwatch_metric(cw_client, 'AWS/EC2', 'NetworkOut', dimensions, 'Sum')
                net_in_mbps = (net_in_bytes / 300 * 8) / (1024 * 1024) if net_in_bytes > 0 else 0.0
                net_out_mbps = (net_out_bytes / 300 * 8) / (1024 * 1024) if net_out_bytes > 0 else 0.0
                all_metrics.append((now, "AWS", region, instance_name, 'net_inbound', net_in_mbps, 0.0, 0.0, 'Mbps'))
                all_metrics.append((now, "AWS", region, instance_name, 'net_outbound', net_out_mbps, 0.0, 0.0, 'Mbps'))
                
                # Status
                state = instance.get('State', {}).get('Name', 'unknown')
                status = 1.0 if state == 'running' else 0.0
                all_metrics.append((now, "AWS", region, instance_name, 'instance_status', status, 1.0, status * 100, 'boolean'))
        
        if count > 0:
            logging.info(f"   ✅ EC2: {count} instances")
        return count
    except Exception as e:
        logging.debug(f"   ⚠️  EC2 scan failed: {e}")
        return 0

def scan_rds_instances(region, cw_client, all_metrics, now):
    """Scans RDS database instances."""
    try:
        rds = boto3.client('rds', region_name=region, config=aws_config)
        response = rds.describe_db_instances()
        
        count = 0
        for db in response.get('DBInstances', []):
            if db['DBInstanceStatus'] != 'available':
                continue
            
            count += 1
            db_id = db['DBInstanceIdentifier']
            dimensions = [{'Name': 'DBInstanceIdentifier', 'Value': db_id}]
            
            # CPU
            cpu_percent = get_cloudwatch_metric(cw_client, 'AWS/RDS', 'CPUUtilization', dimensions, 'Average')
            all_metrics.append((now, "AWS", region, db_id, 'db_cpu', cpu_percent, 100.0, cpu_percent, 'percent'))
            
            # Connections
            connections = get_cloudwatch_metric(cw_client, 'AWS/RDS', 'DatabaseConnections', dimensions, 'Average')
            all_metrics.append((now, "AWS", region, db_id, 'db_connections', connections, 0.0, 0.0, 'count'))
            
            # Storage (free space)
            free_storage = get_cloudwatch_metric(cw_client, 'AWS/RDS', 'FreeStorageSpace', dimensions, 'Average')
            free_storage_gb = free_storage / (1024**3)
            allocated_storage_gb = float(db.get('AllocatedStorage', 0))
            if allocated_storage_gb > 0:
                used_storage_gb = allocated_storage_gb - free_storage_gb
                storage_percent = (used_storage_gb / allocated_storage_gb) * 100
                all_metrics.append((now, "AWS", region, db_id, 'db_storage', used_storage_gb, allocated_storage_gb, storage_percent, 'GiB'))
        
        if count > 0:
            logging.info(f"   ✅ RDS: {count} databases")
        return count
    except Exception as e:
        logging.debug(f"   ⚠️  RDS scan failed: {e}")
        return 0

def scan_lambda_functions(region, cw_client, all_metrics, now):
    """Scans Lambda functions."""
    try:
        lambda_client = boto3.client('lambda', region_name=region, config=aws_config)
        response = lambda_client.list_functions()
        
        count = 0
        for func in response.get('Functions', []):
            count += 1
            func_name = func['FunctionName']
            dimensions = [{'Name': 'FunctionName', 'Value': func_name}]
            
            # Invocations
            invocations = get_cloudwatch_metric(cw_client, 'AWS/Lambda', 'Invocations', dimensions, 'Sum')
            all_metrics.append((now, "AWS", region, func_name, 'lambda_invocations', invocations, 0.0, 0.0, 'count'))
            
            # Errors
            errors = get_cloudwatch_metric(cw_client, 'AWS/Lambda', 'Errors', dimensions, 'Sum')
            all_metrics.append((now, "AWS", region, func_name, 'lambda_errors', errors, 0.0, 0.0, 'count'))
            
            # Duration (milliseconds)
            duration = get_cloudwatch_metric(cw_client, 'AWS/Lambda', 'Duration', dimensions, 'Average')
            all_metrics.append((now, "AWS", region, func_name, 'lambda_duration', duration, 0.0, 0.0, 'ms'))
        
        if count > 0:
            logging.info(f"   ✅ Lambda: {count} functions")
        return count
    except Exception as e:
        logging.debug(f"   ⚠️  Lambda scan failed: {e}")
        return 0

def scan_elastic_beanstalk(region, cw_client, all_metrics, now):
    """Scans Elastic Beanstalk environments."""
    try:
        eb = boto3.client('elasticbeanstalk', region_name=region, config=aws_config)
        response = eb.describe_environments()
        
        count = 0
        for env in response.get('Environments', []):
            if env['Status'] != 'Ready':
                continue
            
            count += 1
            env_name = env['EnvironmentName']
            dimensions = [{'Name': 'EnvironmentName', 'Value': env_name}]
            
            # Application health
            health = env.get('Health', 'Unknown')
            health_score = 1.0 if health in ['Green', 'Ok'] else 0.5 if health == 'Warning' else 0.0
            all_metrics.append((now, "AWS", region, env_name, 'eb_health', health_score, 1.0, health_score * 100, 'score'))
        
        if count > 0:
            logging.info(f"   ✅ ElasticBeanstalk: {count} environments")
        return count
    except Exception as e:
        logging.debug(f"   ⚠️  ElasticBeanstalk scan failed: {e}")
        return 0

def scan_ecs_clusters(region, cw_client, all_metrics, now):
    """Scans ECS (container service) clusters."""
    try:
        ecs = boto3.client('ecs', region_name=region, config=aws_config)
        clusters_response = ecs.list_clusters()
        
        count = 0
        for cluster_arn in clusters_response.get('clusterArns', []):
            cluster_name = cluster_arn.split('/')[-1]
            dimensions = [{'Name': 'ClusterName', 'Value': cluster_name}]
            
            # CPU Reservation
            cpu_reservation = get_cloudwatch_metric(cw_client, 'AWS/ECS', 'CPUReservation', dimensions, 'Average')
            all_metrics.append((now, "AWS", region, cluster_name, 'ecs_cpu_reservation', cpu_reservation, 100.0, cpu_reservation, 'percent'))
            
            # Memory Reservation
            mem_reservation = get_cloudwatch_metric(cw_client, 'AWS/ECS', 'MemoryReservation', dimensions, 'Average')
            all_metrics.append((now, "AWS", region, cluster_name, 'ecs_memory_reservation', mem_reservation, 100.0, mem_reservation, 'percent'))
            
            count += 1
        
        if count > 0:
            logging.info(f"   ✅ ECS: {count} clusters")
        return count
    except Exception as e:
        logging.debug(f"   ⚠️  ECS scan failed: {e}")
        return 0

def scan_elasticache(region, cw_client, all_metrics, now):
    """Scans ElastiCache (Redis/Memcached) clusters."""
    try:
        elasticache = boto3.client('elasticache', region_name=region, config=aws_config)
        response = elasticache.describe_cache_clusters()
        
        count = 0
        for cluster in response.get('CacheClusters', []):
            if cluster['CacheClusterStatus'] != 'available':
                continue
            
            count += 1
            cluster_id = cluster['CacheClusterId']
            dimensions = [{'Name': 'CacheClusterId', 'Value': cluster_id}]
            
            # CPU
            cpu_percent = get_cloudwatch_metric(cw_client, 'AWS/ElastiCache', 'CPUUtilization', dimensions, 'Average')
            all_metrics.append((now, "AWS", region, cluster_id, 'cache_cpu', cpu_percent, 100.0, cpu_percent, 'percent'))
            
            # Memory (for Redis)
            if cluster.get('Engine') == 'redis':
                mem_percent = get_cloudwatch_metric(cw_client, 'AWS/ElastiCache', 'DatabaseMemoryUsagePercentage', dimensions, 'Average')
                all_metrics.append((now, "AWS", region, cluster_id, 'cache_memory', mem_percent, 100.0, mem_percent, 'percent'))
        
        if count > 0:
            logging.info(f"   ✅ ElastiCache: {count} clusters")
        return count
    except Exception as e:
        logging.debug(f"   ⚠️  ElastiCache scan failed: {e}")
        return 0

def scan_load_balancers(region, cw_client, all_metrics, now):
    """Scans Application/Network Load Balancers."""
    try:
        elbv2 = boto3.client('elbv2', region_name=region, config=aws_config)
        response = elbv2.describe_load_balancers()
        
        count = 0
        for lb in response.get('LoadBalancers', []):
            if lb['State']['Code'] != 'active':
                continue
            
            count += 1
            lb_name = lb['LoadBalancerName']
            lb_arn = lb['LoadBalancerArn']
            lb_type = lb['Type']  # application, network
            
            # Extract LB name parts for dimensions
            lb_full_name = '/'.join(lb_arn.split(':')[-1].split('/')[1:])
            dimensions = [{'Name': 'LoadBalancer', 'Value': lb_full_name}]
            
            # Request count (ALB only)
            if lb_type == 'application':
                requests = get_cloudwatch_metric(cw_client, 'AWS/ApplicationELB', 'RequestCount', dimensions, 'Sum')
                all_metrics.append((now, "AWS", region, lb_name, 'alb_requests', requests, 0.0, 0.0, 'count'))
                
                # Target response time
                response_time = get_cloudwatch_metric(cw_client, 'AWS/ApplicationELB', 'TargetResponseTime', dimensions, 'Average')
                all_metrics.append((now, "AWS", region, lb_name, 'alb_response_time', response_time, 0.0, 0.0, 'seconds'))
        
        if count > 0:
            logging.info(f"   ✅ Load Balancers: {count} instances")
        return count
    except Exception as e:
        logging.debug(f"   ⚠️  Load Balancer scan failed: {e}")
        return 0

def scan_s3_buckets(region, cw_client, all_metrics, now):
    """Scans S3 buckets (only in us-east-1, since S3 is global)."""
    if region != 'us-east-1':
        return 0
    
    try:
        s3 = boto3.client('s3', config=aws_config)
        response = s3.list_buckets()
        
        count = 0
        for bucket in response.get('Buckets', []):
            count += 1
            bucket_name = bucket['Name']
            dimensions = [
                {'Name': 'BucketName', 'Value': bucket_name},
                {'Name': 'StorageType', 'Value': 'StandardStorage'}
            ]
            
            # Bucket size (in bytes)
            size_bytes = get_cloudwatch_metric(cw_client, 'AWS/S3', 'BucketSizeBytes', dimensions, 'Average')
            size_gb = size_bytes / (1024**3)
            all_metrics.append((now, "AWS", "global", bucket_name, 's3_size', size_gb, 0.0, 0.0, 'GiB'))
            
            # Object count
            object_count = get_cloudwatch_metric(cw_client, 'AWS/S3', 'NumberOfObjects', dimensions, 'Average')
            all_metrics.append((now, "AWS", "global", bucket_name, 's3_objects', object_count, 0.0, 0.0, 'count'))
        
        if count > 0:
            logging.info(f"   ✅ S3: {count} buckets (global)")
        return count
    except Exception as e:
        logging.debug(f"   ⚠️  S3 scan failed: {e}")
        return 0

def scan_dynamodb_tables(region, cw_client, all_metrics, now):
    """Scans DynamoDB tables."""
    try:
        dynamodb = boto3.client('dynamodb', region_name=region, config=aws_config)
        response = dynamodb.list_tables()
        
        count = 0
        for table_name in response.get('TableNames', []):
            count += 1
            dimensions = [{'Name': 'TableName', 'Value': table_name}]
            
            # Read capacity
            read_capacity = get_cloudwatch_metric(cw_client, 'AWS/DynamoDB', 'ConsumedReadCapacityUnits', dimensions, 'Sum')
            all_metrics.append((now, "AWS", region, table_name, 'dynamodb_read', read_capacity, 0.0, 0.0, 'units'))
            
            # Write capacity
            write_capacity = get_cloudwatch_metric(cw_client, 'AWS/DynamoDB', 'ConsumedWriteCapacityUnits', dimensions, 'Sum')
            all_metrics.append((now, "AWS", region, table_name, 'dynamodb_write', write_capacity, 0.0, 0.0, 'units'))
        
        if count > 0:
            logging.info(f"   ✅ DynamoDB: {count} tables")
        return count
    except Exception as e:
        logging.debug(f"   ⚠️  DynamoDB scan failed: {e}")
        return 0

# --- 5. MAIN EXECUTION ---

def main():
    logging.info("🚀 Starting AWS DRAGNET Telemetry Sync...")
    logging.info("🔍 Scanning ALL AWS services across ALL regions...")
    all_metrics = []
    now = datetime.now(timezone.utc)
    
    # Discover all regions
    regions = discover_all_regions()
    
    total_resources = 0
    
    for region in regions:
        logging.info(f"\n📍 Region: {region}")
        
        try:
            cw_client = boto3.client('cloudwatch', region_name=region, config=aws_config)
            
            # Scan all services
            total_resources += scan_ec2_instances(region, cw_client, all_metrics, now)
            total_resources += scan_rds_instances(region, cw_client, all_metrics, now)
            total_resources += scan_lambda_functions(region, cw_client, all_metrics, now)
            total_resources += scan_elastic_beanstalk(region, cw_client, all_metrics, now)
            total_resources += scan_ecs_clusters(region, cw_client, all_metrics, now)
            total_resources += scan_elasticache(region, cw_client, all_metrics, now)
            total_resources += scan_load_balancers(region, cw_client, all_metrics, now)
            total_resources += scan_dynamodb_tables(region, cw_client, all_metrics, now)
            total_resources += scan_s3_buckets(region, cw_client, all_metrics, now)
            
        except Exception as e:
            logging.error(f"⚠️  Error scanning {region}: {e}")
    
    # --- DATABASE INSERT ---
    if not all_metrics:
        logging.info(f"\n📊 Total resources scanned: {total_resources}")
        logging.info("ℹ️  No metrics collected. Exiting.")
        return
    
    try:
        logging.info(f"\n📊 Total resources scanned: {total_resources}")
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
        logging.info("✅ Success: AWS DRAGNET Telemetry Synced.")
    except Exception as e:
        logging.error(f"❌ Database Error: {e}")

if __name__ == "__main__":
    main()