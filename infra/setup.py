"""
Simplified Distributed Debate System — SQS + SSM + ElastiCache
Stores configuration in AWS Systems Manager Parameter Store for Auto Scaling.

Queues:
  • research-tasks     → research agents
  • research-results   → aggregator
  • critic-tasks       → critic agents
  • judge-tasks        → judges
  • final-results      → notification service
"""

import boto3
import json
import time
import sys
from typing import Dict, Optional

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
REGION     = "us-east-1"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

QUEUES = {
    "research_tasks":   f"research-tasks-{ACCOUNT_ID[-6:]}.fifo",
    "research_results": f"research-results-{ACCOUNT_ID[-6:]}.fifo",
    "critic_tasks":     f"critic-tasks-{ACCOUNT_ID[-6:]}.fifo",
    "judge_tasks":      f"judge-tasks-{ACCOUNT_ID[-6:]}.fifo",
    "final_results":    f"final-results-{ACCOUNT_ID[-6:]}.fifo",
}

DLQS = {
    "research_tasks_dlq":   f"research-tasks-dlq-{ACCOUNT_ID[-6:]}.fifo",
    "research_results_dlq": f"research-results-dlq-{ACCOUNT_ID[-6:]}.fifo",
    "critic_tasks_dlq":     f"critic-tasks-dlq-{ACCOUNT_ID[-6:]}.fifo",
    "judge_tasks_dlq":      f"judge-tasks-dlq-{ACCOUNT_ID[-6:]}.fifo",
    "final_results_dlq":    f"final-results-dlq-{ACCOUNT_ID[-6:]}.fifo",
}

BUCKET = f"debate-results-{ACCOUNT_ID[-12:]}"

# ElastiCache cluster config
ELASTICACHE_CLUSTER_ID    = f"debate-redis-{ACCOUNT_ID[-6:]}"
ELASTICACHE_NODE_TYPE     = "cache.t3.micro"
ELASTICACHE_ENGINE        = "redis"
ELASTICACHE_ENGINE_VERSION = "7.0"
ELASTICACHE_NUM_REPLICAS  = 1   # 1 primary + 1 replica

# SSM Parameter paths
SSM_PATHS = {
    # Research agent
    "tasks_queue_url":          "/research-agent/tasks-queue-url",
    "results_queue_url":        "/research-agent/results-queue-url",
    "openai_api_key":           "/research-agent/openai-api-key",
    "config_json":              "/research-agent/config",
    # Critic agent
    "critic_tasks_queue_url":   "/critic-agent/tasks-queue-url",
    "critic_results_queue_url": "/critic-agent/results-queue-url",
    "critic_openai_api_key":    "/critic-agent/openai-api-key",
    "critic_config_json":       "/critic-agent/config",
    # Shared / aggregator
    "redis_host":               "/debate/redis-host",
    "redis_port":               "/debate/redis-port",
    "redis_auth_token":         "/debate/redis-auth-token",
}

# ─────────────────────────────────────────────────────────────
# CLIENTS
# ─────────────────────────────────────────────────────────────
sqs          = boto3.client("sqs",          region_name=REGION)
s3           = boto3.client("s3",           region_name=REGION)
ssm          = boto3.client("ssm",          region_name=REGION)
elasticache  = boto3.client("elasticache",  region_name=REGION)

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def log(msg):    print(f"→ {msg}")
def ok(msg):     print(f"✓ {msg}")
def exists(msg): print(f"≡ {msg} (already exists)")
def warn(msg):   print(f"⚠ {msg}")
def error(msg):  print(f"✗ {msg}")

def get_queue_url(queue_name: str) -> Optional[str]:
    try:
        return sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        return None

def create_fifo_queue(name: str, dlq_arn: Optional[str] = None) -> str:
    attrs = {
        "FifoQueue":                    "true",
        "ContentBasedDeduplication":    "true",
        "VisibilityTimeout":            "60",
        "MessageRetentionPeriod":       "345600",
    }
    if dlq_arn:
        attrs["RedrivePolicy"] = json.dumps({
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount":     "3"
        })
    return sqs.create_queue(QueueName=name, Attributes=attrs)["QueueUrl"]

def store_in_ssm(path: str, value: str, is_secure: bool = False):
    try:
        ssm.put_parameter(
            Name=path,
            Value=value,
            Type="SecureString" if is_secure else "String",
            Overwrite=True
        )
        ok(f"Stored in SSM: {path}")
    except Exception as e:
        error(f"Failed to store {path}: {e}")

# ─────────────────────────────────────────────────────────────
# QUEUE SETUP
# ─────────────────────────────────────────────────────────────
def setup_queues() -> Dict[str, str]:
    print("\n📦 Setting up SQS FIFO queues...")
    queue_urls = {}

    # DLQs first
    dlq_arns = {}
    for dlq_key, dlq_name in DLQS.items():
        existing_url = get_queue_url(dlq_name)
        if existing_url:
            exists(f"DLQ: {dlq_name}")
            arn = sqs.get_queue_attributes(
                QueueUrl=existing_url, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]
        else:
            log(f"Creating DLQ: {dlq_name}")
            url = create_fifo_queue(dlq_name)
            arn = sqs.get_queue_attributes(
                QueueUrl=url, AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]
            ok(f"Created DLQ: {dlq_name}")
        dlq_arns[dlq_name] = arn

    main_to_dlq = {
        "research_tasks":   "research_tasks_dlq",
        "research_results": "research_results_dlq",
        "critic_tasks":     "critic_tasks_dlq",
        "judge_tasks":      "judge_tasks_dlq",
        "final_results":    "final_results_dlq",
    }

    for main_name, queue_name in QUEUES.items():
        dlq_arn = dlq_arns[DLQS[main_to_dlq[main_name]]]
        existing_url = get_queue_url(queue_name)
        if existing_url:
            queue_urls[main_name] = existing_url
            exists(f"Queue: {queue_name}")
        else:
            log(f"Creating queue: {queue_name}")
            url = create_fifo_queue(queue_name, dlq_arn)
            queue_urls[main_name] = url
            ok(f"Created queue: {queue_name}")
        time.sleep(0.5)

    return queue_urls

# ─────────────────────────────────────────────────────────────
# S3 SETUP
# ─────────────────────────────────────────────────────────────
def setup_s3():
    print("\n🗄️  Setting up S3 bucket...")
    try:
        s3.head_bucket(Bucket=BUCKET)
        exists(f"Bucket: {BUCKET}")
        return
    except:
        pass

    log(f"Creating bucket: {BUCKET}")
    if REGION == "us-east-1":
        s3.create_bucket(Bucket=BUCKET)
    else:
        s3.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
    s3.put_public_access_block(
        Bucket=BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls":      True,
            "IgnorePublicAcls":     True,
            "BlockPublicPolicy":    True,
            "RestrictPublicBuckets": True,
        }
    )
    ok(f"Created bucket: {BUCKET}")

# ─────────────────────────────────────────────────────────────
# ELASTICACHE SETUP
# ─────────────────────────────────────────────────────────────
def setup_elasticache() -> Dict[str, str]:
    """
    Create an ElastiCache Redis replication group (primary + replica).
    Returns dict with host, port, and auth_token.
    Waits for the cluster to become available before returning.
    """
    print("\n🔴 Setting up ElastiCache Redis...")

    # Check if already exists
    try:
        resp = elasticache.describe_replication_groups(
            ReplicationGroupId=ELASTICACHE_CLUSTER_ID
        )
        group = resp["ReplicationGroups"][0]
        status = group["Status"]

        if status == "available":
            endpoint = group["NodeGroups"][0]["PrimaryEndpoint"]
            host     = endpoint["Address"]
            port     = str(endpoint["Port"])
            exists(f"ElastiCache cluster: {ELASTICACHE_CLUSTER_ID} ({host}:{port})")
            return {"host": host, "port": port}

        warn(f"ElastiCache cluster exists but status is '{status}' — waiting...")

    except elasticache.exceptions.ReplicationGroupNotFoundFault:
        # Doesn't exist yet — create it
        log(f"Creating ElastiCache Redis cluster: {ELASTICACHE_CLUSTER_ID}")
        log(f"  Node type:  {ELASTICACHE_NODE_TYPE}")
        log(f"  Engine:     {ELASTICACHE_ENGINE} {ELASTICACHE_ENGINE_VERSION}")
        log(f"  Replicas:   {ELASTICACHE_NUM_REPLICAS}")

        elasticache.create_replication_group(
            ReplicationGroupId=          ELASTICACHE_CLUSTER_ID,
            ReplicationGroupDescription= "Debate system shared Redis state",
            NumCacheClusters=            1 + ELASTICACHE_NUM_REPLICAS,
            CacheNodeType=               ELASTICACHE_NODE_TYPE,
            Engine=                      ELASTICACHE_ENGINE,
            EngineVersion=               ELASTICACHE_ENGINE_VERSION,
            AutomaticFailoverEnabled=    ELASTICACHE_NUM_REPLICAS > 0,
            MultiAZEnabled=              ELASTICACHE_NUM_REPLICAS > 0,
            AtRestEncryptionEnabled=     True,
            TransitEncryptionEnabled=    True,
            Tags=[
                {"Key": "Project", "Value": "Debatrium"},
                {"Key": "Role",    "Value": "shared-state"},
            ]
        )
        ok(f"ElastiCache cluster creation initiated: {ELASTICACHE_CLUSTER_ID}")

    # Poll until available (can take 5-10 min on first create)
    print(f"  Waiting for cluster to become available (this may take ~10 min)...")
    for attempt in range(60):
        time.sleep(15)
        try:
            resp  = elasticache.describe_replication_groups(
                ReplicationGroupId=ELASTICACHE_CLUSTER_ID
            )
            group  = resp["ReplicationGroups"][0]
            status = group["Status"]
            print(f"  [{attempt+1}/60] Status: {status}")

            if status == "available":
                endpoint = group["NodeGroups"][0]["PrimaryEndpoint"]
                host     = endpoint["Address"]
                port     = str(endpoint["Port"])
                ok(f"ElastiCache ready — {host}:{port}")
                return {"host": host, "port": port}

        except Exception as e:
            warn(f"Poll error: {e}")

    error("Timed out waiting for ElastiCache. Check AWS console.")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────
# SSM CONFIGURATION STORAGE
# ─────────────────────────────────────────────────────────────
def store_config_in_ssm(queue_urls: Dict[str, str], redis_info: Dict[str, str]):
    """Store all configuration in SSM Parameter Store."""
    print("\n📝 Storing configuration in SSM Parameter Store...")

    # ── Research agent ────────────────────────────────────────
    print("\n  [Research Agent]")
    store_in_ssm(SSM_PATHS["tasks_queue_url"],  queue_urls["research_tasks"])
    store_in_ssm(SSM_PATHS["results_queue_url"], queue_urls["research_results"])
    research_config = {
        "region": REGION, "account_id": ACCOUNT_ID,
        "queues": queue_urls, "bucket": BUCKET, "ssm_paths": SSM_PATHS,
    }
    store_in_ssm(SSM_PATHS["config_json"], json.dumps(research_config))

    # ── Critic agent ──────────────────────────────────────────
    print("\n  [Critic Agent]")
    store_in_ssm(SSM_PATHS["critic_tasks_queue_url"],   queue_urls["critic_tasks"])
    store_in_ssm(SSM_PATHS["critic_results_queue_url"], queue_urls["judge_tasks"])
    critic_config = {
        "region": REGION, "account_id": ACCOUNT_ID,
        "queues": queue_urls, "bucket": BUCKET, "ssm_paths": SSM_PATHS,
    }
    store_in_ssm(SSM_PATHS["critic_config_json"], json.dumps(critic_config))

    # ── Redis / ElastiCache ───────────────────────────────────
    print("\n  [Redis / ElastiCache]")
    store_in_ssm(SSM_PATHS["redis_host"], redis_info["host"])
    store_in_ssm(SSM_PATHS["redis_port"], redis_info["port"])

    # Ask for Redis auth token if transit encryption is enabled
    print("\n" + "="*60)
    print("Redis AUTH Token (set during ElastiCache creation)")
    print("="*60)
    redis_auth = input("Enter Redis AUTH token (or press Enter to skip): ").strip()
    if redis_auth:
        store_in_ssm(SSM_PATHS["redis_auth_token"], redis_auth, is_secure=True)
    else:
        warn("Skipping Redis AUTH token.")

    # ── OpenAI API key (shared by both agent types) ───────────
    print("\n" + "="*60)
    print("OpenAI API Key — used by Research & Critic Agents")
    print("="*60)
    openai_key = input("Enter your OpenAI API Key (or press Enter to skip): ").strip()
    if openai_key:
        store_in_ssm(SSM_PATHS["openai_api_key"],        openai_key, is_secure=True)
        store_in_ssm(SSM_PATHS["critic_openai_api_key"], openai_key, is_secure=True)
    else:
        warn("Skipping OpenAI API key. Set manually later:")
        print(f"  aws ssm put-parameter --name {SSM_PATHS['openai_api_key']} --value 'your-key' --type SecureString --overwrite")
        print(f"  aws ssm put-parameter --name {SSM_PATHS['critic_openai_api_key']} --value 'your-key' --type SecureString --overwrite")

# ─────────────────────────────────────────────────────────────
# SAVE LOCAL CONFIGURATION
# ─────────────────────────────────────────────────────────────
def save_local_config(queue_urls: Dict[str, str], redis_info: Dict[str, str]):
    config = {
        "region":     REGION,
        "account_id": ACCOUNT_ID,
        "queues":     queue_urls,
        "bucket":     BUCKET,
        "ssm_paths":  SSM_PATHS,
        "redis": {
            "host": redis_info["host"],
            "port": int(redis_info["port"]),
        }
    }
    with open("debate_config.json", "w") as f:
        json.dump(config, f, indent=2)
    ok("Saved debate_config.json")

# ─────────────────────────────────────────────────────────────
# TEST MESSAGE
# ─────────────────────────────────────────────────────────────
def send_test_message(queue_urls: Dict[str, str]):
    print("\n🧪 Sending test message...")
    test_message = {
        "debate_id":    "test-001",
        "round":        1,
        "angle":        "test_angle",
        "query":        "What is the meaning of life?",
        "instructions": "Research and provide answer",
        "timestamp":    time.time()
    }
    try:
        response = sqs.send_message(
            QueueUrl=queue_urls["research_tasks"],
            MessageBody=json.dumps(test_message),
            MessageGroupId="test-001",
            MessageDeduplicationId=f"test-001-{int(time.time())}"
        )
        ok(f"Test message sent! MessageId: {response['MessageId']}")
    except Exception as e:
        warn(f"Failed to send test message: {e}")

# ─────────────────────────────────────────────────────────────
# PRINT QUEUE INFO
# ─────────────────────────────────────────────────────────────
def print_queue_info(queue_urls: Dict[str, str]):
    print("\n📋 Queue Information:")
    print("-" * 60)
    for name, url in queue_urls.items():
        arn = sqs.get_queue_attributes(
            QueueUrl=url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        print(f"\n{name.upper()}:")
        print(f"  URL: {url}")
        print(f"  ARN: {arn}")
    print("\n" + "-" * 60)

# ─────────────────────────────────────────────────────────────
# AWS CREDENTIALS CHECK
# ─────────────────────────────────────────────────────────────
def check_aws_credentials():
    print("\n🔐 Checking AWS credentials...")
    try:
        identity = boto3.client("sts").get_caller_identity()
        ok(f"Using account: {identity['Account']}")
        ok(f"User/Role: {identity['Arn']}")
        return True
    except Exception as e:
        error(f"AWS credentials error: {e}")
        print("\nPlease configure credentials:")
        print("  Option 1: aws configure")
        print("  Option 2: Set environment variables")
        return False

# ─────────────────────────────────────────────────────────────
# VERIFY SSM ACCESS
# ─────────────────────────────────────────────────────────────
def verify_ssm_access():
    print("\n🔐 Checking SSM Parameter Store access...")
    try:
        ssm.describe_parameters(MaxResults=1)
        ok("SSM Parameter Store access verified")
        return True
    except Exception as e:
        warn(f"SSM access limited: {e}")
        print("Note: EC2 instances will need ssm:GetParameter permission")
        return False

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("  DISTRIBUTED DEBATE SYSTEM — SQS + SSM + ElastiCache")
    print("  Auto Scaling Ready!")
    print("="*60)

    if not check_aws_credentials():
        return

    verify_ssm_access()

    # Infrastructure
    queue_urls  = setup_queues()
    setup_s3()
    redis_info  = setup_elasticache()

    # Store config
    save_local_config(queue_urls, redis_info)
    store_config_in_ssm(queue_urls, redis_info)

    # Info + test
    print_queue_info(queue_urls)
    print("\n" + "="*60)
    send_test_message(queue_urls)

    print("\n" + "="*60)
    print("✅ SETUP COMPLETE!")
    print("="*60)
    print("\nConfiguration stored in:")
    print("  • Local:  debate_config.json")
    print("  • SSM:    /research-agent/*")
    print("  • SSM:    /critic-agent/*")
    print("  • SSM:    /debate/redis-*")
    print(f"\nElastiCache endpoint: {redis_info['host']}:{redis_info['port']}")
    print("\nTo deploy Auto Scaling groups:")
    print("  bash agents/research_agent/infra/create-launch-template.sh")
    print("  bash agents/critic_agent/infra/create-critic-launch-template.sh")
    print("\nLambda environment variables to set:")
    print(f"  REDIS_HOST            = {redis_info['host']}")
    print(f"  REDIS_PORT            = {redis_info['port']}")
    print(f"  CRITIC_TASKS_QUEUE_URL = <critic-tasks queue URL>")
    print(f"  EXPECTED_RESULTS      = 3")
    print(f"  NUM_CRITIC_SLOTS      = 3")
    print("="*60)

if __name__ == "__main__":
    main()