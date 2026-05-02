
"""
Simplified Distributed Debate System — SQS Only with SSM Support
Now stores configuration in AWS Systems Manager Parameter Store for Auto Scaling

#   • research-tasks     → research agents
#   • research-results   → aggregator  
#   • critic-tasks       → critic agents
#   • judge-tasks        → judges
#   • final-results      → notification service
"""

import boto3
import json
import time
from typing import Dict, Optional

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
REGION = "us-east-1"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

# SQS Queues (all FIFO - no SNS, no KMS)
QUEUES = {
    "research_tasks": f"research-tasks-{ACCOUNT_ID[-6:]}.fifo",
    "research_results": f"research-results-{ACCOUNT_ID[-6:]}.fifo",
    "critic_tasks": f"critic-tasks-{ACCOUNT_ID[-6:]}.fifo",
    "judge_tasks": f"judge-tasks-{ACCOUNT_ID[-6:]}.fifo",
    "final_results": f"final-results-{ACCOUNT_ID[-6:]}.fifo",
}

# DLQs for each main queue
DLQS = {
    "research_tasks_dlq": f"research-tasks-dlq-{ACCOUNT_ID[-6:]}.fifo",
    "research_results_dlq": f"research-results-dlq-{ACCOUNT_ID[-6:]}.fifo",
    "critic_tasks_dlq": f"critic-tasks-dlq-{ACCOUNT_ID[-6:]}.fifo",
    "judge_tasks_dlq": f"judge-tasks-dlq-{ACCOUNT_ID[-6:]}.fifo",
    "final_results_dlq": f"final-results-dlq-{ACCOUNT_ID[-6:]}.fifo",
}

# S3 bucket for final results
BUCKET = f"debate-results-{ACCOUNT_ID[-12:]}"

# SSM Parameter paths
SSM_PATHS = {
    "tasks_queue_url": "/research-agent/tasks-queue-url",
    "results_queue_url": "/research-agent/results-queue-url",
    "openai_api_key": "/research-agent/openai-api-key",
    "config_json": "/research-agent/config",
}

# ─────────────────────────────────────────────────────────────
# CLIENTS
# ─────────────────────────────────────────────────────────────
sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def log(msg): print(f"→ {msg}")
def ok(msg): print(f"✓ {msg}")
def exists(msg): print(f"≡ {msg} (already exists)")
def warn(msg): print(f"⚠ {msg}")

def get_queue_url(queue_name: str) -> Optional[str]:
    """Get queue URL or None if doesn't exist"""
    try:
        return sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        return None

def create_fifo_queue(name: str, dlq_arn: Optional[str] = None) -> str:
    """Create FIFO queue with optional DLQ"""
    attrs = {
        "FifoQueue": "true",
        "ContentBasedDeduplication": "true",
        "VisibilityTimeout": "60",
        "MessageRetentionPeriod": "345600",
    }
    
    if dlq_arn:
        attrs["RedrivePolicy"] = json.dumps({
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount": "3"
        })
    
    response = sqs.create_queue(QueueName=name, Attributes=attrs)
    return response["QueueUrl"]

def store_in_ssm(path: str, value: str, is_secure: bool = False):
    """Store configuration in SSM Parameter Store"""
    try:
        param_type = "SecureString" if is_secure else "String"
        ssm.put_parameter(
            Name=path,
            Value=value,
            Type=param_type,
            Overwrite=True
        )
        ok(f"Stored in SSM: {path}")
    except Exception as e:
        error(f"Failed to store {path}: {e}")

# ─────────────────────────────────────────────────────────────
# QUEUE SETUP
# ─────────────────────────────────────────────────────────────
def setup_queues() -> Dict[str, str]:
    """Create all queues and return URLs"""
    print("\n📦 Setting up SQS FIFO queues...")
    queue_urls = {}
    
    # First, check/create DLQs
    dlq_arns = {}
    for dlq_name in DLQS.values():
        existing_url = get_queue_url(dlq_name)
        if existing_url:
            exists(f"DLQ: {dlq_name}")
            arn = sqs.get_queue_attributes(
                QueueUrl=existing_url, 
                AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]
            dlq_arns[dlq_name] = arn
        else:
            log(f"Creating DLQ: {dlq_name}")
            url = create_fifo_queue(dlq_name)
            arn = sqs.get_queue_attributes(
                QueueUrl=url, 
                AttributeNames=["QueueArn"]
            )["Attributes"]["QueueArn"]
            dlq_arns[dlq_name] = arn
            ok(f"Created DLQ: {dlq_name}")
    
    # Create main queues with DLQ mapping
    main_to_dlq = {
        "research_tasks": "research_tasks_dlq",
        "research_results": "research_results_dlq", 
        "critic_tasks": "critic_tasks_dlq",
        "judge_tasks": "judge_tasks_dlq",
        "final_results": "final_results_dlq",
    }
    
    for main_name, queue_name in QUEUES.items():
        dlq_key = main_to_dlq[main_name]
        dlq_arn = dlq_arns[DLQS[dlq_key]]
        
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
    """Create S3 bucket for final results"""
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
            CreateBucketConfiguration={"LocationConstraint": REGION},
        )
    
    s3.put_public_access_block(
        Bucket=BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )
    
    ok(f"Created bucket: {BUCKET}")

# ─────────────────────────────────────────────────────────────
# SSM CONFIGURATION STORAGE
# ─────────────────────────────────────────────────────────────
def store_config_in_ssm(queue_urls: Dict[str, str]):
    """Store configuration in SSM Parameter Store for Auto Scaling"""
    print("\n📝 Storing configuration in SSM Parameter Store...")
    
    # Store queue URLs individually
    store_in_ssm(SSM_PATHS["tasks_queue_url"], queue_urls["research_tasks"])
    store_in_ssm(SSM_PATHS["results_queue_url"], queue_urls["research_results"])
    
    # Store full config as JSON
    full_config = {
        "region": REGION,
        "account_id": ACCOUNT_ID,
        "queues": queue_urls,
        "bucket": BUCKET,
        "ssm_paths": SSM_PATHS,
    }
    store_in_ssm(SSM_PATHS["config_json"], json.dumps(full_config))
    
    # Ask for OpenAI API key
    print("\n" + "="*60)
    print("OpenAI API Key Required for Research Agents")
    print("="*60)
    openai_key = input("Enter your OpenAI API Key (or press Enter to skip): ").strip()
    
    if openai_key:
        store_in_ssm(SSM_PATHS["openai_api_key"], openai_key, is_secure=True)
    else:
        warn("Skipping OpenAI API key. You'll need to set it manually in SSM later.")
        print(f"Run: aws ssm put-parameter --name {SSM_PATHS['openai_api_key']} --value 'your-key' --type SecureString --overwrite")

# ─────────────────────────────────────────────────────────────
# SAVE LOCAL CONFIGURATION
# ─────────────────────────────────────────────────────────────
def save_local_config(queue_urls: Dict[str, str]):
    """Save configuration locally for development"""
    config = {
        "region": REGION,
        "account_id": ACCOUNT_ID,
        "queues": queue_urls,
        "bucket": BUCKET,
        "ssm_paths": SSM_PATHS,
        "redis": {
            "host": "localhost",
            "port": 6379,
        }
    }
    
    with open("debate_config.json", "w") as f:
        json.dump(config, f, indent=2)
    
    ok("Saved debate_config.json")

# ─────────────────────────────────────────────────────────────
# TEST MESSAGE
# ─────────────────────────────────────────────────────────────
def send_test_message(queue_urls: Dict[str, str]):
    """Send a test message through the system"""
    print("\n🧪 Sending test message...")
    
    test_message = {
        "debate_id": "test-001",
        "round": 1,
        "angle": "test_angle",
        "query": "What is the meaning of life?",
        "instructions": "Research and provide answer",
        "timestamp": time.time()
    }
    
    try:
        response = sqs.send_message(
            QueueUrl=queue_urls["research_tasks"],
            MessageBody=json.dumps(test_message),
            MessageGroupId="test-001",
            MessageDeduplicationId=f"test-001-{int(time.time())}"
        )
        ok(f"Test message sent! MessageId: {response['MessageId']}")
        return True
    except Exception as e:
        warn(f"Failed to send test message: {e}")
        return False

# ─────────────────────────────────────────────────────────────
# PRINT QUEUE INFO
# ─────────────────────────────────────────────────────────────
def print_queue_info(queue_urls: Dict[str, str]):
    """Print queue URLs and ARNs for reference"""
    print("\n📋 Queue Information:")
    print("-" * 60)
    
    for name, url in queue_urls.items():
        arn = sqs.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        
        print(f"\n{name.upper()}:")
        print(f"  URL: {url}")
        print(f"  ARN: {arn}")
    
    print("\n" + "-" * 60)

# ─────────────────────────────────────────────────────────────
# AWS CREDENTIALS CHECK
# ─────────────────────────────────────────────────────────────
def check_aws_credentials():
    """Verify AWS credentials are configured"""
    print("\n🔐 Checking AWS credentials...")
    
    try:
        identity = boto3.client("sts").get_caller_identity()
        print(f"✓ Using account: {identity['Account']}")
        print(f"✓ User/Role: {identity['Arn']}")
        return True
    except Exception as e:
        print(f"✗ AWS credentials error: {e}")
        print("\nPlease configure credentials:")
        print("  Option 1: aws configure")
        print("  Option 2: Set environment variables")
        return False

# ─────────────────────────────────────────────────────────────
# VERIFY SSM ACCESS
# ─────────────────────────────────────────────────────────────
def verify_ssm_access():
    """Verify SSM Parameter Store access"""
    print("\n🔐 Checking SSM Parameter Store access...")
    try:
        # Try to list parameters (will fail if no permissions)
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
    print("  DISTRIBUTED DEBATE SYSTEM — SQS ONLY with SSM")
    print("  Auto Scaling Ready!")
    print("="*60)
    
    # Check credentials first
    if not check_aws_credentials():
        return
    
    # Verify SSM access
    verify_ssm_access()
    
    # Setup infrastructure
    queue_urls = setup_queues()
    setup_s3()
    
    # Store configuration (BOTH local AND SSM)
    save_local_config(queue_urls)
    store_config_in_ssm(queue_urls)
    
    # Display info
    print_queue_info(queue_urls)
    
    # Optional test
    print("\n" + "="*60)
    send_test_message(queue_urls)
    
    # Next steps
    print("\n" + "="*60)
    print("✅ SETUP COMPLETE!")
    print("="*60)
    print("\nConfiguration stored in:")
    print("  • Local: debate_config.json")
    print("  • SSM Parameter Store: /research-agent/*")
    print("\nFor Auto Scaling, EC2 instances will automatically:")
    print("  1. Fetch config from SSM on startup")
    print("  2. Get OpenAI API key from SecureString")
    print("  3. Start processing messages")
    print("\nTo verify SSM config:")
    print(f"  aws ssm get-parameter --name {SSM_PATHS['tasks_queue_url']}")
    print(f"  aws ssm get-parameter --name {SSM_PATHS['config_json']} --with-decryption")
    print("\nTo deploy Auto Scaling group:")
    print("  ./deploy-auto-scaling.sh")
    print("="*60)

if __name__ == "__main__":
    main()