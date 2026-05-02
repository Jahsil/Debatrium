"""
Simplified Distributed Debate System — SQS Only 

Pure SQS FIFO queues for all messaging:
  • research-tasks     → research agents
  • research-results   → aggregator  
  • critic-tasks       → critic agents
  • judge-tasks        → judges
  • final-results      → notification service
"""

import boto3
import json
import time
from typing import Dict, Optional


# CONFIGURATION

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


# CLIENTS (using default credentials from ~/.aws/credentials)

sqs = boto3.client("sqs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


# HELPERS

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
        "ContentBasedDeduplication": "true",  # Auto-dedupe identical messages
        "VisibilityTimeout": "60",
        "MessageRetentionPeriod": "345600",  # 4 days
    }
    
    if dlq_arn:
        attrs["RedrivePolicy"] = json.dumps({
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount": "3"
        })
    
    response = sqs.create_queue(QueueName=name, Attributes=attrs)
    return response["QueueUrl"]


# QUEUE SETUP

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
            # Get ARN
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
        
        time.sleep(0.5)  # Small delay to avoid throttling
    
    return queue_urls


# S3 SETUP

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
    
    # Block all public access
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


# SAVE CONFIGURATION

def save_config(queue_urls: Dict[str, str]):
    """Save configuration for workers to use"""
    config = {
        "region": REGION,
        "account_id": ACCOUNT_ID,
        "queues": queue_urls,
        "bucket": BUCKET,
        "redis": {
            "host": "localhost",  # Change to ElastiCache endpoint in prod
            "port": 6379,
        }
    }
    
    with open("debate_config.json", "w") as f:
        json.dump(config, f, indent=2)
    
    ok("Saved debate_config.json")


# TEST MESSAGE

def send_test_message(queue_urls: Dict[str, str]):
    """Send a test message through the system"""
    print("\n🧪 Sending test message...")
    
    # Test message for research_tasks
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
            MessageGroupId="test-001",  # Required for FIFO
            MessageDeduplicationId=f"test-001-{int(time.time())}"
        )
        ok(f"Test message sent! MessageId: {response['MessageId']}")
        return True
    except Exception as e:
        warn(f"Failed to send test message: {e}")
        return False


# PRINT QUEUE INFO

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


# AWS CREDENTIALS CHECK

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
        print("  Option 2: Set environment variables:")
        print("    export AWS_ACCESS_KEY_ID=...")
        print("    export AWS_SECRET_ACCESS_KEY=...")
        print("    export AWS_DEFAULT_REGION=us-east-1")
        return False


# MAIN

def main():
    print("\n" + "="*60)
    print("  DISTRIBUTED DEBATE SYSTEM — SQS ONLY")
    print("  No IAM Roles · No KMS · No SNS")
    print("="*60)
    
    # Check credentials first
    if not check_aws_credentials():
        return
    
    # Setup infrastructure
    queue_urls = setup_queues()
    setup_s3()
    save_config(queue_urls)
    
    # Display info
    print_queue_info(queue_urls)
    
    # Optional test
    print("\n" + "="*60)
    send_test_message(queue_urls)
    
    # Next steps
    print("\n" + "="*60)
    print("✅ SETUP COMPLETE!")
    print("="*60)
    print("\nNext steps:")
    print("  1. Start Redis: docker run -d -p 6379:6379 redis:7-alpine")
    print("  2. Run research agents: python research_agent.py")
    print("  3. Run aggregator: python aggregator.py")
    print("  4. Run critic agents: python critic_agent.py")
    print("  5. Run judges: python judge_agent.py")
    print("  6. Run notification service: python notify.py")
    print("\nArchitecture:")
    print("  research-tasks → [Research Agents] → research-results")
    print("  research-results → [Aggregator] → critic-tasks")
    print("  critic-tasks → [Critic Agents] → judge-tasks")
    print("  judge-tasks → [Judges] → final-results")
    print("  final-results → [Notification] → Users")
    print("\nMonitoring:")
    print("  Check DLQs for failed messages:")
    for name, dlq_name in DLQS.items():
        print(f"    • {dlq_name}")
    print("="*60)



if __name__ == "__main__":
    main()
   