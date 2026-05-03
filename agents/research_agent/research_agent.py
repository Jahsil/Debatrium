"""
Research Agent — SQS Only with SSM Support
Now fetches configuration from SSM Parameter Store for Auto Scaling
"""

import os
import json
import time
import logging
import boto3
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
)
log = logging.getLogger("research-agent")

# ── SSM CONFIGURATION FETCH (for Auto Scaling) ──────────────────────────────
def fetch_from_ssm(param_name: str, with_decryption: bool = False) -> str:
    """Fetch a parameter from SSM Parameter Store"""
    try:
        ssm = boto3.client('ssm')
        response = ssm.get_parameter(
            Name=param_name,
            WithDecryption=with_decryption
        )
        return response['Parameter']['Value']
    except Exception as e:
        log.debug(f"Could not fetch {param_name} from SSM: {e}")
        return None

def get_config_from_ssm():
    """Get all configuration from SSM Parameter Store"""
    config = {}
    
    # Try to get full config JSON first
    full_config = fetch_from_ssm('/research-agent/config')
    if full_config:
        try:
            config = json.loads(full_config)
            log.info("✓ Loaded configuration from SSM")
            return config
        except:
            pass
    
    # Fallback to individual parameters
    tasks_queue = fetch_from_ssm('/research-agent/tasks-queue-url')
    if tasks_queue:
        config['tasks_queue'] = tasks_queue
    
    results_queue = fetch_from_ssm('/research-agent/results-queue-url')
    if results_queue:
        config['results_queue'] = results_queue
    
    return config

# ── Environment variables (with SSM fallback) ──────────────────────────────
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Try to get queue URLs from environment first, then SSM
SQS_TASKS_QUEUE = os.environ.get("RESEARCH_TASKS_QUEUE_URL")
SQS_RESULTS_QUEUE = os.environ.get("RESEARCH_RESULTS_QUEUE_URL")

# If not in environment, fetch from SSM
if not SQS_TASKS_QUEUE or not SQS_RESULTS_QUEUE:
    log.info("Environment variables not set, checking SSM Parameter Store...")
    ssm_config = get_config_from_ssm()
    
    if not SQS_TASKS_QUEUE:
        SQS_TASKS_QUEUE = ssm_config.get('tasks_queue') or ssm_config.get('queues', {}).get('research_tasks')
    if not SQS_RESULTS_QUEUE:
        SQS_RESULTS_QUEUE = ssm_config.get('results_queue') or ssm_config.get('queues', {}).get('research_results')

# Validate required config
if not SQS_TASKS_QUEUE or not SQS_RESULTS_QUEUE:
    log.error("=" * 60)
    log.error("MISSING CONFIGURATION!")
    log.error("=" * 60)
    log.error("Could not find queue URLs in:")
    log.error("  1. Environment variables (RESEARCH_TASKS_QUEUE_URL, RESEARCH_RESULTS_QUEUE_URL)")
    log.error("  2. SSM Parameter Store (/research-agent/tasks-queue-url, /research-agent/results-queue-url)")
    log.error("")
    log.error("Please run setup.py first to create queues and store configuration.")
    sys.exit(1)

# Agent configuration (with defaults)
AGENT_ID = os.environ.get("RESEARCH_AGENT_ID", f"RA-{os.uname().nodename}")
MAX_MESSAGES = int(os.environ.get("MAX_MESSAGES_PER_POLL", "5"))
VISIBILITY_TIMEOUT = int(os.environ.get("VISIBILITY_TIMEOUT", "120"))

# OpenAI API Key (try environment first, then SSM)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    log.info("OpenAI API key not in environment, checking SSM...")
    OPENAI_API_KEY = fetch_from_ssm('/research-agent/openai-api-key', with_decryption=True)

if not OPENAI_API_KEY:
    log.error("=" * 60)
    log.error("OPENAI API KEY MISSING!")
    log.error("=" * 60)
    log.error("Could not find OpenAI API key in:")
    log.error("  1. Environment variable (OPENAI_API_KEY)")
    log.error("  2. SSM Parameter Store (/research-agent/openai-api-key)")
    log.error("")
    log.error("Please add your API key to SSM:")
    log.error("  aws ssm put-parameter --name /research-agent/openai-api-key --value 'your-key' --type SecureString --overwrite")
    sys.exit(1)

# ── AWS clients ─────────────────────────────────────────────────────────────
sqs = boto3.client("sqs", region_name=REGION)
llm = OpenAI(api_key=OPENAI_API_KEY)

# ── LLM research call ─────────────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a specialized research agent in a multi-agent debate system.
Your job is to research a specific angle of a query thoroughly and objectively.
Respond ONLY with valid JSON — no markdown fences, no extra text.
Schema:
{
  "angle": "<the angle you researched>",
  "summary": "<3-5 sentence summary of key findings>",
  "key_points": ["<point 1>", "<point 2>", "<point 3>"],
  "evidence": ["<evidence/source 1>", "<evidence/source 2>"],
  "confidence": <float 0.0-1.0>,
  "limitations": "<what this angle misses or cannot address>"
}
Be factual, cite real evidence, and be honest about confidence levels."""


def research_angle(query: str, angle: str, instructions: str, prior_critique: str = None) -> dict:
    """Call OpenAI GPT-4o to research a specific angle of the query."""
    user_content = f"Query: {query}\nAngle to research: {angle}\nInstructions: {instructions}"
    if prior_critique:
        user_content += f"\n\nPrior critique of this angle (address these gaps): {prior_critique}"

    log.info(f"Calling OpenAI for angle: {angle}")
    response = llm.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": user_content},
        ],
        temperature=0.3,
        max_tokens=1024,
        response_format={"type": "json_object"},
    )
    return json.loads(response.choices[0].message.content)


def send_to_results_queue(debate_id: str, round_num: int, angle: str, findings: dict) -> bool:
    """Send research result to SQS results queue (no SNS)"""
    
    message = {
        "debate_id": debate_id,
        "round": round_num,
        "angle": angle,
        "agent_id": AGENT_ID,
        "findings": findings,
        "timestamp": int(time.time()),
        "event_type": "RESEARCH_RESULT",
    }
    
    try:
        response = sqs.send_message(
            QueueUrl=SQS_RESULTS_QUEUE,
            MessageBody=json.dumps(message),
            MessageGroupId=debate_id,
        )
        log.info(f"[{debate_id}] Sent to results queue. MessageId: {response['MessageId']}")
        return True
    except Exception as e:
        log.error(f"[{debate_id}] Failed to send to results queue: {e}")
        raise


def process_message(msg: dict) -> bool:
    """Process one SQS message from tasks queue."""
    receipt_handle = msg["ReceiptHandle"]
    body = json.loads(msg["Body"])

    debate_id       = body["debate_id"]
    round_num       = body["round"]
    angle           = body["angle"]
    query           = body["query"]
    instructions    = body["instructions"]
    prior_critique  = body.get("prior_critique")
    
    expected_angles = body.get("expected_angles", 3)
    total_rounds = body.get("total_rounds", 5)

    log.info(f"[{debate_id}] r{round_num} angle='{angle}' | Expected angles: {expected_angles} | Total rounds: {total_rounds}")

    # STEP 1: Research the angle
    try:
        findings = research_angle(query, angle, instructions, prior_critique)
    except Exception as e:
        log.error(f"[{debate_id}] LLM call failed: {e}")
        raise

    log.info(f"[{debate_id}] Research complete. Confidence: {findings.get('confidence', 0)}")

    # STEP 2: Send result to SQS results queue
    send_to_results_queue(debate_id, round_num, angle, findings)

    # STEP 3: ACK (delete from input queue)
    sqs.delete_message(
        QueueUrl=SQS_TASKS_QUEUE,
        ReceiptHandle=receipt_handle,
    )
    log.info(f"[{debate_id}] Message ACK'd and removed from tasks queue ✓")
    return True


def get_queue_attributes():
    """Log queue statistics for monitoring"""
    try:
        attrs = sqs.get_queue_attributes(
            QueueUrl=SQS_TASKS_QUEUE,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
        )
        visible = attrs["Attributes"].get("ApproximateNumberOfMessages", "0")
        in_flight = attrs["Attributes"].get("ApproximateNumberOfMessagesNotVisible", "0")
        log.info(f"Queue stats - Visible: {visible} | In flight: {in_flight}")
    except Exception as e:
        log.debug(f"Could not get queue stats: {e}")


def main():
    log.info(f"="*60)
    log.info(f" RESEARCH AGENT STARTING (SQS-ONLY + SSM MODE)")
    log.info(f"="*60)
    log.info(f"Agent ID:      {AGENT_ID}")
    log.info(f"Region:        {REGION}")
    log.info(f"Tasks Queue:   {SQS_TASKS_QUEUE}")
    log.info(f"Results Queue: {SQS_RESULTS_QUEUE}")
    log.info(f"Max messages:  {MAX_MESSAGES}")
    log.info(f"Visibility:    {VISIBILITY_TIMEOUT}s")
    log.info(f"Config Source: SSM Parameter Store (fallback)")
    log.info(f"="*60)

    messages_processed = 0
    last_stats_log = time.time()

    while True:
        try:
            # Log stats every 60 seconds
            if time.time() - last_stats_log > 60:
                get_queue_attributes()
                log.info(f"Total processed by this agent: {messages_processed}")
                last_stats_log = time.time()

            # Receive message from tasks queue
            resp = sqs.receive_message(
                QueueUrl=SQS_TASKS_QUEUE,
                MaxNumberOfMessages=MAX_MESSAGES,
                WaitTimeSeconds=20,
                VisibilityTimeout=VISIBILITY_TIMEOUT,
                AttributeNames=["All"],
                MessageAttributeNames=["All"],
            )
            messages = resp.get("Messages", [])

            if not messages:
                continue

            for msg in messages:
                receive_count = int(msg["Attributes"].get("ApproximateReceiveCount", 1))
                log.info(f"Received message (attempt #{receive_count})")
                
                try:
                    process_message(msg)
                    messages_processed += 1
                except Exception as e:
                    log.error(f"Failed to process message: {e}", exc_info=True)
                    log.warning(f"Message will retry in {VISIBILITY_TIMEOUT}s (attempt {receive_count}/3)")

        except KeyboardInterrupt:
            log.info(f"\nShutting down. Total messages processed: {messages_processed}")
            break
        except Exception as e:
            log.error(f"Outer loop error: {e}", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    import sys
    main()