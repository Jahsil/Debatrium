"""
Research Agent — SQS Only 
────────────────────────────────────────────────────────────
Polls research-tasks SQS FIFO queue.
For each task:
  1. Call OpenAI GPT-4o to research the assigned angle
  2. Send result DIRECTLY to research-results SQS queue
  3. Delete message (ACK)

On crash: SQS visibility timeout expires → message requeues
EC2 auto-scaling or manual restart picks it up.
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

# ── Environment ──────────────────────────────────────────────────────────────
REGION              = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
SQS_TASKS_QUEUE     = os.environ["RESEARCH_TASKS_QUEUE_URL"]      # input queue
SQS_RESULTS_QUEUE   = os.environ["RESEARCH_RESULTS_QUEUE_URL"]    # output queue
AGENT_ID            = os.environ.get("RESEARCH_AGENT_ID", "RA-1")
MAX_MESSAGES        = int(os.environ.get("MAX_MESSAGES_PER_POLL", "1"))
VISIBILITY_TIMEOUT  = int(os.environ.get("VISIBILITY_TIMEOUT", "120"))

# ── AWS clients (using default credentials from EC2 role or env) ────────────
sqs = boto3.client("sqs", region_name=REGION)
llm = OpenAI()   # reads OPENAI_API_KEY from env automatically

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
    
    # Message group ID must be the debate_id to preserve ordering per debate
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
        # Use content-based deduplication (queue must have this enabled)
        response = sqs.send_message(
            QueueUrl=SQS_RESULTS_QUEUE,
            MessageBody=json.dumps(message),
            MessageGroupId=debate_id,  # FIFO queues need this
            # MessageDeduplicationId is auto-generated if ContentBasedDeduplication=true
        )
        log.info(f"[{debate_id}] Sent to results queue. MessageId: {response['MessageId']}")
        return True
    except Exception as e:
        log.error(f"[{debate_id}] Failed to send to results queue: {e}")
        raise


def process_message(msg: dict) -> bool:
    """
    Process one SQS message from tasks queue.
    Returns True on success (message will be deleted).
    """
    receipt_handle = msg["ReceiptHandle"]
    body = json.loads(msg["Body"])

    debate_id       = body["debate_id"]
    round_num       = body["round"]
    angle           = body["angle"]
    query           = body["query"]
    instructions    = body["instructions"]
    prior_critique  = body.get("prior_critique")
    
    # Optional metadata
    expected_angles = body.get("expected_angles", 3)
    total_rounds = body.get("total_rounds", 5)

    log.info(f"[{debate_id}] r{round_num} angle='{angle}' | Expected angles: {expected_angles} | Total rounds: {total_rounds}")

    # ── STEP 1: Research the angle ─────────────────────────────────────────
    try:
        findings = research_angle(query, angle, instructions, prior_critique)
    except Exception as e:
        log.error(f"[{debate_id}] LLM call failed: {e}")
        raise  # Let SQS retry (visibility timeout will expire)

    log.info(f"[{debate_id}] Research complete. Confidence: {findings.get('confidence', 0)}")

    # ── STEP 2: Send result to SQS results queue ──────────────────────────
    send_to_results_queue(debate_id, round_num, angle, findings)

    # ── STEP 3: ACK (delete from input queue) ─────────────────────────────
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
    log.info(f" RESEARCH AGENT STARTING (SQS-ONLY MODE)")
    log.info(f"="*60)
    log.info(f"Agent ID:      {AGENT_ID}")
    log.info(f"Region:        {REGION}")
    log.info(f"Tasks Queue:   {SQS_TASKS_QUEUE}")
    log.info(f"Results Queue: {SQS_RESULTS_QUEUE}")
    log.info(f"Max messages:  {MAX_MESSAGES}")
    log.info(f"Visibility:    {VISIBILITY_TIMEOUT}s")
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
                WaitTimeSeconds=20,  # Long polling to reduce costs
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
                    # Don't delete - message will become visible again after timeout
                    # After 3 attempts, it moves to DLQ automatically
                    log.warning(f"Message will retry in {VISIBILITY_TIMEOUT}s (attempt {receive_count}/3)")

        except KeyboardInterrupt:
            log.info(f"\nShutting down. Total messages processed: {messages_processed}")
            break
        except Exception as e:
            log.error(f"Outer loop error: {e}", exc_info=True)
            time.sleep(5)


if __name__ == "__main__":
    main()



