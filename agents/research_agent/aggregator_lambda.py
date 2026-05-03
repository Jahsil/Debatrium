"""
aggregator_lambda.py — Research Results Aggregator
Triggered by SQS Event Source Mapping on `research-results-queue`.
Research agents (ECS Fargate) post findings directly to that queue — no SNS wrapper.

Flow:
  1. Parse incoming research result from SQS record body
  2. Write findings to Redis hash  (key: res:<debate_id>:r<round>)
  3. Atomically increment result counter (key: res_count:<debate_id>:r<round>)
  4. If count == expected → bundle all results → publish NUM_CRITIC_SLOTS copies to critic.tasks SQS FIFO
  5. SET NX dispatched flag to prevent duplicate dispatches across concurrent Lambda invocations
  6. Return batchItemFailures so only failed records are retried, not the whole batch
"""

import os
import json
import time
import hashlib
import logging
import boto3
import redis

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Shorthand print helpers — show up as plain lines in CloudWatch Logs
def log(msg):  print(f"→ {msg}")
def ok(msg):   print(f"✓ {msg}")
def warn(msg): print(f"⚠ {msg}")
def fail(msg): print(f"✗ {msg}")

# ─────────────────────────────────────────────────────────────
# ENVIRONMENT VARIABLES  (set in Lambda configuration)
# ─────────────────────────────────────────────────────────────
REGION             = os.environ.get("AWS_REGION", "us-east-1")
REDIS_HOST         = os.environ["REDIS_HOST"]               # ElastiCache endpoint
REDIS_PORT         = int(os.environ.get("REDIS_PORT", 6379))
REDIS_AUTH_TOKEN   = os.environ.get("REDIS_AUTH_TOKEN", "")
CRITIC_TASKS_QUEUE = os.environ["CRITIC_TASKS_QUEUE_URL"]   # SQS FIFO URL
EXPECTED_RESULTS   = int(os.environ.get("EXPECTED_RESULTS", 3))
NUM_CRITIC_SLOTS   = int(os.environ.get("NUM_CRITIC_SLOTS", 3))

# Redis key TTL — 2 hours
KEY_TTL = 7200

# ─────────────────────────────────────────────────────────────
# AWS CLIENTS
# ─────────────────────────────────────────────────────────────
sqs = boto3.client("sqs", region_name=REGION)


# ─────────────────────────────────────────────────────────────
# REDIS CONNECTION
# ─────────────────────────────────────────────────────────────
def get_redis() -> redis.Redis:
    """
    Create a Redis connection.
    Lambda execution context reuse means this runs once per warm container,
    effectively acting as a connection pool across invocations.
    """
    log(f"Connecting to Redis — {REDIS_HOST}:{REDIS_PORT}")
    kwargs = {
        "host":                   REDIS_HOST,
        "port":                   REDIS_PORT,
        "decode_responses":       True,
        "ssl":                    True,
        "socket_timeout":         5,
        "socket_connect_timeout": 5,
    }
    if REDIS_AUTH_TOKEN:
        kwargs["password"] = REDIS_AUTH_TOKEN
        log("Redis AUTH token set")
    else:
        log("Redis AUTH token not set — connecting without password")
    r = redis.Redis(**kwargs)
    # Ping to verify connection is live before processing any records
    r.ping()
    ok("Redis connection established")
    return r


# ─────────────────────────────────────────────────────────────
# REDIS OPERATIONS
# ─────────────────────────────────────────────────────────────
def store_result(r: redis.Redis, debate_id: str, round_num: int, angle: str, findings: dict) -> int:
    """
    Write findings to Redis hash and atomically increment the result counter.

    Keys used:
      res:<debate_id>:r<round>        — Hash of { angle: findings_json }
      res_count:<debate_id>:r<round>  — Integer counter, one increment per angle

    Returns the new counter value after this write.
    """
    res_key   = f"res:{debate_id}:r{round_num}"
    count_key = f"res_count:{debate_id}:r{round_num}"

    log(f"Writing to Redis — key={res_key} angle={angle}")
    pipe = r.pipeline()
    pipe.hset(res_key, angle, json.dumps(findings))
    pipe.expire(res_key, KEY_TTL)
    pipe.incr(count_key)
    pipe.expire(count_key, KEY_TTL)
    results = pipe.execute()

    new_count = results[2]  # INCR return value is the third command (index 2)
    ok(f"Redis write done — angle={angle} | progress={new_count}/{EXPECTED_RESULTS}")
    logger.info(f"Stored result — {res_key}[{angle}] | count={new_count}/{EXPECTED_RESULTS}")
    return new_count


def get_all_results(r: redis.Redis, debate_id: str, round_num: int) -> dict:
    """
    Read all findings for this debate round from Redis.
    Returns a dict of { angle: findings_dict }.
    """
    res_key = f"res:{debate_id}:r{round_num}"
    raw = r.hgetall(res_key)
    return {angle: json.loads(findings) for angle, findings in raw.items()}


def get_query(r: redis.Redis, debate_id: str) -> str:
    """
    Retrieve the original user query for this debate.
    Written by the orchestrator when the debate is initialised.
    """
    return r.get(f"query:{debate_id}") or ""


def get_expected(r: redis.Redis, debate_id: str) -> int:
    """
    Return the expected result count for this debate.
    Falls back to the Lambda environment variable if not set in Redis.
    Allows per-debate flexibility (e.g. different numbers of research angles).
    """
    stored = r.get(f"expected:{debate_id}")
    return int(stored) if stored else EXPECTED_RESULTS


def mark_dispatched(r: redis.Redis, debate_id: str, round_num: int) -> bool:
    """
    Atomically set the dispatched flag using SET NX (set only if not exists).

    Because multiple Lambda invocations may reach the dispatch threshold concurrently,
    only the one that successfully sets this key should fan-out to critics.

    Returns True  → this invocation won the race, proceed with dispatch.
    Returns False → another invocation already dispatched, skip.
    """
    dispatch_key = f"dispatched:{debate_id}:r{round_num}"
    result = r.set(dispatch_key, "true", nx=True, ex=KEY_TTL)
    return result is True


# ─────────────────────────────────────────────────────────────
# CRITIC DISPATCH
# ─────────────────────────────────────────────────────────────
def dispatch_to_critics(debate_id: str, round_num: int, query: str, all_results: dict) -> None:
    """
    Publish NUM_CRITIC_SLOTS copies of the full research bundle to the critic.tasks SQS FIFO queue.

    Each copy gets a unique MessageGroupId (CA1…CA3) so SQS routes it to a different
    consumer (critic agent container), achieving fan-out over a FIFO queue.

    MessageDeduplicationId is deterministic so re-runs / retries are idempotent within
    the SQS 5-minute deduplication window.
    """
    log(f"Dispatching to {NUM_CRITIC_SLOTS} critic slots — debate_id={debate_id} round={round_num}")
    print(f"  Query: {query[:80]}{'...' if len(query) > 80 else ''}")
    print(f"  Angles in bundle: {list(all_results.keys())}")

    bundle = {
        "debate_id": debate_id,
        "round":     round_num,
        "query":     query,
        "results":   all_results,
        "timestamp": time.time(),
    }

    for slot in range(1, NUM_CRITIC_SLOTS + 1):
        group_id = f"CA{slot}-{debate_id}-r{round_num}"
        dedup_id = hashlib.md5(
            f"{debate_id}-r{round_num}-slot{slot}".encode()
        ).hexdigest()

        log(f"Sending to critic slot {slot} — group={group_id}")
        sqs.send_message(
            QueueUrl=CRITIC_TASKS_QUEUE,
            MessageBody=json.dumps({**bundle, "critic_slot": slot}),
            MessageGroupId=group_id,
            MessageDeduplicationId=dedup_id,
        )
        ok(f"Critic slot {slot} dispatched | group={group_id} | dedup={dedup_id}")

    ok(f"All {NUM_CRITIC_SLOTS} critic slots dispatched for debate_id={debate_id}")


# ─────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    """
    SQS trigger — Lambda is invoked by the `research-results-queue` Event Source Mapping.

    Each SQS record body is a raw JSON payload posted directly by a research agent:
    {
        "debate_id": "<uuid>",
        "round":     <int>,
        "angle":     "<perspective_name>",
        "findings":  { ... }
    }

    Partial batch failure is enabled (ReportBatchItemFailures = true on the ESM), so
    returning batchItemFailures causes only the failed records to be retried / sent to DLQ.
    Successfully processed records are deleted from the queue automatically.
    """
    record_count = len(event.get("Records", []))
    print("\n" + "="*60)
    print(f"  AGGREGATOR LAMBDA INVOKED")
    print(f"  Records in batch : {record_count}")
    print(f"  Function version : {context.function_version}")
    print(f"  Log stream       : {context.log_stream_name}")
    print("="*60)

    # ── Connect to Redis once per invocation ──────────────────
    try:
        r = get_redis()
    except Exception as e:
        fail(f"Could not connect to Redis — aborting all {record_count} records: {e}")
        return {
            "batchItemFailures": [
                {"itemIdentifier": rec["messageId"]}
                for rec in event.get("Records", [])
            ]
        }

    failed_message_ids = []

    for i, record in enumerate(event.get("Records", []), 1):
        message_id = record["messageId"]
        print(f"\n─── Record {i}/{record_count} — messageId={message_id} ───")

        try:
            # ── 1. Parse the SQS message body ────────────────────────────
            log("Stage 1 — Parsing SQS message body")
            payload   = json.loads(record["body"])
            debate_id = payload["debate_id"]
            round_num = int(payload["round"])
            angle     = payload["angle"]
            findings  = payload["findings"]
            ok(f"Parsed — debate_id={debate_id} round={round_num} angle={angle}")

            # ── 2. Write findings to Redis + increment counter ────────────
            log("Stage 2 — Writing findings to Redis")
            new_count = store_result(r, debate_id, round_num, angle, findings)

            # ── 3. How many results are we waiting for? ───────────────────
            log("Stage 3 — Checking completion threshold")
            expected = get_expected(r, debate_id)
            print(f"  Progress: {new_count}/{expected} results received")

            if new_count < expected:
                warn(f"Not ready yet — waiting for {expected - new_count} more result(s)")
                continue

            ok(f"All {expected} results received for debate_id={debate_id} round={round_num}")

            # ── 4. Race-safe dispatch gate ────────────────────────────────
            log("Stage 4 — Acquiring dispatch lock (SET NX)")
            if not mark_dispatched(r, debate_id, round_num):
                warn(f"Another invocation already dispatched — skipping duplicate dispatch")
                continue
            ok("Dispatch lock acquired — this invocation will fan-out to critics")

            # ── 5. Read full result set + original query ──────────────────
            log("Stage 5 — Loading full result bundle from Redis")
            all_results = get_all_results(r, debate_id, round_num)
            query       = get_query(r, debate_id)
            ok(f"Loaded {len(all_results)} angle(s) from Redis")
            print(f"  Angles: {list(all_results.keys())}")
            print(f"  Query : {query[:100]}{'...' if len(query) > 100 else ''}")

            # ── 6. Fan-out to critic agents ───────────────────────────────
            log("Stage 6 — Dispatching to critic agents")
            dispatch_to_critics(debate_id, round_num, query, all_results)
            ok(f"Record {i} fully processed ✓")

        except KeyError as e:
            fail(f"Stage 1 failed — malformed payload, missing field: {e}")
            logger.error(f"Malformed payload — missing field {e} in record {message_id}", exc_info=True)
            failed_message_ids.append(message_id)

        except redis.RedisError as e:
            fail(f"Redis error on record {message_id}: {e}")
            logger.error(f"Redis error processing record {message_id}: {e}", exc_info=True)
            failed_message_ids.append(message_id)

        except Exception as e:
            fail(f"Unexpected error on record {message_id}: {e}")
            logger.error(f"Unexpected error processing record {message_id}: {e}", exc_info=True)
            failed_message_ids.append(message_id)

    # ── Final summary ─────────────────────────────────────────
    print("\n" + "="*60)
    succeeded = record_count - len(failed_message_ids)
    print(f"  BATCH COMPLETE — {succeeded}/{record_count} succeeded")
    if failed_message_ids:
        warn(f"{len(failed_message_ids)} record(s) failed — returning for retry/DLQ")
        print("="*60 + "\n")
        return {
            "batchItemFailures": [
                {"itemIdentifier": mid} for mid in failed_message_ids
            ]
        }

    print("="*60 + "\n")
    return {"statusCode": 200, "body": "OK"}