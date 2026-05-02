#!/bin/bash
set -e

# -------------------------
# CONFIGURATION
# -------------------------
LAUNCH_TEMPLATE_NAME="research-agent-template"
INSTANCE_TYPE="t3.micro"
AMI_ID="ami-02b9a589195146a8f"
SECURITY_GROUP_ID="sg-037056b3a9748c27b"
INSTANCE_PROFILE="LabInstanceProfile"
REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")

echo "Using Launch Template: $LAUNCH_TEMPLATE_NAME"
echo "Instance Type:         $INSTANCE_TYPE"
echo "AMI ID:                $AMI_ID"
echo "Security Group:        $SECURITY_GROUP_ID"
echo "Instance Profile:      $INSTANCE_PROFILE"
echo "Region:                $REGION"

# -------------------------
# USER DATA SCRIPT
# Outer delimiter: OUTEREOF (quoted = no variable expansion by local shell)
# Inner delimiters: ENDOFPYTHON, SERVICEFILE (different names = no collision)
# -------------------------
cat > /tmp/user-data.sh << 'OUTEREOF'
#!/bin/bash
set -e
exec > >(tee /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "=========================================="
echo "Research Agent Auto-Scaling Deployment"
echo "=========================================="

INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)
AGENT_ID="RA-${INSTANCE_ID}"

echo "Instance ID: $INSTANCE_ID"
echo "Region:      $REGION"
echo "Agent ID:    $AGENT_ID"

# -------------------------
# INSTALL DEPENDENCIES
# -------------------------
yum update -y
yum install -y python3-pip

mkdir -p /opt/research-agent
cd /opt/research-agent

pip3 install --upgrade pip
pip3 install boto3 openai python-dotenv

# Resolve python3 path at install time so the service file is always correct
PYTHON_BIN=$(which python3)
echo "Python binary: $PYTHON_BIN"

# -------------------------
# WRITE RESEARCH AGENT PYTHON SCRIPT
# -------------------------
cat > /opt/research-agent/research_agent.py << 'ENDOFPYTHON'
import os
import sys
import json
import time
import logging
import hashlib
import urllib.request
import boto3
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s"
)
log = logging.getLogger("research-agent")


def get_region():
    """Fetch region from EC2 instance metadata (IMDSv1).
    Falls back to env var then us-east-1 if metadata is unreachable."""
    try:
        url = "http://169.254.169.254/latest/meta-data/placement/region"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=2) as r:
            return r.read().decode().strip()
    except Exception:
        return os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


REGION   = get_region()
AGENT_ID = os.environ.get("RESEARCH_AGENT_ID", "unknown")
log.info(f"Region: {REGION} | Agent: {AGENT_ID}")


def get_ssm(name, decrypt=False):
    try:
        ssm = boto3.client('ssm', region_name=REGION)
        return ssm.get_parameter(Name=name, WithDecryption=decrypt)['Parameter']['Value']
    except Exception as e:
        log.error(f"SSM error [{name}]: {e}")
        return None


SQS_TASKS_QUEUE   = get_ssm('/research-agent/tasks-queue-url')
SQS_RESULTS_QUEUE = get_ssm('/research-agent/results-queue-url')
OPENAI_API_KEY    = get_ssm('/research-agent/openai-api-key', decrypt=True)

if not all([SQS_TASKS_QUEUE, SQS_RESULTS_QUEUE, OPENAI_API_KEY]):
    log.error("Missing required SSM config — aborting.")
    sys.exit(1)

sqs = boto3.client('sqs', region_name=REGION)
llm = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = "You are a research agent. Return ONLY valid JSON with no extra text."


def research(query, angle):
    resp = llm.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": f"Query: {query}\nAngle: {angle}"}
        ],
        response_format={"type": "json_object"}
    )
    return json.loads(resp.choices[0].message.content)


def loop():
    log.info(f"Agent {AGENT_ID} entering polling loop...")
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_TASKS_QUEUE,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            msgs = response.get("Messages", [])
        except Exception as e:
            log.error(f"Failed to poll SQS: {e}")
            time.sleep(5)
            continue

        for msg in msgs:
            receipt = msg["ReceiptHandle"]
            try:
                body   = json.loads(msg["Body"])
                query  = body["query"]
                angle  = body["angle"]

                log.info(f"Processing — query: {query!r} | angle: {angle!r}")
                result = research(query, angle)

                msg_body = json.dumps({
                    "agent_id": AGENT_ID,
                    "query":    query,
                    "angle":    angle,
                    "result":   result
                })

                # FIFO queues require MessageGroupId + MessageDeduplicationId
                dedup_id = hashlib.md5(
                    f"{AGENT_ID}-{query}-{angle}-{time.time()}".encode()
                ).hexdigest()

                sqs.send_message(
                    QueueUrl=SQS_RESULTS_QUEUE,
                    MessageBody=msg_body,
                    MessageGroupId=AGENT_ID,
                    MessageDeduplicationId=dedup_id
                )
                log.info(f"Result sent for query: {query!r}")

            except Exception as e:
                log.error(f"Error processing message: {e}")
            finally:
                # Always delete the message to avoid reprocessing
                try:
                    sqs.delete_message(
                        QueueUrl=SQS_TASKS_QUEUE,
                        ReceiptHandle=receipt
                    )
                except Exception as e:
                    log.error(f"Failed to delete message: {e}")


if __name__ == "__main__":
    loop()
ENDOFPYTHON

chmod +x /opt/research-agent/research_agent.py

# -------------------------
# WRITE SYSTEMD SERVICE
# Uses $PYTHON_BIN resolved above — baked in at install time
# Uses SERVICEFILE delimiter (not EOF) to avoid heredoc collision
# -------------------------
cat > /etc/systemd/system/research-agent.service << SERVICEFILE
[Unit]
Description=Research Agent
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/research-agent
Environment=RESEARCH_AGENT_ID=${AGENT_ID}
ExecStart=${PYTHON_BIN} /opt/research-agent/research_agent.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
SERVICEFILE

# -------------------------
# ENABLE AND START SERVICE
# -------------------------
systemctl daemon-reload
systemctl enable research-agent
systemctl start research-agent

echo "=========================================="
echo "Deployment complete!"
echo "Agent ID:    ${AGENT_ID}"
echo "Python:      ${PYTHON_BIN}"
echo "Service:     $(systemctl is-active research-agent)"
echo "=========================================="
OUTEREOF

chmod +x /tmp/user-data.sh

# -------------------------
# BASE64 ENCODE USER DATA
# -i is supported on both macOS and Linux
# tr -d '\n' strips line breaks AWS doesn't want
# -------------------------
USER_DATA_B64=$(base64 -i /tmp/user-data.sh | tr -d '\n')


if aws ec2 describe-launch-templates \
     --region "$REGION" \
     --launch-template-names "$LAUNCH_TEMPLATE_NAME" \
     --query "LaunchTemplates[0].LaunchTemplateName" \
     --output text 2>/dev/null | grep -q "$LAUNCH_TEMPLATE_NAME"; then
  echo "Deleting existing launch template: $LAUNCH_TEMPLATE_NAME"
  aws ec2 delete-launch-template \
    --region "$REGION" \
    --launch-template-name "$LAUNCH_TEMPLATE_NAME"
fi

# -------------------------
# CREATE LAUNCH TEMPLATE
# -------------------------
aws ec2 create-launch-template \
  --region "$REGION" \
  --launch-template-name "$LAUNCH_TEMPLATE_NAME" \
  --version-description "v3-autostart-fix" \
  --launch-template-data "{
    \"ImageId\": \"$AMI_ID\",
    \"InstanceType\": \"$INSTANCE_TYPE\",
    \"SecurityGroupIds\": [\"$SECURITY_GROUP_ID\"],
    \"IamInstanceProfile\": {
      \"Name\": \"$INSTANCE_PROFILE\"
    },
    \"UserData\": \"$USER_DATA_B64\",
    \"TagSpecifications\": [
      {
        \"ResourceType\": \"instance\",
        \"Tags\": [
          {\"Key\": \"Name\",  \"Value\": \"research-agent\"},
          {\"Key\": \"Role\",  \"Value\": \"research-agent\"}
        ]
      }
    ]
  }"

echo ""
echo "Launch template '$LAUNCH_TEMPLATE_NAME' created successfully."