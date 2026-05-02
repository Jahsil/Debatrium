#!/bin/bash
# create-asg.sh - Using default VPC subnets

set -e

ASG_NAME="research-agent-asg"
LAUNCH_TEMPLATE_NAME="research-agent-template"
DESIRED_CAPACITY=2
MIN_SIZE=2
MAX_SIZE=2
REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")

echo "Region: $REGION"

# -------------------------
# GET DEFAULT VPC SUBNETS
# -------------------------
echo "Fetching default VPC subnets..."

DEFAULT_VPC=$(aws ec2 describe-vpcs \
    --region "$REGION" \
    --filters "Name=isDefault,Values=true" \
    --query "Vpcs[0].VpcId" \
    --output text)

if [[ -z "$DEFAULT_VPC" || "$DEFAULT_VPC" == "None" ]]; then
  echo "ERROR: No default VPC found in region $REGION."
  exit 1
fi

echo "Default VPC: $DEFAULT_VPC"

SUBNETS=$(aws ec2 describe-subnets \
    --region "$REGION" \
    --filters "Name=vpc-id,Values=${DEFAULT_VPC}" \
    --query "Subnets[*].SubnetId" \
    --output text | tr '\t' ',')

if [[ -z "$SUBNETS" ]]; then
  echo "ERROR: No subnets found in default VPC $DEFAULT_VPC."
  exit 1
fi

echo "Using subnets: ${SUBNETS}"

# -------------------------
# CREATE AUTO SCALING GROUP
# -------------------------
echo "Creating Auto Scaling Group: ${ASG_NAME}..."

aws autoscaling create-auto-scaling-group \
    --region "$REGION" \
    --auto-scaling-group-name "${ASG_NAME}" \
    --launch-template "LaunchTemplateName=${LAUNCH_TEMPLATE_NAME},Version=\$Latest" \
    --min-size ${MIN_SIZE} \
    --max-size ${MAX_SIZE} \
    --desired-capacity ${DESIRED_CAPACITY} \
    --vpc-zone-identifier "${SUBNETS}" \
    --health-check-type EC2 \
    --health-check-grace-period 300 \
    --tags \
        Key=Name,Value=ResearchAgent,PropagateAtLaunch=true \
        Key=Environment,Value=Lab,PropagateAtLaunch=true

echo "✓ Auto Scaling Group created: ${ASG_NAME}"

# -------------------------
# WAIT FOR INSTANCES (manual poll — 24 x 15s = 6 min max)
# -------------------------
echo "Waiting for instances to reach InService state..."

for i in $(seq 1 24); do
  IN_SERVICE=$(aws autoscaling describe-auto-scaling-groups \
      --region "$REGION" \
      --auto-scaling-group-names "${ASG_NAME}" \
      --query "length(AutoScalingGroups[0].Instances[?LifecycleState=='InService'])" \
      --output text 2>/dev/null || echo "0")

  echo "  [${i}/24] InService: ${IN_SERVICE}/${DESIRED_CAPACITY}  (checking every 15s)"

  if [[ "$IN_SERVICE" -ge "$DESIRED_CAPACITY" ]]; then
    echo "✓ All instances InService."
    break
  fi

  if [[ "$i" -eq 24 ]]; then
    echo "WARNING: Timed out after 6 minutes. Instances may still be starting."
  fi

  sleep 15
done

# -------------------------
# SHOW FINAL STATUS
# -------------------------
echo ""
echo "Current instances:"
aws autoscaling describe-auto-scaling-groups \
    --region "$REGION" \
    --auto-scaling-group-names "${ASG_NAME}" \
    --query "AutoScalingGroups[0].Instances[*].[InstanceId,LifecycleState,HealthStatus]" \
    --output table