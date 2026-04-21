#!/usr/bin/env bash
# Shared deploy config. Source at the top of each deploy script:
#   source "$(dirname "${BASH_SOURCE[0]}")/config.sh"

set -euo pipefail

# AWS target
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-009910375489}"
export ENVIRONMENT="${ENVIRONMENT:-prod}"

# Per-tenant identifier. Override for multi-tenant deploys:
#   TENANT_ID=real-tenant ./02-create-sqs.sh
export TENANT_ID="${TENANT_ID:-test-tenant}"

# Deterministic resource names
export QUEUE_NAME="adapty-ct-${TENANT_ID}"
export DLQ_NAME="adapty-ct-${TENANT_ID}-dlq"
export LAMBDA_NAME="adapty-ct-${TENANT_ID}"
export LAMBDA_ROLE_NAME="adapty-ct-lambda-role-${TENANT_ID}"
export SECRET_NAME="/adapty-ct-connector/${ENVIRONMENT}/${TENANT_ID}/credentials"
export LOG_GROUP="/aws/lambda/${LAMBDA_NAME}"

# Deterministic ARNs (don't require AWS lookups)
export QUEUE_URL="https://sqs.${AWS_REGION}.amazonaws.com/${AWS_ACCOUNT_ID}/${QUEUE_NAME}"
export QUEUE_ARN="arn:aws:sqs:${AWS_REGION}:${AWS_ACCOUNT_ID}:${QUEUE_NAME}"
export DLQ_ARN="arn:aws:sqs:${AWS_REGION}:${AWS_ACCOUNT_ID}:${DLQ_NAME}"
export LAMBDA_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${LAMBDA_ROLE_NAME}"

# State file for non-deterministic IDs (API Gateway ID, etc.)
DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATE_FILE="${DEPLOY_DIR}/.state/${TENANT_ID}.env"
mkdir -p "$(dirname "$STATE_FILE")"
export STATE_FILE
[ -f "$STATE_FILE" ] && source "$STATE_FILE"

save_state() {
  local key="$1"
  local value="$2"
  grep -v "^export ${key}=" "$STATE_FILE" 2>/dev/null > "${STATE_FILE}.tmp" || true
  echo "export ${key}=\"${value}\"" >> "${STATE_FILE}.tmp"
  mv "${STATE_FILE}.tmp" "$STATE_FILE"
  export "${key}=${value}"
}
