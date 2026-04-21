#!/usr/bin/env bash
# Deploy the Lambda function + SQS event source mapping.
# Idempotent: updates existing function/mapping if already present.
#
# To swap real CleverTap creds later:
#   CT_ACCOUNT_ID_VAL=... CT_PASSCODE_VAL=... CT_REGION_VAL=eu1 ./04-deploy-lambda.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=./config.sh
source "$SCRIPT_DIR/config.sh"

LAMBDA_ZIP="${REPO_ROOT}/bin/lambda.zip"
if [ ! -f "$LAMBDA_ZIP" ]; then
  echo "ERROR: $LAMBDA_ZIP not found. Run 01-build-lambda.sh first." >&2
  exit 1
fi

# Dummy defaults — override via env for real deploys.
CT_ACCOUNT_ID_VAL="${CT_ACCOUNT_ID_VAL:-DUMMY-ACCOUNT-ID}"
CT_PASSCODE_VAL="${CT_PASSCODE_VAL:-DUMMY-PASSCODE}"
CT_REGION_VAL="${CT_REGION_VAL:-us1}"
LOG_LEVEL_VAL="${LOG_LEVEL_VAL:-debug}"

ENV_VARS=$(cat <<EOF
{"Variables":{"CT_ACCOUNT_ID":"${CT_ACCOUNT_ID_VAL}","CT_PASSCODE":"${CT_PASSCODE_VAL}","CT_REGION":"${CT_REGION_VAL}","LOG_LEVEL":"${LOG_LEVEL_VAL}"}}
EOF
)

echo ">>> Deploying Lambda: $LAMBDA_NAME"
if aws lambda get-function --function-name "$LAMBDA_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
  echo "    exists — updating code + config"
  aws lambda update-function-code \
    --function-name "$LAMBDA_NAME" \
    --zip-file "fileb://${LAMBDA_ZIP}" \
    --region "$AWS_REGION" >/dev/null
  aws lambda wait function-updated --function-name "$LAMBDA_NAME" --region "$AWS_REGION"
  aws lambda update-function-configuration \
    --function-name "$LAMBDA_NAME" \
    --environment "$ENV_VARS" \
    --region "$AWS_REGION" >/dev/null
  aws lambda wait function-updated --function-name "$LAMBDA_NAME" --region "$AWS_REGION"
else
  echo "    creating new function"
  # Retry on InvalidParameterValueException — IAM role propagation can take ~10s.
  for attempt in 1 2 3 4 5; do
    if aws lambda create-function \
      --function-name "$LAMBDA_NAME" \
      --runtime provided.al2023 \
      --architectures arm64 \
      --role "$LAMBDA_ROLE_ARN" \
      --handler bootstrap \
      --zip-file "fileb://${LAMBDA_ZIP}" \
      --timeout 60 \
      --memory-size 256 \
      --environment "$ENV_VARS" \
      --region "$AWS_REGION" >/dev/null 2>&1; then
      break
    fi
    echo "    create attempt $attempt/5 failed (role may still be propagating)..."
    sleep 5
    if [ "$attempt" -eq 5 ]; then
      echo "    final attempt (with full error output):"
      aws lambda create-function \
        --function-name "$LAMBDA_NAME" \
        --runtime provided.al2023 \
        --architectures arm64 \
        --role "$LAMBDA_ROLE_ARN" \
        --handler bootstrap \
        --zip-file "fileb://${LAMBDA_ZIP}" \
        --timeout 60 \
        --memory-size 256 \
        --environment "$ENV_VARS" \
        --region "$AWS_REGION"
      exit 1
    fi
  done
  aws lambda wait function-active --function-name "$LAMBDA_NAME" --region "$AWS_REGION"
fi

echo ">>> Creating SQS event source mapping"
EXISTING_UUID=$(aws lambda list-event-source-mappings \
  --function-name "$LAMBDA_NAME" \
  --region "$AWS_REGION" \
  --query "EventSourceMappings[?EventSourceArn=='${QUEUE_ARN}'].UUID | [0]" \
  --output text)

if [ -n "$EXISTING_UUID" ] && [ "$EXISTING_UUID" != "None" ]; then
  echo "    exists (UUID: $EXISTING_UUID), skipping"
else
  aws lambda create-event-source-mapping \
    --function-name "$LAMBDA_NAME" \
    --event-source-arn "$QUEUE_ARN" \
    --batch-size 10 \
    --maximum-batching-window-in-seconds 30 \
    --function-response-types ReportBatchItemFailures \
    --scaling-config '{"MaximumConcurrency":5}' \
    --region "$AWS_REGION" >/dev/null
  echo "    created"
fi

echo ">>> Lambda ready:"
aws lambda get-function-configuration --function-name "$LAMBDA_NAME" --region "$AWS_REGION" \
  --query '[FunctionArn,State,LastUpdateStatus,Runtime,Architectures[0],MemorySize,Timeout]' \
  --output table
