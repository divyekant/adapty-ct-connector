#!/usr/bin/env bash
# Create per-tenant SQS main queue and DLQ with redrive policy.
# - DLQ: 14-day retention
# - Main: 14-day retention, 5-min visibility, long-polling, DLQ after 5 retries

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
source "$SCRIPT_DIR/config.sh"

echo ">>> Creating DLQ: $DLQ_NAME"
if aws sqs get-queue-url --queue-name "$DLQ_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
  echo "    already exists, skipping"
else
  aws sqs create-queue \
    --queue-name "$DLQ_NAME" \
    --attributes '{"MessageRetentionPeriod":"1209600"}' \
    --region "$AWS_REGION" >/dev/null
  echo "    created"
fi

echo ">>> Creating main queue: $QUEUE_NAME"
MAIN_ATTRS=$(cat <<EOF
{
  "MessageRetentionPeriod": "1209600",
  "VisibilityTimeout": "300",
  "ReceiveMessageWaitTimeSeconds": "20",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"5\"}"
}
EOF
)

if aws sqs get-queue-url --queue-name "$QUEUE_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
  echo "    already exists, updating attributes"
  aws sqs set-queue-attributes \
    --queue-url "$QUEUE_URL" \
    --attributes "$MAIN_ATTRS" \
    --region "$AWS_REGION" >/dev/null
else
  aws sqs create-queue \
    --queue-name "$QUEUE_NAME" \
    --attributes "$MAIN_ATTRS" \
    --region "$AWS_REGION" >/dev/null
  echo "    created"
fi

echo ">>> Queues ready:"
echo "    Main URL: $QUEUE_URL"
echo "    Main ARN: $QUEUE_ARN"
echo "    DLQ ARN:  $DLQ_ARN"
