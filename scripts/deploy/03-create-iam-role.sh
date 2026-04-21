#!/usr/bin/env bash
# Create IAM execution role for the Lambda consumer.
# Permissions: CloudWatch Logs (managed policy) + SQS access (inline, scoped to this tenant).

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
source "$SCRIPT_DIR/config.sh"

TRUST_POLICY='{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "lambda.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}'

echo ">>> Creating IAM role: $LAMBDA_ROLE_NAME"
if aws iam get-role --role-name "$LAMBDA_ROLE_NAME" >/dev/null 2>&1; then
  echo "    already exists, skipping create"
else
  aws iam create-role \
    --role-name "$LAMBDA_ROLE_NAME" \
    --assume-role-policy-document "$TRUST_POLICY" >/dev/null
  echo "    created"
fi

echo ">>> Attaching AWSLambdaBasicExecutionRole (CloudWatch Logs)"
aws iam attach-role-policy \
  --role-name "$LAMBDA_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

SQS_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes"
    ],
    "Resource": "${QUEUE_ARN}"
  }]
}
EOF
)

echo ">>> Attaching inline SQS policy (scoped to $QUEUE_NAME)"
aws iam put-role-policy \
  --role-name "$LAMBDA_ROLE_NAME" \
  --policy-name "sqs-access" \
  --policy-document "$SQS_POLICY"

echo ">>> Role ready:"
echo "    ARN: $LAMBDA_ROLE_ARN"
