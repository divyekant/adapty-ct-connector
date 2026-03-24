#!/bin/bash
set -euo pipefail

QUEUE_URL="${QUEUE_URL:-http://localhost:4566/000000000000/adapty-ct-test}"
ENDPOINT="${ENDPOINT:-http://localhost:4566}"
PAYLOAD_FILE="${1:-testdata/subscription_started.json}"

if [ ! -f "$PAYLOAD_FILE" ]; then
  echo "Error: payload file not found: $PAYLOAD_FILE" >&2
  exit 1
fi

BODY=$(cat "$PAYLOAD_FILE")

aws --endpoint-url "$ENDPOINT" sqs send-message \
  --queue-url "$QUEUE_URL" \
  --message-body "$BODY"

echo "Sent $PAYLOAD_FILE to $QUEUE_URL"
