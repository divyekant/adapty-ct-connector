#!/bin/bash
set -euo pipefail

QUEUE_URL="${QUEUE_URL:-http://localhost:4566/000000000000/adapty-ct-test}"
ENDPOINT="${ENDPOINT:-http://localhost:4566}"
COUNT="${1:-10}"

TESTDATA_FILES=(testdata/*.json)

if [ ${#TESTDATA_FILES[@]} -eq 0 ]; then
  echo "Error: no JSON files found in testdata/" >&2
  exit 1
fi

echo "Seeding $COUNT iterations of ${#TESTDATA_FILES[@]} payload file(s) to $QUEUE_URL"

for i in $(seq 1 "$COUNT"); do
  for f in "${TESTDATA_FILES[@]}"; do
    BODY=$(cat "$f")
    aws --endpoint-url "$ENDPOINT" sqs send-message \
      --queue-url "$QUEUE_URL" \
      --message-body "$BODY" \
      --output text --query 'MessageId' | xargs -I{} echo "  [$i] sent $f (MessageId: {})"
  done
done

echo "Done seeding queue."
