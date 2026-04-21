#!/usr/bin/env bash
# Create API Gateway REST API with direct SQS integration.
# Routes: POST /ingest/{ct_account_id} → SQS queue adapty-ct-{ct_account_id}
# NOTE: No auth yet. Add via Lambda authorizer in a follow-up (06-add-auth.sh).

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
source "$SCRIPT_DIR/config.sh"

API_NAME="adapty-ct-connector"
API_GW_ROLE_NAME="adapty-ct-apigw-sqs-role"
STAGE_NAME="${ENVIRONMENT}"

# --- 1. IAM role for API Gateway → SQS ---
echo ">>> IAM role for API Gateway → SQS: $API_GW_ROLE_NAME"
API_GW_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"apigateway.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

if aws iam get-role --role-name "$API_GW_ROLE_NAME" >/dev/null 2>&1; then
  echo "    exists"
else
  aws iam create-role --role-name "$API_GW_ROLE_NAME" --assume-role-policy-document "$API_GW_TRUST" >/dev/null
  echo "    created"
fi

SQS_SEND_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "sqs:SendMessage",
    "Resource": "arn:aws:sqs:${AWS_REGION}:${AWS_ACCOUNT_ID}:adapty-ct-*"
  }]
}
EOF
)
aws iam put-role-policy --role-name "$API_GW_ROLE_NAME" --policy-name "sqs-send" --policy-document "$SQS_SEND_POLICY"
API_GW_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${API_GW_ROLE_NAME}"

# --- 2. REST API ---
echo ">>> REST API: $API_NAME"
API_ID=$(aws apigateway get-rest-apis --region "$AWS_REGION" --query "items[?name=='${API_NAME}'].id | [0]" --output text)
if [ -z "$API_ID" ] || [ "$API_ID" = "None" ]; then
  API_ID=$(aws apigateway create-rest-api --name "$API_NAME" --region "$AWS_REGION" --endpoint-configuration "types=REGIONAL" --query 'id' --output text)
  echo "    created: $API_ID"
else
  echo "    exists: $API_ID"
fi
save_state API_ID "$API_ID"

# --- 3. Resources: /ingest/{ct_account_id} ---
ROOT_ID=$(aws apigateway get-resources --rest-api-id "$API_ID" --region "$AWS_REGION" --query "items[?path=='/'].id | [0]" --output text)

INGEST_ID=$(aws apigateway get-resources --rest-api-id "$API_ID" --region "$AWS_REGION" --query "items[?path=='/ingest'].id | [0]" --output text)
if [ -z "$INGEST_ID" ] || [ "$INGEST_ID" = "None" ]; then
  INGEST_ID=$(aws apigateway create-resource --rest-api-id "$API_ID" --parent-id "$ROOT_ID" --path-part "ingest" --region "$AWS_REGION" --query 'id' --output text)
  echo ">>> Created /ingest: $INGEST_ID"
fi

TENANT_RES_ID=$(aws apigateway get-resources --rest-api-id "$API_ID" --region "$AWS_REGION" --query "items[?path=='/ingest/{ct_account_id}'].id | [0]" --output text)
if [ -z "$TENANT_RES_ID" ] || [ "$TENANT_RES_ID" = "None" ]; then
  TENANT_RES_ID=$(aws apigateway create-resource --rest-api-id "$API_ID" --parent-id "$INGEST_ID" --path-part "{ct_account_id}" --region "$AWS_REGION" --query 'id' --output text)
  echo ">>> Created /ingest/{ct_account_id}: $TENANT_RES_ID"
fi

# --- 4. POST method ---
echo ">>> POST method on /ingest/{ct_account_id}"
aws apigateway put-method \
  --rest-api-id "$API_ID" \
  --resource-id "$TENANT_RES_ID" \
  --http-method POST \
  --authorization-type NONE \
  --request-parameters "method.request.path.ct_account_id=true" \
  --region "$AWS_REGION" >/dev/null 2>&1 || echo "    already configured"

# --- 5. Integration: POST → SQS SendMessage (direct AWS service integration) ---
TMP_PARAMS=$(mktemp)
cat > "$TMP_PARAMS" <<'EOF'
{
  "integration.request.path.ctAccountId": "method.request.path.ct_account_id",
  "integration.request.header.Content-Type": "'application/x-www-form-urlencoded'"
}
EOF

TMP_TEMPLATES=$(mktemp)
cat > "$TMP_TEMPLATES" <<'EOF'
{
  "application/json": "Action=SendMessage&MessageBody=$util.urlEncode($input.body)"
}
EOF

aws apigateway put-integration \
  --rest-api-id "$API_ID" \
  --resource-id "$TENANT_RES_ID" \
  --http-method POST \
  --type AWS \
  --integration-http-method POST \
  --uri "arn:aws:apigateway:${AWS_REGION}:sqs:path/${AWS_ACCOUNT_ID}/adapty-ct-{ctAccountId}" \
  --credentials "$API_GW_ROLE_ARN" \
  --request-parameters "file://$TMP_PARAMS" \
  --request-templates "file://$TMP_TEMPLATES" \
  --passthrough-behavior WHEN_NO_MATCH \
  --region "$AWS_REGION" >/dev/null

rm -f "$TMP_PARAMS" "$TMP_TEMPLATES"

# --- 6. Method response ---
aws apigateway put-method-response \
  --rest-api-id "$API_ID" \
  --resource-id "$TENANT_RES_ID" \
  --http-method POST \
  --status-code 200 \
  --response-models '{"application/json":"Empty"}' \
  --region "$AWS_REGION" >/dev/null 2>&1 || true

# --- 7. Integration response ---
aws apigateway put-integration-response \
  --rest-api-id "$API_ID" \
  --resource-id "$TENANT_RES_ID" \
  --http-method POST \
  --status-code 200 \
  --selection-pattern "" \
  --response-templates '{"application/json":"{\"status\":\"enqueued\"}"}' \
  --region "$AWS_REGION" >/dev/null 2>&1 || true

# --- 8. Deploy to stage ---
echo ">>> Deploying to stage: $STAGE_NAME"
aws apigateway create-deployment \
  --rest-api-id "$API_ID" \
  --stage-name "$STAGE_NAME" \
  --description "Deploy $(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --region "$AWS_REGION" >/dev/null

WEBHOOK_BASE="https://${API_ID}.execute-api.${AWS_REGION}.amazonaws.com/${STAGE_NAME}/ingest"
save_state WEBHOOK_BASE "$WEBHOOK_BASE"

echo ""
echo ">>> API Gateway deployed:"
echo "    API ID:       $API_ID"
echo "    Stage:        $STAGE_NAME"
echo "    Webhook URL:  ${WEBHOOK_BASE}/${TENANT_ID}"
echo "    Pattern:      ${WEBHOOK_BASE}/{ct_account_id}"
