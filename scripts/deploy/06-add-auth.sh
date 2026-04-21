#!/usr/bin/env bash
# Add a Lambda TOKEN authorizer to the POST /ingest/{ct_account_id} method.
# Validates the Authorization header against AUTH_TOKEN stored in the authorizer Lambda env.
#
# Usage:
#   AUTH_TOKEN=$(openssl rand -hex 32) ./06-add-auth.sh
#
# The token is saved to .state/${TENANT_ID}.env so follow-up scripts/tests can use it.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=./config.sh
source "$SCRIPT_DIR/config.sh"

AUTHORIZER_NAME="adapty-ct-authorizer"
AUTHORIZER_ROLE_NAME="adapty-ct-authorizer-role"
AUTHORIZER_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${AUTHORIZER_ROLE_NAME}"
AUTHORIZER_ZIP="${REPO_ROOT}/bin/authorizer.zip"

# Accept AUTH_TOKEN from env, or reuse the one saved from a previous run.
AUTH_TOKEN="${AUTH_TOKEN:-${AUTH_TOKEN_SAVED:-}}"
if [ -z "$AUTH_TOKEN" ]; then
  echo "ERROR: AUTH_TOKEN not set. Generate one with: openssl rand -hex 32" >&2
  exit 1
fi

if [ -z "${API_ID:-}" ]; then
  echo "ERROR: API_ID not found in state. Run 05-create-api-gateway.sh first." >&2
  exit 1
fi

# --- 1. Build authorizer zip ---
if [ ! -f "$AUTHORIZER_ZIP" ]; then
  echo ">>> Building authorizer zip"
  (cd "$REPO_ROOT" && make build-authorizer)
fi

# --- 2. IAM role for authorizer Lambda ---
echo ">>> IAM role for authorizer: $AUTHORIZER_ROLE_NAME"
LAMBDA_TRUST='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'
if aws iam get-role --role-name "$AUTHORIZER_ROLE_NAME" >/dev/null 2>&1; then
  echo "    exists"
else
  aws iam create-role --role-name "$AUTHORIZER_ROLE_NAME" --assume-role-policy-document "$LAMBDA_TRUST" >/dev/null
  echo "    created"
fi
aws iam attach-role-policy --role-name "$AUTHORIZER_ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

# --- 3. Deploy authorizer Lambda ---
echo ">>> Deploying authorizer Lambda: $AUTHORIZER_NAME"
ENV_VARS="{\"Variables\":{\"AUTH_TOKEN\":\"${AUTH_TOKEN}\"}}"

if aws lambda get-function --function-name "$AUTHORIZER_NAME" --region "$AWS_REGION" >/dev/null 2>&1; then
  echo "    exists — updating code + config"
  aws lambda update-function-code --function-name "$AUTHORIZER_NAME" --zip-file "fileb://${AUTHORIZER_ZIP}" --region "$AWS_REGION" >/dev/null
  aws lambda wait function-updated --function-name "$AUTHORIZER_NAME" --region "$AWS_REGION"
  aws lambda update-function-configuration --function-name "$AUTHORIZER_NAME" --environment "$ENV_VARS" --region "$AWS_REGION" >/dev/null
  aws lambda wait function-updated --function-name "$AUTHORIZER_NAME" --region "$AWS_REGION"
else
  echo "    creating new function"
  for attempt in 1 2 3 4 5; do
    if aws lambda create-function \
      --function-name "$AUTHORIZER_NAME" \
      --runtime provided.al2023 \
      --architectures arm64 \
      --role "$AUTHORIZER_ROLE_ARN" \
      --handler bootstrap \
      --zip-file "fileb://${AUTHORIZER_ZIP}" \
      --timeout 10 \
      --memory-size 128 \
      --environment "$ENV_VARS" \
      --region "$AWS_REGION" >/dev/null 2>&1; then
      break
    fi
    echo "    create attempt $attempt/5 failed (role may still be propagating)..."
    sleep 5
    if [ "$attempt" -eq 5 ]; then
      aws lambda create-function \
        --function-name "$AUTHORIZER_NAME" \
        --runtime provided.al2023 \
        --architectures arm64 \
        --role "$AUTHORIZER_ROLE_ARN" \
        --handler bootstrap \
        --zip-file "fileb://${AUTHORIZER_ZIP}" \
        --timeout 10 \
        --memory-size 128 \
        --environment "$ENV_VARS" \
        --region "$AWS_REGION"
      exit 1
    fi
  done
  aws lambda wait function-active --function-name "$AUTHORIZER_NAME" --region "$AWS_REGION"
fi

AUTHORIZER_LAMBDA_ARN=$(aws lambda get-function --function-name "$AUTHORIZER_NAME" --region "$AWS_REGION" --query 'Configuration.FunctionArn' --output text)

# --- 4. Allow API Gateway to invoke the authorizer ---
echo ">>> Granting API Gateway permission to invoke authorizer"
aws lambda add-permission \
  --function-name "$AUTHORIZER_NAME" \
  --statement-id "apigw-invoke-authorizer" \
  --action lambda:InvokeFunction \
  --principal apigateway.amazonaws.com \
  --source-arn "arn:aws:execute-api:${AWS_REGION}:${AWS_ACCOUNT_ID}:${API_ID}/authorizers/*" \
  --region "$AWS_REGION" >/dev/null 2>&1 || echo "    permission already granted"

# --- 5. Create or update API Gateway authorizer ---
echo ">>> API Gateway authorizer"
AUTHORIZER_URI="arn:aws:apigateway:${AWS_REGION}:lambda:path/2015-03-31/functions/${AUTHORIZER_LAMBDA_ARN}/invocations"

AUTHORIZER_ID=$(aws apigateway get-authorizers --rest-api-id "$API_ID" --region "$AWS_REGION" \
  --query "items[?name=='${AUTHORIZER_NAME}'].id | [0]" --output text)

if [ -z "$AUTHORIZER_ID" ] || [ "$AUTHORIZER_ID" = "None" ]; then
  AUTHORIZER_ID=$(aws apigateway create-authorizer \
    --rest-api-id "$API_ID" \
    --name "$AUTHORIZER_NAME" \
    --type TOKEN \
    --authorizer-uri "$AUTHORIZER_URI" \
    --identity-source "method.request.header.Authorization" \
    --authorizer-result-ttl-in-seconds 300 \
    --region "$AWS_REGION" \
    --query 'id' --output text)
  echo "    created: $AUTHORIZER_ID"
else
  echo "    exists: $AUTHORIZER_ID (updating URI in case Lambda ARN changed)"
  aws apigateway update-authorizer \
    --rest-api-id "$API_ID" \
    --authorizer-id "$AUTHORIZER_ID" \
    --patch-operations "op=replace,path=/authorizerUri,value=${AUTHORIZER_URI}" \
    --region "$AWS_REGION" >/dev/null
fi
save_state AUTHORIZER_ID "$AUTHORIZER_ID"

# --- 6. Attach authorizer to POST /ingest/{ct_account_id} ---
echo ">>> Attaching authorizer to POST method"
TENANT_RES_ID=$(aws apigateway get-resources --rest-api-id "$API_ID" --region "$AWS_REGION" \
  --query "items[?path=='/ingest/{ct_account_id}'].id | [0]" --output text)

aws apigateway update-method \
  --rest-api-id "$API_ID" \
  --resource-id "$TENANT_RES_ID" \
  --http-method POST \
  --patch-operations \
    "op=replace,path=/authorizationType,value=CUSTOM" \
    "op=replace,path=/authorizerId,value=${AUTHORIZER_ID}" \
  --region "$AWS_REGION" >/dev/null

# --- 7. Redeploy stage ---
echo ">>> Redeploying stage: $ENVIRONMENT"
aws apigateway create-deployment \
  --rest-api-id "$API_ID" \
  --stage-name "$ENVIRONMENT" \
  --description "Add authorizer $(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --region "$AWS_REGION" >/dev/null

# --- 8. Save token for test convenience (gitignored) ---
save_state AUTH_TOKEN_SAVED "$AUTH_TOKEN"

echo ""
echo ">>> Authorizer attached. Test with:"
echo "    curl -X POST \"${WEBHOOK_BASE}/${TENANT_ID}\" \\"
echo "      -H \"Authorization: Bearer ${AUTH_TOKEN}\" \\"
echo "      -H \"Content-Type: application/json\" \\"
echo "      -d '{\"hello\":\"world\"}'"
