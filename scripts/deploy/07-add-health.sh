#!/usr/bin/env bash
# Add an unauthenticated GET /health endpoint (API Gateway MOCK integration).
# Returns a static 200 {"status":"ok"} — proves the API is deployed and
# reachable for external monitors and webhook-endpoint validation. No Lambda,
# no per-probe cost. Tenant-agnostic: run once per API, idempotent.
#
# Usage:
#   ./07-add-health.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./config.sh
source "$SCRIPT_DIR/config.sh"

if [ -z "${API_ID:-}" ]; then
  echo "ERROR: API_ID not found in state. Run 05-create-api-gateway.sh first." >&2
  exit 1
fi

# --- 1. /health resource ---
ROOT_ID=$(aws apigateway get-resources --rest-api-id "$API_ID" --region "$AWS_REGION" --query "items[?path=='/'].id | [0]" --output text)

HEALTH_ID=$(aws apigateway get-resources --rest-api-id "$API_ID" --region "$AWS_REGION" --query "items[?path=='/health'].id | [0]" --output text)
if [ -z "$HEALTH_ID" ] || [ "$HEALTH_ID" = "None" ]; then
  HEALTH_ID=$(aws apigateway create-resource --rest-api-id "$API_ID" --parent-id "$ROOT_ID" --path-part "health" --region "$AWS_REGION" --query 'id' --output text)
  echo ">>> Created /health: $HEALTH_ID"
else
  echo ">>> /health exists: $HEALTH_ID"
fi

# --- 2. GET method (no auth) + MOCK integration ---
echo ">>> GET method on /health (MOCK integration)"
aws apigateway put-method \
  --rest-api-id "$API_ID" \
  --resource-id "$HEALTH_ID" \
  --http-method GET \
  --authorization-type NONE \
  --region "$AWS_REGION" >/dev/null

aws apigateway put-integration \
  --rest-api-id "$API_ID" \
  --resource-id "$HEALTH_ID" \
  --http-method GET \
  --type MOCK \
  --request-templates '{"application/json":"{\"statusCode\": 200}"}' \
  --region "$AWS_REGION" >/dev/null

aws apigateway put-method-response \
  --rest-api-id "$API_ID" \
  --resource-id "$HEALTH_ID" \
  --http-method GET \
  --status-code 200 \
  --response-models '{"application/json":"Empty"}' \
  --region "$AWS_REGION" >/dev/null

aws apigateway put-integration-response \
  --rest-api-id "$API_ID" \
  --resource-id "$HEALTH_ID" \
  --http-method GET \
  --status-code 200 \
  --response-templates '{"application/json":"{\"status\":\"ok\"}"}' \
  --region "$AWS_REGION" >/dev/null

# --- 3. Redeploy stage ---
echo ">>> Redeploying stage: $ENVIRONMENT"
aws apigateway create-deployment \
  --rest-api-id "$API_ID" \
  --stage-name "$ENVIRONMENT" \
  --description "Add /health $(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --region "$AWS_REGION" >/dev/null

HEALTH_URL="https://${API_ID}.execute-api.${AWS_REGION}.amazonaws.com/${ENVIRONMENT}/health"
echo ""
echo ">>> Health endpoint live:"
echo "    curl ${HEALTH_URL}"
