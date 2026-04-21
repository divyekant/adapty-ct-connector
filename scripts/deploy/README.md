# Deploy Scripts (Lambda mode, AWS CLI)

Idempotent, committable shell scripts for provisioning the adapty-ct-connector on AWS.

## Prerequisites

- AWS CLI v2 configured (`aws sts get-caller-identity` works)
- Go 1.25+ (for `make build-lambda` and `make build-authorizer`)
- Bash 4+

## Configuration

Defaults live in `config.sh`. Override via env vars:

| Variable | Default | Notes |
|---|---|---|
| `AWS_REGION` | `us-east-1` | |
| `AWS_ACCOUNT_ID` | `009910375489` | |
| `ENVIRONMENT` | `prod` | API GW stage name |
| `TENANT_ID` | `test-tenant` | Per-tenant identifier |

For a second tenant:
```bash
TENANT_ID=real-tenant ./02-create-sqs.sh
TENANT_ID=real-tenant ./04-deploy-lambda.sh
# ...etc.
```

## Script order

| # | Script | Creates |
|---|---|---|
| 01 | `01-build-lambda.sh` | `bin/lambda.zip` (linux/arm64) |
| 02 | `02-create-sqs.sh` | SQS main queue + DLQ |
| 03 | `03-create-iam-role.sh` | Lambda execution role |
| 04 | `04-deploy-lambda.sh` | Lambda function + SQS event source mapping |
| 05 | `05-create-api-gateway.sh` | API GW REST → SQS direct integration |
| 06 | `06-add-auth.sh` | TOKEN authorizer on `POST /ingest/{ct_account_id}` |

Non-deterministic IDs (API Gateway ID, authorizer ID, AUTH_TOKEN, etc.) are saved to `.state/{TENANT_ID}.env` (gitignored) and sourced by subsequent scripts.

## CleverTap credentials

`04-deploy-lambda.sh` reads CleverTap credentials from env vars and injects them as Lambda environment variables:

```bash
CT_ACCOUNT_ID_VAL=W67-774-7Z5Z \
CT_PASSCODE_VAL=<passcode> \
CT_REGION_VAL=us1 \
./04-deploy-lambda.sh
```

Defaults are dummy placeholders (`DUMMY-ACCOUNT-ID` / `DUMMY-PASSCODE`) so re-running the script without these vars will intentionally break uploads — update them explicitly when wiring a real tenant.

**Region behavior:** `CT_REGION_VAL` is used to build the endpoint `https://{region}.api.clevertap.com/1/upload`. For CleverTap EU accounts, the region-prefixed URL resolves to the same endpoint as the prefix-less one — `us1` vs. `eu1` vs. bare routing is ultimately decided by CleverTap based on the `X-CleverTap-Account-Id` header, not the URL.

**Follow-up:** move passcode to AWS Secrets Manager / SSM Parameter Store (not yet implemented).

## Authorization (Bearer token)

`06-add-auth.sh` attaches a Lambda TOKEN authorizer that validates the `Authorization: Bearer <token>` header against `AUTH_TOKEN` in the authorizer Lambda's env.

Generate a token on first run:

```bash
AUTH_TOKEN=$(openssl rand -hex 32) ./06-add-auth.sh
```

The token is persisted to `.state/{TENANT_ID}.env` as `AUTH_TOKEN_SAVED` so the script can reuse it on subsequent runs without regeneration.

**Cache TTL is 0** (no caching) by design: tokens rotate without waiting for a cached result to expire.

### Rotate the token

```bash
AUTH_TOKEN=$(openssl rand -hex 32) ./06-add-auth.sh
```

This updates the Lambda env var in place. Any request using the old token starts failing as soon as the Lambda update propagates (typically seconds).

### Test

```bash
curl -X POST "${WEBHOOK_BASE}/${TENANT_ID}" \
  -H "Authorization: Bearer ${AUTH_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"hello":"world"}'
```

## Teardown

`99-teardown.sh` (TBD) will remove all resources for a given `TENANT_ID`.
