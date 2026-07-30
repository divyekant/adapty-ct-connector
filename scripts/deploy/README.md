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
| 07 | `07-add-health.sh` | Unauthenticated `GET /health` (MOCK, static 200) — once per API, tenant-agnostic |

Non-deterministic IDs (API Gateway ID, authorizer ID, AUTH_TOKEN, etc.) are saved to `.state/{TENANT_ID}.env` (gitignored) and sourced by subsequent scripts.

## CleverTap credentials

`04-deploy-lambda.sh` reads CleverTap credentials from env vars and injects them as Lambda environment variables:

```bash
CT_ACCOUNT_ID_VAL=<ct-account-id> \
CT_PASSCODE_VAL=<ct-passcode> \
CT_REGION_VAL=us1 \
./04-deploy-lambda.sh
```

Defaults are dummy placeholders (`DUMMY-ACCOUNT-ID` / `DUMMY-PASSCODE`) so re-running the script without these vars will intentionally break uploads — update them explicitly when wiring a real tenant.

**Region behavior:** `CT_REGION_VAL` is used to build the endpoint `https://{region}.api.clevertap.com/1/upload`. For CleverTap EU accounts, the region-prefixed URL resolves to the same endpoint as the prefix-less one — `us1` vs. `eu1` vs. bare routing is ultimately decided by CleverTap based on the `X-CleverTap-Account-Id` header, not the URL.

**Follow-up:** move passcode to AWS Secrets Manager / SSM Parameter Store (not yet implemented; auth tokens already live in SSM).

## Dry run (onboarding)

`DRY_RUN_VAL=true ./04-deploy-lambda.sh` (with the usual `CT_*_VAL` vars) deploys the consumer in dry-run mode: events flow through parse → dedup → transform and every would-be CleverTap record is logged (`dry_run: would upload record` with the full payload), but nothing is sent to CleverTap. Point Adapty at the webhook, inspect the logs, then redeploy with `DRY_RUN_VAL=false` to go live. Note: re-running 04 always requires re-passing the `CT_*_VAL` credentials or they reset to dummy placeholders.

## Health check

`GET {WEBHOOK_BASE_ORIGIN}/health` returns `200 {"status":"ok"}` with no auth — an API Gateway MOCK integration (no Lambda, no per-probe cost). It proves the API is deployed and reachable; it does not exercise SQS or the consumer.

## Authorization (Bearer token, per tenant)

`06-add-auth.sh` attaches a shared Lambda TOKEN authorizer that is **tenant-aware**: it derives the tenant slug from the method ARN (`.../POST/ingest/{tenant}`) and validates the `Authorization: Bearer <token>` header against the SSM SecureString parameter `/adapty-ct-connector/{ENVIRONMENT}/{tenant}/auth-token`. Tokens are isolated per tenant — onboarding or rotating one tenant never affects another, and one tenant's token cannot post to another tenant's path.

Generate a token on first run for a tenant:

```bash
TENANT_ID=some-tenant AUTH_TOKEN=$(openssl rand -hex 32) ./06-add-auth.sh
```

The token is persisted to `.state/{TENANT_ID}.env` as `AUTH_TOKEN_SAVED` so the script can reuse it on subsequent runs without regeneration.

**Caching:** the API Gateway authorizer result TTL is 0 (no caching) by design, and the authorizer Lambda caches SSM lookups in memory for 15 s (`TOKEN_CACHE_TTL_SECONDS`). A rotated token takes effect within ~15 s.

### Rotate a tenant's token

```bash
TENANT_ID=some-tenant AUTH_TOKEN=$(openssl rand -hex 32) ./06-add-auth.sh
```

This overwrites that tenant's SSM parameter only. Requests using the old token start failing within the 15 s lookup-cache window.

### Test

```bash
curl -X POST "${WEBHOOK_BASE}/${TENANT_ID}" \
  -H "Authorization: Bearer ${AUTH_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"hello":"world"}'
```

## Teardown

`99-teardown.sh` (TBD) will remove all resources for a given `TENANT_ID`.
