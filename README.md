# Adapty to CleverTap Connector

A multi-tenant webhook connector that receives [Adapty](https://adapty.io) subscription events and forwards them to [CleverTap](https://clevertap.com)'s Upload Events API.

## Architecture

```
Adapty Webhooks
    |
    v
API Gateway (POST /ingest/{ct_account_id})
    |  -- validates Authorization header
    |  -- routes to per-account SQS queue
    v
SQS Queue (per account)
    |  -- standard queue, 14-day retention
    |  -- DLQ after 5 failed attempts
    v
Fargate Service (per account, Go)
    |  -- long-polls SQS
    |  -- transforms Adapty -> CleverTap events
    |  -- retries transient errors (3x backoff)
    |  -- structured JSON logging
    v
CleverTap Upload Events API
```

Each CleverTap account gets its own SQS queue + DLQ + Fargate task for complete data isolation.

## Supported Events

All 17 Adapty webhook event types are supported:

| Category | Events |
|---|---|
| Subscriptions | `subscription_started`, `subscription_renewed`, `subscription_renewal_cancelled`, `subscription_renewal_reactivated`, `subscription_expired`, `subscription_paused`, `subscription_deferred`, `subscription_refunded` |
| Purchases | `non_subscription_purchase`, `non_subscription_purchase_refunded` |
| Trials | `trial_started`, `trial_converted`, `trial_renewal_cancelled`, `trial_renewal_reactivated`, `trial_expired` |
| Other | `entered_grace_period`, `billing_issue_detected`, `access_level_updated` |

Unknown future event types are forwarded automatically as `Adapty {event_type}`.

## Data Transformation

The connector flattens the Adapty webhook payload into CleverTap's `evtData` across 7 configurable layers:

| Layer | Source | Prefix | Example Key |
|---|---|---|---|
| 1. Top-level | Root fields | *(none)* | `profile_id`, `email` |
| 2. Event Properties | `event_properties` | *(none)* | `store`, `price_usd`, `vendor_product_id` |
| 3. Attributions | `attributions.{source}` | `attribution_{source}_` | `attribution_appsflyer_campaign` |
| 4. User Attributes | `user_attributes` | `user_attr_` | `user_attr_tier` |
| 5. Integration IDs | `integration_ids` | `integration_` | `integration_firebase_app_instance_id` |
| 6. Play Store Token | `play_store_purchase_token` | `play_store_` | `play_store_product_id` |
| 7. Profiles Sharing | `profiles_sharing_access_level` | *(none)* | `profiles_sharing_access_level` (JSON string) |

Layer 2 overrides Layer 1 on key collision. Null values are omitted. Any layer or individual field can be excluded via configuration.

### Identity Resolution

- Primary: `customer_user_id` from Adapty payload
- Fallback: `profile_id` (for anonymous users)

### Timestamp

The top-level `event_datetime` is converted to UNIX epoch seconds for CleverTap's `ts` field, ensuring correct event timing even for delayed, retried, or backfilled events.

## Configuration

### Environment Variables (Fargate)

| Variable | Required | Description |
|---|---|---|
| `CT_ACCOUNT_ID` | Yes | CleverTap account ID |
| `CT_PASSCODE` | Yes | CleverTap account passcode |
| `CT_REGION` | Yes | CleverTap region (`eu1`, `us1`, `in1`, `sg1`, `mec1`) |
| `SQS_QUEUE_URL` | Yes | SQS queue URL to poll |
| `LOG_LEVEL` | No | `debug` / `info` / `warn` / `error` (default: `info`) |
| `DRY_RUN` | No | `true` = log events without sending to CleverTap |
| `TRANSFORM_CONFIG_PATH` | No | Path to field exclusion config JSON |
| `BATCH_SIZE` | No | Messages per CleverTap API call (default: 10, max: 10) |
| `DEDUP_LRU_SIZE` | No | Dedup cache size (default: 100000) |
| `CT_BASE_URL` | No | Override CleverTap URL (for local dev) |
| `SQS_ENDPOINT` | No | Override SQS endpoint (for LocalStack) |

### Transform Config

Disable entire layers or exclude specific fields without code changes:

```json
{
  "disabled_layers": ["play_store", "profiles_sharing"],
  "excluded_fields": {
    "top_level": ["user_agent"],
    "event_properties": ["profile_ip_address"]
  }
}
```

Valid layer names: `top_level`, `event_properties`, `attributions`, `user_attributes`, `integration_ids`, `play_store`, `profiles_sharing`

## Error Handling

| Scenario | Action |
|---|---|
| CleverTap 200 + `success` | Delete message from SQS |
| CleverTap 200 + `partial` | Delete successful records; leave failed ones for SQS redelivery |
| CleverTap 429 / 5xx | Retry with exponential backoff (1s, 2s, 4s), max 3 attempts |
| CleverTap 400 | Log error, no retry (payload issue) |
| CleverTap 401/403 | Log critical alert, stop processing (credential issue) |
| Malformed SQS message | Leave for SQS redelivery to DLQ |
| Empty `event_type` | Delete silently (Adapty verification request) |
| Duplicate `profile_event_id` | Delete (dedup via in-memory LRU cache) |

## Backfill CLI

Upload historical data from NDJSON files:

```bash
./backfill \
  --ct-account-id=XXX \
  --ct-passcode=YYY \
  --ct-region=eu1 \
  --input=adapty_export.ndjson \
  --batch-size=500 \
  --concurrency=5 \
  --transform-config=transform-config.json \
  --dry-run
```

Input format: one Adapty webhook payload per line (NDJSON). Supports `--offset` for resuming.

## Local Development

### Prerequisites

- Go 1.25+
- Docker (OrbStack recommended)

### Run Tests

```bash
go test ./... -v
```

### Run Locally with Docker Compose

```bash
docker compose up -d --build

# Wait for LocalStack to initialize (~15s), then:
# Send a test event (requires aws CLI or use curl):
curl -s "http://localhost:4566/000000000000/adapty-ct-test?Action=SendMessage&MessageBody=$(python3 -c 'import urllib.parse; print(urllib.parse.quote(open("testdata/subscription_started.json").read()))')"

# Check connector logs:
docker compose logs connector | grep "event processed"

# Check mock CleverTap received it:
docker compose logs mock-clevertap | grep "received event"

# Stop:
docker compose down
```

### Docker Compose Services

| Service | Port | Purpose |
|---|---|---|
| `localstack` | 4566 | SQS mock |
| `mock-clevertap` | 8089 | Validates and logs received CleverTap payloads |
| `connector` | 8080 (internal) | The connector service (polls SQS, uploads to mock CT) |

## Project Structure

```
adapty-ct-connector/
├── cmd/
│   ├── connector/       # Fargate SQS consumer entrypoint
│   ├── backfill/        # Backfill CLI tool
│   └── mock-clevertap/  # Local dev mock CleverTap server
├── internal/
│   ├── adapty/          # Adapty webhook payload types
│   ├── clevertap/       # CleverTap API client + types
│   ├── transform/       # 7-layer Adapty -> CleverTap mapping
│   └── queue/           # SQS consumer + adapter
├── testdata/            # Sample Adapty webhook payloads
├── scripts/             # Local dev test scripts
├── docs/
│   └── architecture.md  # Infra team handoff document
├── Dockerfile           # Production multi-stage build
├── Dockerfile.mock      # Mock CleverTap server build
├── docker-compose.yml   # Local dev stack
└── transform-config.json
```

## Deployment

See [`docs/architecture.md`](docs/architecture.md) for the full infra team handoff document covering:

- API Gateway setup (path routing, auth validation, SQS integration)
- Per-account provisioning (SQS + DLQ + Fargate task)
- CloudWatch alarms and monitoring
- Adapty webhook configuration
- Scaling guidance

### Docker Image

```bash
docker build -t adapty-ct-connector .
```

The image contains both `connector` (entrypoint) and `backfill` binaries.

## Health Check

The connector exposes `/healthz` on port 8080:
- `200 OK` if last successful SQS poll was < 60 seconds ago
- `503 Service Unavailable` if stale

## Logging

Structured JSON logs to stdout (CloudWatch Logs compatible):

```json
{"time":"...","level":"INFO","msg":"queue: event processed","event_type":"subscription_renewed","identity":"user_123","profile_event_id":"evt-789","status":"success","latency_ms":3}
```
