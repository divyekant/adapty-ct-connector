# Adapty to CleverTap Connector — Design Spec

## Overview

A multi-tenant webhook connector that receives Adapty subscription events and forwards them to CleverTap's Upload Events API. Designed for per-account data isolation, configurable field mapping, and resilient delivery.

**Scope:** All Adapty webhook event types → CleverTap events. User profile updates are out of scope.

## Architecture

```
Adapty Webhooks
    │
    ▼
API Gateway (POST /ingest/{ct_account_id})
    │  ── validates Authorization header (Adapty webhook secret)
    │  ── routes to per-account SQS queue
    ▼
SQS Queue (per account)
    │  ── standard queue, 14-day retention
    │  ── DLQ after 5 failed processing attempts
    ▼
Fargate Service (per account, Go)
    │  ── long-polls SQS
    │  ── transforms Adapty event → CleverTap event
    │  ── retries transient errors (3x exponential backoff)
    │  ── structured JSON logging (success counts + failure details)
    ▼
CleverTap Upload Events API
```

### Per-Account Isolation

Each CleverTap account gets its own:
- SQS queue (named `adapty-ct-{account_id}`)
- SQS Dead Letter Queue (named `adapty-ct-{account_id}-dlq`)
- Fargate task with credentials as environment variables

Adding a new account = deploying a new queue + Fargate task with different env vars.

### Fargate Environment Variables

| Variable | Description |
|---|---|
| `CT_ACCOUNT_ID` | CleverTap account ID |
| `CT_PASSCODE` | CleverTap account passcode |
| `CT_REGION` | CleverTap region (e.g., `eu1`, `us1`, `in1`, `sg1`) |
| `SQS_QUEUE_URL` | SQS queue URL to poll |
| `LOG_LEVEL` | Logging verbosity (`debug`, `info`, `warn`, `error`) |
| `DRY_RUN` | If `true`, logs transformed events without sending to CleverTap |
| `TRANSFORM_CONFIG_PATH` | Optional path to transformation config file (for field exclusions) |
| `BATCH_SIZE` | Number of SQS messages to batch into a single CleverTap API call (default: 10, max: 1000) |
| `DEDUP_LRU_SIZE` | Size of in-memory deduplication LRU cache (default: 100000) |

### Authentication

Adapty sends a configurable `Authorization` header on every webhook request. This is set in the Adapty Dashboard under Integrations → Webhooks → "Authorization header value for production endpoint". Adapty sends the value verbatim — no modifications.

**Infra team responsibility:**
1. Configure API Gateway to validate the `Authorization` header against an expected value (stored in SSM Parameter Store or API Gateway API keys)
2. Reject requests with invalid/missing auth before they reach SQS
3. Adapty sends a verification request when the webhook is first saved — must return 2xx

**Adapty verification request:** When the webhook is first saved, Adapty sends an empty JSON body `{}` and expects a 2xx. API Gateway should return 200 directly for empty/verification payloads without forwarding to SQS. The consumer should also silently drop messages without an `event_type` as a safety net.

**Adapty retry behavior:** Adapty retries failed webhook deliveries up to 9 times over 24 hours with exponential backoff for non-2xx responses. Since API Gateway returns 200 immediately after writing to SQS, Adapty retries should not fire in normal operation. However, if API Gateway returns 5xx (e.g., SQS throttled), Adapty will re-send the same webhook. The `profile_event_id` deduplication handles these at the consumer level.

**Adapty constraint:** Webhook responses must return within 10 seconds. Since API Gateway → SQS is near-instant, this is not a concern.

### CleverTap API

**Endpoint:** `https://{CT_REGION}.api.clevertap.com/1/upload`

Region mapping:
| `CT_REGION` | API Host |
|---|---|
| `eu1` | `eu1.api.clevertap.com` |
| `us1` | `us1.api.clevertap.com` |
| `in1` | `in1.api.clevertap.com` |
| `sg1` | `sg1.api.clevertap.com` |
| `mec1` | `mec1.api.clevertap.com` |

**Required request headers:**
```
X-CleverTap-Account-Id: {CT_ACCOUNT_ID}
X-CleverTap-Passcode: {CT_PASSCODE}
Content-Type: application/json; charset=utf-8
```

**Rate limit:** 15 concurrent requests per account. The Fargate consumer sends sequentially (one batch at a time), so this is not a concern. The backfill CLI must limit concurrency to stay under this cap (default: 5 concurrent requests).

**Property limits:** CleverTap enforces max 120-character event key names, max 512-byte property values, max 256 properties per event type, and max 512 event types per account. The "forward everything" default should stay well under 256 properties for typical Adapty payloads. If exceeded, CleverTap silently drops excess properties. The transform config exclusion mechanism can be used to prune properties if a specific account hits this limit.

## Data Transformation

### Identity Resolution

- Primary: `customer_user_id` → CleverTap `identity`
- Fallback: `profile_id` (when `customer_user_id` is null or empty)

### Event Name Mapping

| Adapty `event_type` | CleverTap `evtName` |
|---|---|
| `subscription_started` | `Subscription Started` |
| `subscription_renewed` | `Subscription Renewed` |
| `subscription_renewal_cancelled` | `Subscription Renewal Cancelled` |
| `subscription_renewal_reactivated` | `Subscription Renewal Reactivated` |
| `subscription_expired` | `Subscription Expired` |
| `subscription_paused` | `Subscription Paused` |
| `subscription_deferred` | `Subscription Deferred` |
| `subscription_refunded` | `Subscription Refunded` |
| `non_subscription_purchase` | `Non Subscription Purchase` |
| `non_subscription_purchase_refunded` | `Non Subscription Purchase Refunded` |
| `trial_started` | `Trial Started` |
| `trial_converted` | `Trial Converted` |
| `trial_renewal_cancelled` | `Trial Renewal Cancelled` |
| `trial_renewal_reactivated` | `Trial Renewal Reactivated` |
| `trial_expired` | `Trial Expired` |
| `entered_grace_period` | `Entered Grace Period` |
| `billing_issue_detected` | `Billing Issue Detected` |
| `access_level_updated` | `Access Level Updated` |
| *(unknown future)* | `Adapty {event_type}` |

### Payload Transformation

The Adapty webhook payload is flattened into CleverTap's `evtData` across 7 layers. Each layer can be disabled entirely, or individual fields within a layer can be excluded via configuration.

#### Transformation Config

```json
{
  "disabled_layers": ["play_store"],
  "excluded_fields": {
    "top_level": ["user_agent"],
    "event_properties": ["profile_ip_address"]
  }
}
```

- `disabled_layers`: Array of layer names to skip entirely. Valid values: `top_level`, `event_properties`, `attributions`, `user_attributes`, `integration_ids`, `play_store`, `profiles_sharing`
- `excluded_fields`: Map of layer name → field names to exclude from that layer

If no config file is provided, all layers and all fields are included (default: forward everything).

#### Field Collision Rules

Some fields exist in both top-level and `event_properties` (e.g., `profile_id`, `event_datetime`). Rule: **Layer 2 (event_properties) wins.** If a field exists in both layers, the `event_properties` value is used. In practice these values are identical, but this establishes deterministic behavior.

The top-level `event_datetime` is used for the CleverTap `ts` field (UNIX epoch) — see below.

#### Layer 1 — Top-Level Fields

Added directly to `evtData`:

| Adapty Field | `evtData` Key | Type |
|---|---|---|
| `profile_id` | `profile_id` | UUID |
| `idfv` | `idfv` | UUID |
| `idfa` | `idfa` | UUID |
| `advertising_id` | `advertising_id` | UUID |
| `profile_install_datetime` | `profile_install_datetime` | ISO 8601 |
| `user_agent` | `user_agent` | String |
| `email` | `email` | String |
| `event_api_version` | `event_api_version` | Integer |

#### Layer 2 — Event Properties

All `event_properties` fields are flattened directly into `evtData` (no prefix). Present fields vary by event type — all are forwarded when present.

**Common fields (all event types):**

| Field | Type |
|---|---|
| `store` | String |
| `currency` | String |
| `price_usd` | Float |
| `price_local` | Float |
| `vendor_product_id` | String |
| `transaction_id` | String |
| `original_transaction_id` | String |
| `purchase_date` | ISO 8601 |
| `original_purchase_date` | ISO 8601 |
| `subscription_expires_at` | ISO 8601 |
| `profile_event_id` | UUID |
| `profile_id` | UUID |
| `profile_country` | String |
| `profile_ip_address` | String |
| `profile_has_access_level` | Boolean |
| `profile_total_revenue_usd` | Float |
| `environment` | String |
| `consecutive_payments` | Integer |
| `rate_after_first_year` | Boolean |
| `store_country` | String |
| `cohort_name` | String |
| `paywall_name` | String |
| `paywall_revision` | String |
| `variation_id` | UUID |
| `developer_id` | String |
| `ab_test_name` | String |
| `ab_test_revision` | Integer |
| `base_plan_id` | String |
| `promotional_offer_id` | String |
| `store_offer_category` | String |
| `store_offer_discount_type` | String |
| `event_datetime` | ISO 8601 |

**Conditional fields (present on specific event types):**

| Field | Type | Present On |
|---|---|---|
| `cancellation_reason` | String | `subscription_renewal_cancelled`, `subscription_refunded`, `trial_renewal_cancelled` |
| `trial_duration` | String | `trial_started`, `trial_converted`, `trial_renewal_cancelled` |

**Tax & revenue fields (present on specific event types):**

Event types: `subscription_renewed`, `subscription_started`, `subscription_refunded`, `non_subscription_purchase`

| Field | Type |
|---|---|
| `net_revenue_usd` | Float |
| `net_revenue_local` | Float |
| `proceeds_usd` | Float |
| `proceeds_local` | Float |
| `tax_amount_usd` | Float |
| `tax_amount_local` | Float |

**Access Level Updated fields (present only on `access_level_updated`):**

| Field | Type |
|---|---|
| `access_level_id` | String |
| `activated_at` | ISO 8601 |
| `active_introductory_offer_type` | String |
| `active_promotional_offer_id` | String |
| `active_promotional_offer_type` | String |
| `billing_issue_detected_at` | ISO 8601 |
| `expires_at` | ISO 8601 |
| `is_active` | Boolean |
| `is_in_grace_period` | Boolean |
| `is_lifetime` | Boolean |
| `is_refund` | Boolean |
| `renewed_at` | ISO 8601 |
| `starts_at` | ISO 8601 |
| `will_renew` | Boolean |

#### Layer 3 — Attributions

Flattened with `attribution_{source}_{field}` prefix. Dynamic — supports any attribution source.

| Adapty Path | `evtData` Key |
|---|---|
| `attributions.{source}.campaign` | `attribution_{source}_campaign` |
| `attributions.{source}.status` | `attribution_{source}_status` |
| `attributions.{source}.channel` | `attribution_{source}_channel` |
| `attributions.{source}.ad_set` | `attribution_{source}_ad_set` |
| `attributions.{source}.ad_group` | `attribution_{source}_ad_group` |
| `attributions.{source}.creative` | `attribution_{source}_creative` |
| `attributions.{source}.created_at` | `attribution_{source}_created_at` |
| `attributions.{source}.network_user_id` | `attribution_{source}_network_user_id` |

#### Layer 4 — User Attributes

Flattened with `user_attr_` prefix.

| Adapty Path | `evtData` Key |
|---|---|
| `user_attributes.{key}` | `user_attr_{key}` |

Values can be strings or floats. Boolean and integer values from the server-side API are converted to floats by Adapty.

#### Layer 5 — Integration IDs

Flattened with `integration_` prefix.

| Adapty Path | `evtData` Key |
|---|---|
| `integration_ids.{key}` | `integration_{key}` |

Known integration IDs: `adjust_device_id`, `airbridge_device_id`, `amplitude_device_id`, `amplitude_user_id`, `appmetrica_device_id`, `appmetrica_profile_id`, `appsflyer_id`, `branch_id`, `facebook_anonymous_id`, `firebase_app_instance_id`, `mixpanel_user_id`, `pushwoosh_hwid`, `one_signal_player_id`, `one_signal_subscription_id`, `tenjin_analytics_installation_id`, `posthog_distinct_user_id`

#### Layer 6 — Play Store Purchase Token

Flattened with `play_store_` prefix. Only present when "Send Play Store purchase token" is enabled in Adapty webhook settings.

| Adapty Path | `evtData` Key | Type |
|---|---|---|
| `play_store_purchase_token.product_id` | `play_store_product_id` | String |
| `play_store_purchase_token.purchase_token` | `play_store_purchase_token` | String |
| `play_store_purchase_token.is_subscription` | `play_store_is_subscription` | Boolean |

#### Layer 7 — Profiles Sharing Access Level

| Adapty Path | `evtData` Key | Type |
|---|---|---|
| `profiles_sharing_access_level` | `profiles_sharing_access_level` | JSON string |

The `profiles_sharing_access_level` array of objects (each with `profile_id` and `customer_user_id`) is JSON-serialized into a single string value, since CleverTap event properties do not support nested objects. Example: `"[{\"profile_id\":\"abc\",\"customer_user_id\":\"user1\"}]"`

### Null Handling

Fields with `null` values are omitted from `evtData`.

### Deduplication

`profile_event_id` is included in every event and logged. The Go consumer maintains an in-memory set of recently processed IDs (bounded LRU, ~100K entries) to skip duplicates from SQS redelivery.

### Example Transformation

**Input (Adapty webhook):**
```json
{
  "profile_id": "abc-123",
  "customer_user_id": "user_42",
  "email": "john@example.com",
  "event_type": "subscription_started",
  "event_datetime": "2026-03-23T10:00:00.000000+0000",
  "event_properties": {
    "store": "play_store",
    "currency": "USD",
    "price_usd": 4.99,
    "vendor_product_id": "premium_monthly",
    "profile_event_id": "evt-789",
    "environment": "Production"
  },
  "attributions": {
    "appsflyer": {
      "campaign": "spring_promo",
      "status": "non_organic"
    }
  },
  "user_attributes": {"plan_preference": "annual"}
}
```

**Output (CleverTap Upload Events API):**
```json
{
  "d": [{
    "identity": "user_42",
    "ts": 1774436400,
    "type": "event",
    "evtName": "Subscription Started",
    "evtData": {
      "profile_id": "abc-123",
      "email": "john@example.com",
      "store": "play_store",
      "currency": "USD",
      "price_usd": 4.99,
      "vendor_product_id": "premium_monthly",
      "profile_event_id": "evt-789",
      "environment": "Production",
      "attribution_appsflyer_campaign": "spring_promo",
      "attribution_appsflyer_status": "non_organic",
      "user_attr_plan_preference": "annual"
    }
  }]
}
```

**Note:** The `ts` field is a UNIX epoch in seconds, converted from the top-level `event_datetime` (ISO 8601). This ensures CleverTap records the actual event time, not the API receipt time — critical for delayed, retried, or backfilled events.

## Error Handling

### Retry Strategy

| Error Type | Action |
|---|---|
| CleverTap 200 + `status: "success"` | Success — delete message from SQS |
| CleverTap 200 + `status: "partial"` | Log failed records from `unprocessed` array, send failures to DLQ |
| CleverTap 200 + `status: "fail"` | Inspect error — retry if transient, DLQ if payload issue |
| CleverTap 429 (rate limit) | Retry with exponential backoff (1s, 2s, 4s), max 3 attempts |
| CleverTap 5xx (server error) | Same retry strategy |
| CleverTap 400 (bad request) | Log error + send to DLQ immediately (no retry — payload issue) |
| CleverTap 401/403 (auth failure) | Log critical alert + stop processing (all events for this account will fail — credential issue, not a per-message problem) |
| Network timeout | Retry with backoff |
| Malformed SQS message | Log + DLQ immediately |
| Missing `event_type` in payload | Log + skip silently (likely Adapty verification request) |

After 3 in-app retries fail, the message returns to SQS. After 5 SQS redelivery failures → DLQ.

### Batching

The Fargate consumer batches up to `BATCH_SIZE` SQS messages (default: 10) into a single CleverTap API call. SQS `ReceiveMessage` returns up to 10 messages per call — these are transformed and sent as a single `"d": [...]` array. This reduces HTTP overhead and improves throughput during traffic spikes.

### Dead Letter Queue

Each account has a DLQ (`adapty-ct-{account_id}-dlq`). Messages in the DLQ retain the original payload for manual inspection and replay. The infra team should set up CloudWatch alarms on DLQ depth.

## Logging & Observability

### Structured JSON Logs

Every processed event produces a log line:

```json
{"level":"info","ts":"2026-03-23T10:00:01Z","event_type":"subscription_renewed","ct_account":"abc","identity":"user_123","profile_event_id":"evt-789","status":"delivered","ct_status_code":200,"latency_ms":142}
```

```json
{"level":"error","ts":"2026-03-23T10:00:02Z","event_type":"billing_issue_detected","ct_account":"abc","identity":"user_456","profile_event_id":"evt-790","status":"failed","error":"ct_api_timeout","attempt":3,"will_retry":false}
```

### Metrics (via log aggregation)

| Metric | Type | Labels |
|---|---|---|
| `events_processed_total` | Counter | `event_type`, `ct_account`, `status` |
| `ct_api_latency_ms` | Histogram | `ct_account`, `event_type` |
| `dlq_messages_total` | Counter | `ct_account` |
| `sqs_messages_received_total` | Counter | `ct_account` |
| `dedup_skipped_total` | Counter | `ct_account` |

Logs go to stdout → CloudWatch Logs. Infra team sets up metric filters and dashboards.

## Backfill CLI

A CLI tool in the same repo sharing the transformation logic:

```
./backfill \
  --ct-account-id=XXX \
  --ct-passcode=YYY \
  --ct-region=eu1 \
  --input=adapty_export.json \
  --batch-size=500 \
  --concurrency=5 \
  --transform-config=transform-config.json \
  --dry-run
```

**Input format:** Newline-delimited JSON (NDJSON) — one Adapty webhook payload per line. This matches what you'd get from an Adapty data export or by piping webhook logs. Example:
```
{"profile_id":"abc","customer_user_id":"user_1","event_type":"subscription_started",...}
{"profile_id":"def","customer_user_id":"user_2","event_type":"trial_started",...}
```

Features:
- Reads NDJSON file of Adapty webhook payloads
- Reuses the same `transform` package as the Fargate consumer
- Batches events (CleverTap supports up to 1000 per API call)
- Concurrency-limited (default: 5, max: 15 to respect CleverTap rate limit)
- Progress logging with processed/total counts
- Resumable via `--offset` flag (skip N records)
- `--dry-run` mode for validation
- Same retry logic as the connector
- Respects the same `TransformConfig` for field exclusions via `--transform-config`

## Local Development

### Docker Compose Stack

| Service | Purpose | Port |
|---|---|---|
| `localstack` | SQS mock | 4566 |
| `connector` | Go Fargate container (polls LocalStack SQS) | — |
| `mock-clevertap` | HTTP server that validates and logs received payloads | 8080 |

### Test Scripts

- `scripts/send-webhook.sh` — sends a single sample Adapty webhook payload to the local SQS queue
- `scripts/seed-queue.sh` — bulk-loads test events for load/integration testing
- Both scripts use the sample payloads from `testdata/` directory

### Running Locally

```bash
docker compose up          # Start all services
./scripts/send-webhook.sh  # Send a test event
# Check connector logs for processed event
# Check mock-clevertap logs for received CleverTap payload
```

## Repo Structure

```
adapty-ct-connector/
├── cmd/
│   ├── connector/           # Fargate SQS consumer entrypoint
│   │   └── main.go
│   └── backfill/            # Backfill CLI entrypoint
│       └── main.go
├── internal/
│   ├── adapty/              # Adapty webhook payload types & parsing
│   │   └── types.go
│   ├── clevertap/           # CleverTap API client & event types
│   │   ├── client.go
│   │   └── types.go
│   ├── transform/           # Adapty → CleverTap mapping (shared by connector & backfill)
│   │   ├── transform.go
│   │   └── config.go        # TransformConfig, layer/field exclusion logic
│   └── queue/               # SQS consumer (polling, ack, error routing)
│       └── consumer.go
├── testdata/                 # Sample Adapty webhook payloads (all event types)
│   ├── subscription_started.json
│   ├── subscription_renewed.json
│   ├── access_level_updated.json
│   └── ...
├── scripts/
│   ├── send-webhook.sh
│   └── seed-queue.sh
├── docker-compose.yml        # Local dev (LocalStack + connector + mock CT)
├── Dockerfile
├── go.mod
├── go.sum
├── transform-config.json    # Default transform config (all layers enabled)
└── docs/
    └── architecture.md       # Infra team handoff document
```

## Operational Concerns

### Graceful Shutdown

The Fargate consumer catches `SIGTERM` (sent by ECS during deploys/scaling), stops polling SQS, finishes processing in-flight messages, and exits cleanly. Messages not yet acknowledged return to SQS automatically (visibility timeout).

### Health Check

The consumer exposes a `/healthz` HTTP endpoint on port 8080. ECS health checks hit this endpoint. It returns:
- `200` if the consumer is polling SQS and processing normally
- `503` if the consumer has been unable to reach SQS or CleverTap for > 60 seconds

## Infra Team Handoff (architecture.md)

The `docs/architecture.md` file will contain:
1. Architecture diagram and data flow
2. API Gateway setup: path-based routing, Authorization header validation, SQS integration
3. Per-account provisioning: SQS queue + DLQ + Fargate task definition
4. Environment variables reference
5. CloudWatch alarms: DLQ depth, error rate, consumer lag
6. Adapty webhook setup: where to configure the endpoint URL and auth header in the Adapty Dashboard
7. Scaling guidance: Fargate task CPU/memory, SQS polling concurrency

## Decisions Log

| Decision | Rationale |
|---|---|
| Fargate over Lambda | Durability — persistent connections, longer processing windows, controlled retry logic |
| Per-account isolation | Data isolation, independent failure domains, simple credential management via env vars |
| Go | Smallest container image, fastest execution, compiled type safety, clean structured logging |
| Forward all fields | Future-proof — new Adapty fields forwarded without code changes. Exclusion config handles filtering |
| API Gateway → SQS direct | No compute needed for ingestion. Handles burst traffic with zero scaling concerns |
| In-app + SQS retries | Fast recovery from transient errors (backoff in code), DLQ safety net for persistent failures |
| Configurable exclusions | Per-layer and per-field exclusion via JSON config, no code changes to filter data |
| Batching | Consumer batches SQS messages into single CleverTap API calls to reduce HTTP overhead |
| NDJSON for backfill | Line-delimited JSON is streamable, resumable, and matches typical export formats |
