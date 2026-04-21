# adapty-ct-connector

## Overview

A Go service that bridges Adapty subscription webhooks into CleverTap. Events arrive on an Adapty webhook, land in an SQS queue via an API Gateway direct integration, and are processed by a Lambda that transforms each Adapty event into CleverTap profile + event records and uploads them to the CleverTap Upload API.

Each Adapty event may produce two CleverTap records in one upload call: a `profile` record (when email or user_attributes are present) and an `event` record. Deduplication is keyed by `profile_event_id` in the event properties.

## Development

### Setup

```bash
go mod download
make build   # builds all binaries into ./bin
```

Run tests: `go test ./...`

### Commands

| Target | Output |
|---|---|
| `make build-connector` | `bin/connector` — standalone connector (non-Lambda) |
| `make build-lambda` | `bin/lambda.zip` — AWS Lambda SQS consumer (linux/arm64) |
| `make build-authorizer` | `bin/authorizer.zip` — API Gateway TOKEN authorizer (linux/arm64) |
| `make build-backfill` | `bin/backfill` — historical replay tool |
| `make test` | `go test ./... -v` |
| `make clean` | removes `bin/` |

Deployment is driven by `scripts/deploy/` — see that directory's README for the full bring-up sequence.

### Stack

- **Go 1.25**, standard library plus `github.com/aws/aws-lambda-go`, `github.com/hashicorp/golang-lru/v2`
- **AWS Lambda** (runtime `provided.al2023`, arch `arm64`)
- **Amazon SQS** event source (batch size 10, `ReportBatchItemFailures`)
- **Amazon API Gateway** REST API → SQS direct integration
- **CleverTap Upload API** (`https://{region}.api.clevertap.com/1/upload`) — supports profile + event records in a single request

### Layout

- `cmd/lambda/` — SQS-triggered Lambda entrypoint
- `cmd/authorizer/` — API Gateway TOKEN authorizer Lambda
- `cmd/connector/` — local/long-running variant
- `cmd/backfill/` — batch replay tool
- `cmd/mock-clevertap/` — local CT API stub for integration tests
- `internal/processor/` — parse → dedup → transform → upload pipeline
- `internal/transform/` — Adapty event → CleverTap record conversion (layered, configurable)
- `internal/clevertap/` — HTTP client for the Upload API
- `internal/queue/` — consumer loop (used by connector mode)
- `internal/adapty/` — Adapty event types
- `scripts/deploy/` — idempotent AWS provisioning scripts
- `testdata/` — Adapty event fixtures used by transform/processor tests

## Conventions

- **Record indexing is load-bearing.** `CleverTap Upload` replies reference failed records by their position in the request array. `internal/processor` preserves a mapping from `emissions[i]` back to the input SQS message so that per-record failures surface as per-message `OutcomeFail`. Don't reorder emissions after they're appended.
- **Dedup is two-tiered.** A transient within-batch `seenThisBatch` set catches duplicates inside one SQS batch; the persistent LRU cache only receives an event ID after a successful upload. This avoids a silent data loss on retry if an upload fails between the dedup write and the acknowledgment.
- **Fail closed on auth.** `clevertap.AuthError` short-circuits retries and is surfaced to the caller as `BatchResult.FatalError`. The Lambda caller treats this as "don't ack anything from this batch" so operators notice.
- **Transform layers are configurable.** `transform.Config` disables entire layers or excludes fields. See `transform.Layer*` constants.
