# Changelog

## [Unreleased] - 2026-07-29

### Added
- **Per-tenant webhook auth** — the TOKEN authorizer now derives the tenant from the method ARN and validates against a per-tenant SSM SecureString parameter (`/adapty-ct-connector/{env}/{tenant}/auth-token`). Onboarding or rotating one tenant never affects another; a tenant's token is only valid on its own `/ingest/{tenant}` path. 15 s in-memory lookup cache (`TOKEN_CACHE_TTL_SECONDS`).
- **`DRY_RUN` for all consumer modes** — previously Fargate-only; the Lambda consumer now supports it too via a shared `clevertap.DryRun` uploader that logs each would-be CleverTap record (full payload) without uploading. Deploy with `DRY_RUN_VAL=true ./04-deploy-lambda.sh` for onboarding inspection.
- **`GET /health`** — unauthenticated API Gateway MOCK integration returning a static `200 {"status":"ok"}` for external monitors (`scripts/deploy/07-add-health.sh`).

### Fixed
- `.gitignore` binary patterns anchored to the repo root — the bare `connector` / `backfill` / `mock-clevertap` patterns were unintentionally ignoring the corresponding `cmd/` source directories, which were missing from the repository.

## [Unreleased] - 2026-03-25

### Added
- **Lambda deployment mode** — SQS event source mapping auto-invokes a Lambda handler, eliminating the need for container infrastructure. Uses `provided.al2023` runtime with a compiled Go binary (`arm64`).
- **Shared processor package** (`internal/processor`) — core parse/dedup/transform/upload pipeline extracted from the Fargate consumer, shared by both Lambda and Fargate entrypoints.
- **Makefile** with `build-connector`, `build-lambda`, `build-backfill`, `test`, and `clean` targets. `build-lambda` cross-compiles for `linux/arm64` and produces a deployment-ready `bin/lambda.zip`.
- **Lambda deployment section** in `docs/architecture.md` covering configuration, SQS event source mapping, IAM role, and deployment checklist.
- **Partial batch failure support** in Lambda handler — returns `SQSBatchItemFailure` for failed messages so only those are retried, not the entire batch.

### Changed
- Fargate consumer (`internal/queue`) now delegates to the shared `processor.Process()` instead of owning the processing logic directly. External behavior is unchanged.
- Architecture documentation reorganized with dual-mode overview (Lambda recommended, Fargate original).
- README updated with Quick Start section, deployment mode comparison, and updated project structure.

### Fixed
- `aws-lambda-go` dependency correctly listed as direct (not indirect) in `go.mod`.
- Consumer error wrapping restored on `ReceiveMessages` failure for better debug context.
- Lambda IAM policy in docs narrowed from `logs:*` to specific CloudWatch Logs actions.
