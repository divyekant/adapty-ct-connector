# Changelog

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
