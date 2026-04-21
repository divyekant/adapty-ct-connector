#!/usr/bin/env bash
# Cross-compiles cmd/lambda for linux/arm64 and packages bin/lambda.zip.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=./config.sh
source "$SCRIPT_DIR/config.sh"

echo ">>> Building lambda.zip (linux/arm64)..."
cd "$REPO_ROOT"
make build-lambda

echo ">>> Artifact:"
ls -lh bin/lambda.zip
