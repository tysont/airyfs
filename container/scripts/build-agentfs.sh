#!/usr/bin/env bash
# ABOUTME: Compatibility entrypoint for the vendored AgentFS build.
# ABOUTME: Delegates to the repository-local patch materializer and build gate.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$SCRIPT_DIR/../../agentfs/build.sh" "$@"
