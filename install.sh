#!/usr/bin/env bash
# ABOUTME: Bootstrap installer for the AiryFS CLI; builds the SDK and CLI and links `airyfs`/`airy`.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v node >/dev/null 2>&1; then
  echo "Node.js 22+ is required but was not found on PATH." >&2
  exit 1
fi

exec node "${ROOT_DIR}/scripts/install.mjs" "$@"
