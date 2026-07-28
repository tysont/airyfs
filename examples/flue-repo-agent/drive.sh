#!/usr/bin/env bash
# Drive the sample against a running `flue dev --target cloudflare` (port 3599).
set -euo pipefail
PORT="${PORT:-3599}"
BASE="http://127.0.0.1:${PORT}"

echo "== workflow: summarize (staged + produced files land in the AiryFS volume) =="
curl -s "${BASE}/workflows/summarize?wait=result" \
  -H "Content-Type: application/json" \
  -d '{"text":"AiryFS keeps each volume in one Durable Object SQLite database and attaches a Container over FUSE only for real Linux work."}'
echo

echo "== agent: workspace (writes notes.txt to its durable volume) =="
curl -s "${BASE}/agents/workspace/demo-user?wait=result" \
  -H "Content-Type: application/json" \
  -d '{"message":"Use the write tool to save exactly the word Nimbus into notes.txt, then reply DONE."}'
echo
