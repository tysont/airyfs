#!/bin/bash
# ABOUTME: End-to-end test script for AiryFS.
# ABOUTME: Tests the full flow: DO write → FUSE read → FUSE write → DO read → persistence.

set -e

if [ -z "${AIRYFS_URL:-}" ]; then
  echo "AIRYFS_URL is required; target an explicit environment URL (for example airyfs-int)"
  exit 2
fi
BASE="$AIRYFS_URL"
VOL="e2e-$(date +%s)"
PASS=0
FAIL=0

check() {
  local desc="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $desc"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $desc"
    echo "    expected: $expected"
    echo "    actual:   $actual"
    FAIL=$((FAIL + 1))
  fi
}

check_contains() {
  local desc="$1" expected="$2" actual="$3"
  if echo "$actual" | grep -q "$expected"; then
    echo "  PASS: $desc"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $desc"
    echo "    expected to contain: $expected"
    echo "    actual: $actual"
    FAIL=$((FAIL + 1))
  fi
}

check_max() {
  local desc="$1" maximum="$2" actual="$3"
  if [ "$actual" -le "$maximum" ]; then
    echo "  PASS: $desc (${actual}ms <= ${maximum}ms)"
    PASS=$((PASS + 1))
  else
    echo "  FAIL: $desc"
    echo "    maximum: ${maximum}ms"
    echo "    actual:  ${actual}ms"
    FAIL=$((FAIL + 1))
  fi
}

now_ms() {
  python3 -c 'import time; print(time.time_ns() // 1_000_000)'
}

echo "=== AiryFS E2E Tests ==="
echo "Base URL: $BASE"
echo "Volume: $VOL"
echo ""

# -- 0. Volume configuration and chunk amplification --
echo "0. Volume configuration"

CREATE=$(curl -sf -X PUT "$BASE/v1/volumes/$VOL" \
  -H "Content-Type: application/json" -d '{"chunkSize":262144}')
check_contains "create 256 KiB volume" '"chunkSize":262144' "$CREATE"

dd if=/dev/zero bs=1048576 count=1 2>/dev/null | \
  curl -sf -X PUT "$BASE/v1/volumes/$VOL/files/chunks.bin" --data-binary @- > /dev/null
CHUNK_ROWS=$(curl -sf "$BASE/db-info?volume=$VOL" | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('fs_data',-1))")
check "1 MiB uses four 256 KiB rows" "4" "$CHUNK_ROWS"

echo ""

# -- 1. Filesystem (DO SDK, no container) --
echo "1. Filesystem via DO SDK"

curl -sf -X POST "$BASE/fs/write?volume=$VOL&path=/config.json" -d '{"from":"do"}' > /dev/null
check "write file" "ok" "$(curl -sf -X POST "$BASE/fs/write?volume=$VOL&path=/config.json" -d '{"from":"do"}')"

CONTENT=$(curl -sf "$BASE/fs/read?volume=$VOL&path=/config.json")
check "read file" '{"from":"do"}' "$CONTENT"

LS=$(curl -sf "$BASE/fs/ls?volume=$VOL&path=/")
check_contains "list root" "config.json" "$LS"

echo ""

# -- 2. KV store --
echo "2. KV store"

curl -sf -X POST "$BASE/kv/set?volume=$VOL&key=env" -d "production" > /dev/null
KV=$(curl -sf "$BASE/kv/get?volume=$VOL&key=env")
check "kv round-trip" "production" "$KV"

echo ""

# -- 3. Container exec (FUSE mount) --
echo "3. Container exec with FUSE"

# Trigger container setup
EXEC_RESULT=$(curl -sf --max-time 90 -X POST "$BASE/exec?volume=$VOL" \
  -H "Content-Type: application/json" -d '{"command":"cat /volume/config.json"}')
STDOUT=$(echo "$EXEC_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('stdout','').strip())" 2>/dev/null)
check "FUSE reads DO-written file" '{"from":"do"}' "$STDOUT"

CHUNK_SIZE_RESULT=$(curl -sf --max-time 15 -X POST "$BASE/exec?volume=$VOL" \
  -H "Content-Type: application/json" -d '{"command":"stat -c%s /volume/chunks.bin"}')
CHUNK_SIZE_STDOUT=$(echo "$CHUNK_SIZE_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('stdout','').strip())" 2>/dev/null)
check "FUSE reads 1 MiB direct file" "1048576" "$CHUNK_SIZE_STDOUT"

TTL_START=$(now_ms)
curl -sf -X POST "$BASE/v1/volumes/$VOL/operations/truncate" \
  -H "Content-Type: application/json" -d '{"path":"/chunks.bin","size":524288}' > /dev/null
TRUNCATED_RESULT=$(curl -sf --max-time 15 -X POST "$BASE/exec?volume=$VOL" \
  -H "Content-Type: application/json" -d '{"command":"stat -c%s /volume/chunks.bin"}')
TTL_VISIBILITY_MS=$(( $(now_ms) - TTL_START ))
TRUNCATED_STDOUT=$(echo "$TRUNCATED_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('stdout','').strip())" 2>/dev/null)
check "FUSE refreshes direct size mutation" "524288" "$TRUNCATED_STDOUT"
check_max "attribute refresh is bounded" 5000 "$TTL_VISIBILITY_MS"

curl -sf -X POST "$BASE/v1/volumes/$VOL/operations/rename" \
  -H "Content-Type: application/json" -d '{"from":"/config.json","to":"/renamed-config.json"}' > /dev/null
RENAMED_RESULT=$(curl -sf --max-time 15 -X POST "$BASE/exec?volume=$VOL" \
  -H "Content-Type: application/json" -d '{"command":"test ! -e /volume/config.json && cat /volume/renamed-config.json"}')
RENAMED_STDOUT=$(echo "$RENAMED_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('stdout','').strip())" 2>/dev/null)
check "FUSE refreshes direct entry rename" '{"from":"do"}' "$RENAMED_STDOUT"

# Write from FUSE
curl -sf --max-time 15 -X POST "$BASE/exec?volume=$VOL" \
  -H "Content-Type: application/json" -d '{"command":"echo from-fuse > /volume/output.txt"}' > /dev/null

DO_READ=$(curl -sf "$BASE/fs/read?volume=$VOL&path=/output.txt")
check_contains "DO reads FUSE-written file" "from-fuse" "$DO_READ"

echo ""

# -- 4. Transport admission --
echo "4. Transport admission"

EXEC_ONE_FILE=$(mktemp)
curl -sf --max-time 30 -X POST "$BASE/exec?volume=$VOL" \
  -H "Content-Type: application/json" -d '{"command":"printf ready > /volume/phase4-ready; sleep 3; printf phase4"}' > "$EXEC_ONE_FILE" &
EXEC_ONE_PID=$!
for _ in $(seq 1 50); do
  [ "$(curl -s "$BASE/fs/read?volume=$VOL&path=/phase4-ready")" = "ready" ] && break
  sleep 0.1
done
EXEC_BUSY_RESPONSE=$(curl -s --max-time 15 -w $'\n%{http_code}' -X POST "$BASE/exec?volume=$VOL" \
  -H "Content-Type: application/json" -d '{"command":"printf overlap"}')
EXEC_BUSY_STATUS=${EXEC_BUSY_RESPONSE##*$'\n'}
EXEC_BUSY=${EXEC_BUSY_RESPONSE%$'\n'*}
wait "$EXEC_ONE_PID"
EXEC_ONE=$(<"$EXEC_ONE_FILE")
rm -f "$EXEC_ONE_FILE"
check_contains "first concurrent exec completes" '"stdout":"phase4"' "$EXEC_ONE"
check "overlapping exec returns 503" "503" "$EXEC_BUSY_STATUS"
check_contains "overlapping exec is rejected" '"code":"EXEC_BUSY"' "$EXEC_BUSY"

echo ""

# -- 5. Git inside FUSE --
echo "5. Git inside FUSE"

# Run metadata-heavy Git work on the same volume after mixed direct/FUSE
# mutations; the journal poller must invalidate the active mount first.
GIT_VOL="$VOL"

curl -sf --max-time 330 -X POST "$BASE/exec?volume=$GIT_VOL" \
  -H "Content-Type: application/json" -d '{"command":"git init -q --template= /volume 2>&1"}' > /dev/null
curl -sf --max-time 330 -X POST "$BASE/exec?volume=$GIT_VOL" \
  -H "Content-Type: application/json" -d '{"command":"cd /volume && echo hello > main.py && git add -A 2>&1"}' > /dev/null
curl -sf --max-time 330 -X POST "$BASE/exec?volume=$GIT_VOL" \
  -H "Content-Type: application/json" -d '{"command":"git -C /volume -c user.name=airyfs -c user.email=airyfs@test -c maintenance.auto=false commit --quiet --no-verify -m init 2>&1"}' > /dev/null
GIT_LOG=$(curl -sf --max-time 330 -X POST "$BASE/exec?volume=$GIT_VOL" \
  -H "Content-Type: application/json" -d '{"command":"git -C /volume log --oneline 2>&1"}')
GIT_STDOUT=$(echo "$GIT_LOG" | python3 -c "import sys,json; print(json.load(sys.stdin).get('stdout','').strip())" 2>/dev/null)
check_contains "git commit exists" "init" "$GIT_STDOUT"

LS2=$(curl -sf "$BASE/fs/ls?volume=$GIT_VOL&path=/")
check_contains "DO sees .git" ".git" "$LS2"
check_contains "DO sees main.py" "main.py" "$LS2"

echo ""

# -- 6. Persistence across container eviction --
echo "6. Persistence"

curl -sf -X POST "$BASE/destroy?volume=$GIT_VOL" > /dev/null
sleep 3

PERSIST=$(curl -sf "$BASE/fs/read?volume=$GIT_VOL&path=/main.py")
check_contains "file survives container destroy" "hello" "$PERSIST"

# New container reads via FUSE
NEW_FUSE=$(curl -sf --max-time 90 -X POST "$BASE/exec?volume=$GIT_VOL" \
  -H "Content-Type: application/json" -d '{"command":"cat /volume/main.py && git -C /volume log --oneline 2>&1"}')
NEW_STDOUT=$(echo "$NEW_FUSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('stdout','').strip())" 2>/dev/null)
check_contains "new container sees file via FUSE" "hello" "$NEW_STDOUT"
check_contains "new container sees git history" "init" "$NEW_STDOUT"

echo ""

# -- Summary --
echo "=== Results: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
