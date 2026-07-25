#!/usr/bin/env bash
# ABOUTME: Manual end-to-end test of AiryFS nested volumes (mounts) via the CLI.
# ABOUTME: Several steps intentionally FAIL (EXDEV, cycle, self, not-found, unavailable) — read the error codes.

# Deliberately NOT `set -e`: the guard checks are expected to error.

# --- config ---------------------------------------------------------------
# `airy` if installed (npm run install-cli), else point at the built binary:
AIRY="${AIRY:-airy}"                 # e.g. AIRY="node cli/dist/bin/airyfs.js"
ENDPOINT="${ENDPOINT:-https://airyfs-int.tyson-s-sandbox.workers.dev}"
TS="$(date +%s)"
HOST="mount-host-$TS"                # volume A (the namespace host)
TGT="mount-target-$TS"               # volume B (mounted into A)

section() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

# exec through the Container can return MOUNT_TARGET_UNAVAILABLE (503) during the
# brief cold-start window while the guest mount comes up; retry like a client.
hexec() {
  for i in 1 2 3 4 5; do
    $AIRY --session host exec "$@" && return 0
    echo "  (exec transient — retrying $i/5)"; sleep 3
  done
  return 1
}

# --- sessions (the CLI is scoped to one volume; make one per volume) --------
section "Create sessions"
$AIRY session create host   -e "$ENDPOINT" -v "$HOST"
$AIRY session create target -e "$ENDPOINT" -v "$TGT"

# --- create + seed the target volume ---------------------------------------
section "Create and seed target volume B"
$AIRY --session target volume create
printf 'lives in target B\n' | $AIRY --session target write /seed.txt
$AIRY --session target ls /

# --- mount B into A at /data -----------------------------------------------
section "Mount B at /data in A (target already exists)"
$AIRY --session host mount create /data --target "$TGT"
$AIRY --session host mount list
# One-shot create-and-mount of a brand-new target also works:
$AIRY --session host mount create /data2 --target "${TGT}-2" --create
$AIRY --session host mount list

# --- direct-path forwarding: ops under /data hit B's storage ---------------
section "Direct-path forwarding (host -> target)"
printf 'written through the host mount\n' | $AIRY --session host write /data/hello.txt
$AIRY --session host   cat /data/hello.txt      # forwarded read
$AIRY --session target cat /hello.txt           # same bytes, proves it lives in B
$AIRY --session host   ls  /data                # forwarded readdir: seed.txt, hello.txt

# --- in-container FUSE: exec sees the mount at /volume/data ------------------
section "In-container FUSE via exec"
hexec cat /volume/data/hello.txt                                    # read B through nested FUSE
hexec sh -c 'echo "written from exec via FUSE" > /volume/data/from-exec.txt'
$AIRY --session target cat /from-exec.txt        # the exec write landed in B

# --- fan-out truncation signal (HTTP header, not surfaced by the CLI) -------
section "Fan-out truncation header (tree over a scope containing /data)"
curl -sS -D - -o /dev/null "$ENDPOINT/v1/volumes/$HOST/tree/" | grep -i 'truncated-at-mounts'

# --- guards (each of these SHOULD error) ------------------------------------
section "Guard: cross-boundary rename returns EXDEV"
$AIRY --session host mv /data/hello.txt /top.txt        # EXDEV

section "Guard: mounting the host back into the target is a cycle"
$AIRY --session target mount create /back --target "$HOST"   # MOUNT_CYCLE

section "Guard: a volume cannot mount itself"
$AIRY --session host mount create /self --target "$HOST"     # MOUNT_SELF

section "Guard: mounting a non-existent target is refused"
$AIRY --session host mount create /typo --target "nope-$TS"  # MOUNT_TARGET_NOT_FOUND

# --- degraded mount: delete the target, reads become structured errors ------
section "Degraded: delete target, forwarded read returns MOUNT_TARGET_UNAVAILABLE"
$AIRY --session target volume delete --force
$AIRY --session host cat /data/hello.txt        # MOUNT_TARGET_UNAVAILABLE (503)
$AIRY --session host ls  /data                  # same — not a false-empty listing

# --- unmount + observe mount health -----------------------------------------
section "Unmount and inspect mount health"
curl -sS "$ENDPOINT/v1/volumes/$HOST/usage" | grep -o '"mounts":\[[^]]*\]' || true   # healthy per mount
$AIRY --session host mount rm /data
$AIRY --session host mount rm /data2
$AIRY --session host mount list

# --- cleanup ----------------------------------------------------------------
section "Cleanup"
$AIRY --session host volume delete --force
$AIRY session delete host   2>/dev/null || true
$AIRY session delete target 2>/dev/null || true
echo "done."
