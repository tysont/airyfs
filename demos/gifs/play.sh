#!/usr/bin/env bash
# Typewriter player: types each command from a demo file, runs it for real,
# and shows the output. Recorded by asciinema, rendered to GIF by agg.
set -u
demo="$1"
typeline() {
  printf '$ '
  local s="$1" i
  for (( i=0; i<${#s}; i++ )); do printf '%s' "${s:i:1}"; sleep 0.045; done
  printf '\n'
}
sleep 0.6
while IFS= read -r line; do
  case "$line" in ''|\#*) continue ;; esac
  typeline "$line"
  # Detach stderr from the PTY so the CLI's ora spinner (which checks
  # stderr.isTTY) stays off; the real stdout output is still recorded.
  eval "$line" 2>/dev/null
  sleep 0.9
done < <(grep -vE '^[[:space:]]*(#|$)' "$demo")
sleep 1.3
