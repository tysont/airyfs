#!/usr/bin/env bash
# ABOUTME: Auto-size and record a demo script to a README-embeddable GIF.
# ABOUTME: Rows are derived from the demo's own command + "# Output:" lines; long demos scroll.
set -u
demo="${1:?usage: record.sh demos/scripts/NN-name.sh [cols] [maxrows]}"
cols="${2:-76}"; maxrows="${3:-28}"
theme=dracula; fontsize=16
name="$(basename "$demo" .sh)"
gif="demos/gifs/${name}.gif"

# Command lines: non-comment, non-blank. Output lines: the "# " comment lines in
# the trailing "# Output:" block. rows = commands + output + prompt + buffer,
# capped so very long demos scroll naturally instead of producing a giant image.
ncmd="$(grep -cE '^[[:space:]]*[^#[:space:]]' "$demo")"
nout="$(awk '/^# Output:/{f=1;next} f&&/^#/{c++} END{print c+0}' "$demo")"
rows=$(( ncmd + nout + 2 ))
# The "# Output:" comment block often abbreviates the real output, so apply a
# floor to keep short demos from clipping their live output.
floor=8
if [ "$rows" -lt "$floor" ]; then rows="$floor"; fi
scroll=""
if [ "$rows" -gt "$maxrows" ]; then rows="$maxrows"; scroll=" (capped -> scrolls)"; fi

asciinema rec --overwrite --window-size "${cols}x${rows}" \
  -c "bash demos/gifs/play.sh $demo" /tmp/rec.cast >/dev/null 2>&1
agg --font-size "$fontsize" --theme "$theme" --idle-time-limit 2 /tmp/rec.cast "$gif" >/dev/null 2>&1

dims="$(ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=p=0 "$gif" 2>/dev/null)"
printf '%s  %s  rows=%s%s\n' "$gif" "$dims" "$rows" "$scroll"
