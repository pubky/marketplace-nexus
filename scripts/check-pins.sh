#!/usr/bin/env bash
# Fails when a GitHub Action or a Docker base image is selected by a movable
# reference instead of an immutable one: every `uses:` of a third-party action
# must name a 40-hex commit, and every Dockerfile `FROM` must carry a digest.
set -euo pipefail

ROOT="${1:-$(cd "$(dirname "$0")/.." && pwd)}"
status=0

while IFS= read -r hit; do
  file="${hit%%:*}"
  rest="${hit#*:}"
  line="${rest%%:*}"
  ref="${rest#*:}"
  echo "unpinned action: ${file}:${line}:${ref}" >&2
  status=1
done < <(
  grep -nE '^[[:space:]]*(-[[:space:]]+)?uses:[[:space:]]*[^./[:space:]]' "$ROOT"/.github/workflows/*.yml 2>/dev/null \
    | grep -vE 'uses:[[:space:]]*[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[0-9a-f]{40}([[:space:]]|$)' \
    | sed -E 's/^([^:]+):([0-9]+):[[:space:]]*/\1:\2:/' || true
)

while IFS= read -r hit; do
  echo "base image without digest: ${hit}" >&2
  status=1
done < <(
  awk '
    /^FROM[[:space:]]/ {
      image = $2
      if (!(image in stage) && image != "scratch" && image !~ /@sha256:[0-9a-f]{64}$/) {
        print NR ":" $0
      }
      for (i = 3; i < NF; i++) if (toupper($i) == "AS") stage[$(i + 1)] = 1
    }
  ' "$ROOT"/Dockerfile 2>/dev/null || true
)

if [ "$status" -eq 0 ]; then
  echo "pins ok"
fi
exit "$status"
