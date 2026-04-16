#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_LABEL=""

usage() {
  cat <<'USAGE'
usage: scripts/stop_soak_run.sh [options]

Options:
  --workspace-root DIR  Workspace root (default: repo root)
  --run-label LABEL     Specific soak run label (default: latest)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workspace-root)
      WORKSPACE_ROOT="${2:?missing value for --workspace-root}"
      shift 2
      ;;
    --run-label)
      RUN_LABEL="${2:?missing value for --run-label}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

WORKSPACE_ROOT="$(cd "$WORKSPACE_ROOT" && pwd)"
SOAK_ROOT="${WORKSPACE_ROOT}/logs/soak"
if [[ ! -d "$SOAK_ROOT" ]]; then
  echo "No soak directory found at $SOAK_ROOT" >&2
  exit 1
fi

if [[ -z "$RUN_LABEL" ]]; then
  latest_dir="$(ls -1dt "$SOAK_ROOT"/* 2>/dev/null | head -n 1 || true)"
  if [[ -z "$latest_dir" ]]; then
    echo "No soak runs found under $SOAK_ROOT" >&2
    exit 1
  fi
  RUN_LABEL="$(basename "$latest_dir")"
fi

RUN_DIR="${SOAK_ROOT}/${RUN_LABEL}"
if [[ ! -d "$RUN_DIR" ]]; then
  echo "Requested soak run does not exist: $RUN_DIR" >&2
  exit 1
fi

STATE_PATH="${RUN_DIR}/run-state.json"
if [[ ! -f "$STATE_PATH" ]]; then
  echo "State file not found: $STATE_PATH" >&2
  exit 1
fi

MULTI_RUN_LABEL="$(sed -n 's/.*"multi_run_label"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$STATE_PATH" | head -n 1)"
if [[ -z "$MULTI_RUN_LABEL" ]]; then
  echo "multi_run_label not found in state file: $STATE_PATH" >&2
  exit 1
fi

echo "Stopping soak run label=$RUN_LABEL (multi_run_label=$MULTI_RUN_LABEL)"
"${WORKSPACE_ROOT}/scripts/stop_4pairs.sh" --run-label "$MULTI_RUN_LABEL" || true
"${WORKSPACE_ROOT}/scripts/stop_4pairs.sh" --run-label "$MULTI_RUN_LABEL" --force || true
"${WORKSPACE_ROOT}/scripts/status_4pairs.sh" --run-label "$MULTI_RUN_LABEL" || true

stopped_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

# Update stop reason and stopped timestamp in-place.
tmp_file="${STATE_PATH}.tmp"
awk -v ts="$stopped_utc" '
BEGIN {
  stop_updated = 0;
  stopped_updated = 0;
}
{
  if ($0 ~ /"stop_reason"[[:space:]]*:/) {
    sub(/"stop_reason"[[:space:]]*:[[:space:]]*"[^"]*"/, "\"stop_reason\": \"manual_stop_script\"");
    stop_updated = 1;
  }

  if ($0 ~ /"stopped_utc"[[:space:]]*:/) {
    sub(/"stopped_utc"[[:space:]]*:[[:space:]]*"[^"]*"/, "\"stopped_utc\": \"" ts "\"");
    stopped_updated = 1;
  }

  print;
}
END {
  if (!stopped_updated) {
    # Insert before final closing brace when field is missing.
    # Caller rewrites file only if this marker is present.
    if (!stop_updated) {
      print "__APPEND_STOP_AND_TS__";
    } else {
      print "__APPEND_TS_ONLY__";
    }
  }
}
' "$STATE_PATH" >"$tmp_file"

if grep -q "__APPEND_STOP_AND_TS__" "$tmp_file"; then
  grep -v "__APPEND_STOP_AND_TS__" "$tmp_file" | sed '$ s/}$/  ,\n  "stop_reason": "manual_stop_script",\n  "stopped_utc": "'"$stopped_utc"'"\n}/' >"$STATE_PATH"
elif grep -q "__APPEND_TS_ONLY__" "$tmp_file"; then
  grep -v "__APPEND_TS_ONLY__" "$tmp_file" | sed '$ s/}$/  ,\n  "stopped_utc": "'"$stopped_utc"'"\n}/' >"$STATE_PATH"
else
  mv "$tmp_file" "$STATE_PATH"
  tmp_file=""
fi

if [[ -n "${tmp_file}" && -f "$tmp_file" ]]; then
  rm -f "$tmp_file"
fi

echo "Updated state file: $STATE_PATH"
