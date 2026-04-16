#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_LABEL=""
AS_JSON=false

usage() {
  cat <<'USAGE'
usage: scripts/get_soak_status.sh [options]

Options:
  --workspace-root DIR  Workspace root (default: repo root)
  --run-label LABEL     Specific soak run label (default: latest)
  --as-json             Print structured JSON output
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
    --as-json)
      AS_JSON=true
      shift
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
REPORT_PATH="${RUN_DIR}/gate-report.json"
STDOUT_PATH="${RUN_DIR}/stdout.log"
STDERR_PATH="${RUN_DIR}/stderr.log"

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

extract_json_string() {
  local file_path="$1"
  local key="$2"
  if [[ ! -f "$file_path" ]]; then
    return 0
  fi
  sed -n "s/.*\"${key}\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" "$file_path" | head -n 1
}

extract_json_bool() {
  local file_path="$1"
  local key="$2"
  if [[ ! -f "$file_path" ]]; then
    return 0
  fi
  sed -n "s/.*\"${key}\"[[:space:]]*:[[:space:]]*\(true\|false\).*/\1/p" "$file_path" | tail -n 1
}

telemetry_lines_observed=0
if [[ -f "$REPORT_PATH" ]]; then
  telemetry_lines_observed="$(sed -n 's/.*"telemetry_lines_observed"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$REPORT_PATH" | head -n 1)"
fi
if [[ -z "$telemetry_lines_observed" ]]; then
  telemetry_lines_observed=0
fi

state_multi_run_label="$(extract_json_string "$STATE_PATH" "multi_run_label")"
state_stop_reason="$(extract_json_string "$STATE_PATH" "stop_reason")"
report_stop_reason="$(extract_json_string "$REPORT_PATH" "stop_reason")"
report_overall_pass="$(extract_json_bool "$REPORT_PATH" "overall_pass")"

if [[ "$AS_JSON" == true ]]; then
  cat <<EOF
{
  "run_label": "$(json_escape "$RUN_LABEL")",
  "run_dir": "$(json_escape "$RUN_DIR")",
  "stdout_log": "$(json_escape "$STDOUT_PATH")",
  "stderr_log": "$(json_escape "$STDERR_PATH")",
  "state_file": "$(json_escape "$STATE_PATH")",
  "state_file_exists": $( [[ -f "$STATE_PATH" ]] && echo true || echo false ),
  "state_multi_run_label": "$(json_escape "$state_multi_run_label")",
  "state_stop_reason": "$(json_escape "$state_stop_reason")",
  "gate_report_exists": $( [[ -f "$REPORT_PATH" ]] && echo true || echo false ),
  "telemetry_lines_observed": $telemetry_lines_observed,
  "report_stop_reason": "$(json_escape "$report_stop_reason")",
  "report_overall_pass": ${report_overall_pass:-null}
}
EOF
  exit 0
fi

echo "Run label: $RUN_LABEL"
echo "Run dir: $RUN_DIR"
echo "Gate report exists: $( [[ -f "$REPORT_PATH" ]] && echo true || echo false )"
echo "Telemetry lines observed: $telemetry_lines_observed"
if [[ -n "$state_multi_run_label" ]]; then
  echo "State multi_run_label: $state_multi_run_label"
fi
if [[ -n "$state_stop_reason" ]]; then
  echo "State stop_reason: $state_stop_reason"
fi
if [[ -n "$report_overall_pass" ]]; then
  echo "Report gate overall_pass: $report_overall_pass"
fi
if [[ -n "$report_stop_reason" ]]; then
  echo "Report stop_reason: $report_stop_reason"
fi
