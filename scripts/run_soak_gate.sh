#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILES_DIR="${WORKSPACE_ROOT}/env/profiles"
DURATION_MINUTES=1440
RUN_LABEL="$(date -u +%Y%m%d-%H%M%S)"
MODE="debug"
LOSS_RATE_MAX="0.0001"
P99_LATENCY_MAX_MS=60

usage() {
  cat <<'USAGE'
usage: scripts/run_soak_gate.sh [options]

Options:
  --duration-minutes N      Soak duration in minutes (default: 1440)
  --workspace-root DIR      Workspace root (default: repo root)
  --profiles-dir DIR        Profiles directory (default: env/profiles)
  --run-label LABEL         Override run label (default: UTC timestamp)
  --release                 Use release binary for launched profiles
  --debug                   Use debug binary for launched profiles (default)
  --loss-rate-max VALUE     Loss-rate SLO threshold (default: 0.0001)
  --p99-latency-max-ms N    p99 latency SLO threshold in ms (default: 60)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration-minutes)
      DURATION_MINUTES="${2:?missing value for --duration-minutes}"
      shift 2
      ;;
    --workspace-root)
      WORKSPACE_ROOT="${2:?missing value for --workspace-root}"
      shift 2
      ;;
    --profiles-dir)
      PROFILES_DIR="${2:?missing value for --profiles-dir}"
      shift 2
      ;;
    --run-label)
      RUN_LABEL="${2:?missing value for --run-label}"
      shift 2
      ;;
    --release)
      MODE="release"
      shift
      ;;
    --debug)
      MODE="debug"
      shift
      ;;
    --loss-rate-max)
      LOSS_RATE_MAX="${2:?missing value for --loss-rate-max}"
      shift 2
      ;;
    --p99-latency-max-ms)
      P99_LATENCY_MAX_MS="${2:?missing value for --p99-latency-max-ms}"
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

if ! [[ "$DURATION_MINUTES" =~ ^[0-9]+$ ]] || (( DURATION_MINUTES <= 0 )); then
  echo "--duration-minutes must be a positive integer" >&2
  exit 2
fi

if ! [[ "$P99_LATENCY_MAX_MS" =~ ^[0-9]+$ ]] || (( P99_LATENCY_MAX_MS <= 0 )); then
  echo "--p99-latency-max-ms must be a positive integer" >&2
  exit 2
fi

WORKSPACE_ROOT="$(cd "$WORKSPACE_ROOT" && pwd)"
if [[ ! -d "$PROFILES_DIR" ]]; then
  echo "profiles directory not found: $PROFILES_DIR" >&2
  exit 1
fi

SOAK_DIR="${WORKSPACE_ROOT}/logs/soak/${RUN_LABEL}"
mkdir -p "$SOAK_DIR"

STDOUT_LOG="${SOAK_DIR}/stdout.log"
STDERR_LOG="${SOAK_DIR}/stderr.log"
STATE_JSON="${SOAK_DIR}/run-state.json"
REPORT_JSON="${SOAK_DIR}/gate-report.json"
REPORT_MD="${SOAK_DIR}/gate-report.md"

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

strip_ansi() {
  printf '%s\n' "$1" | sed -E $'s/\x1B\[[0-9;]*[ -/]*[@-~]//g'
}

log_info() {
  local msg="$1"
  printf '%s\n' "$msg" | tee -a "$STDOUT_LOG"
}

log_error() {
  local msg="$1"
  printf '%s\n' "$msg" | tee -a "$STDERR_LOG" >&2
}

started_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
finished_utc=""
stop_reason="running"
stopped_by_user=false
process_exit_code=-1
multi_run_label=""

write_state() {
  cat >"$STATE_JSON" <<EOF
{
  "run_label": "$(json_escape "$RUN_LABEL")",
  "started_utc": "$(json_escape "$started_utc")",
  "finished_utc": "$(json_escape "$finished_utc")",
  "workspace_root": "$(json_escape "$WORKSPACE_ROOT")",
  "profiles_dir": "$(json_escape "$PROFILES_DIR")",
  "mode": "$(json_escape "$MODE")",
  "duration_minutes": $DURATION_MINUTES,
  "multi_run_label": "$(json_escape "$multi_run_label")",
  "stop_reason": "$(json_escape "$stop_reason")",
  "process_exit_code": $process_exit_code
}
EOF
}

cancel_requested=false
on_interrupt() {
  cancel_requested=true
  stopped_by_user=true
  log_info "Ctrl+C received, stopping soak run..."
}
trap on_interrupt INT TERM

write_state

log_info "Soak run label: $RUN_LABEL"
log_info "Soak output directory: $SOAK_DIR"
log_info "Starting 4-profile soak run with mode=$MODE"

START_CMD=("${WORKSPACE_ROOT}/scripts/start_4pairs.sh" "--profiles-dir" "$PROFILES_DIR")
if [[ "$MODE" == "release" ]]; then
  START_CMD+=("--release")
else
  START_CMD+=("--debug")
fi

set +e
start_output="$("${START_CMD[@]}" 2>&1)"
start_rc=$?
set -e
printf '%s\n' "$start_output" | tee -a "$STDOUT_LOG"

if (( start_rc != 0 )); then
  stop_reason="start_failed"
  process_exit_code=$start_rc
  finished_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  write_state
  log_error "failed to start multi-profile soak run"
  exit $start_rc
fi

multi_run_label="$(printf '%s\n' "$start_output" | sed -n 's/^run_label=//p' | tail -n 1)"
if [[ -z "$multi_run_label" ]]; then
  stop_reason="start_failed"
  process_exit_code=1
  finished_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  write_state
  log_error "failed to parse run label from start_4pairs output"
  exit 1
fi

write_state

deadline_epoch=$(( $(date +%s) + DURATION_MINUTES * 60 ))
while true; do
  if [[ "$cancel_requested" == true ]]; then
    stop_reason="ctrl_c_requested"
    break
  fi

  if (( $(date +%s) >= deadline_epoch )); then
    stop_reason="duration_elapsed"
    break
  fi

  sleep 1
done

log_info "Stopping multi-profile run: $multi_run_label"
"${WORKSPACE_ROOT}/scripts/stop_4pairs.sh" --run-label "$multi_run_label" >>"$STDOUT_LOG" 2>>"$STDERR_LOG" || true
status_output="$(${WORKSPACE_ROOT}/scripts/status_4pairs.sh --run-label "$multi_run_label" 2>&1 || true)"
printf '%s\n' "$status_output" >>"$STDOUT_LOG"

running_count="$(printf '%s\n' "$status_output" | sed -n 's/^running=\([0-9]\+\) stopped=.*/\1/p' | tail -n 1)"
if [[ -n "$running_count" ]] && (( running_count > 0 )); then
  log_info "Forcing stop for remaining processes"
  "${WORKSPACE_ROOT}/scripts/stop_4pairs.sh" --run-label "$multi_run_label" --force >>"$STDOUT_LOG" 2>>"$STDERR_LOG" || true
  "${WORKSPACE_ROOT}/scripts/status_4pairs.sh" --run-label "$multi_run_label" >>"$STDOUT_LOG" 2>>"$STDERR_LOG" || true
fi

multi_log_dir="${WORKSPACE_ROOT}/logs/multi/${multi_run_label}"
if [[ ! -d "$multi_log_dir" ]]; then
  stop_reason="log_dir_missing"
  process_exit_code=1
  finished_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  write_state
  log_error "multi-profile log directory missing: $multi_log_dir"
  exit 1
fi

float_leq() {
  local value="$1"
  local max="$2"
  awk -v v="$value" -v m="$max" 'BEGIN { exit !(v <= m) }'
}

extract_metric() {
  local line="$1"
  local key="$2"
  awk -v key="$key" '
    {
      for (i = 1; i <= NF; i++) {
        if ($i ~ ("^" key "=")) {
          value = $i
          sub("^" key "=", "", value)
          gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
          print value
          exit
        }
      }
    }
  ' <<<"$line"
}

telemetry_lines_observed=0
profiles_total=0
profiles_telemetry_ok=0
profiles_depth_ok=0
profiles_loss_ok=0
profiles_p99_ok=0
profile_entries_file="${SOAK_DIR}/profile-entries.json.part"
: >"$profile_entries_file"
first_profile_entry=true

for log_file in "$multi_log_dir"/*.log; do
  [[ -f "$log_file" ]] || continue

  profile_name="$(basename "$log_file" .log)"
  profiles_total=$((profiles_total + 1))

  telemetry_lines="$(grep -c "runtime telemetry" "$log_file" || true)"
  telemetry_lines_observed=$((telemetry_lines_observed + telemetry_lines))

  last_line=""
  clean_line=""
  if (( telemetry_lines > 0 )); then
    last_line="$(grep "runtime telemetry" "$log_file" | tail -n 1)"
    clean_line="$(strip_ansi "$last_line")"
  fi

  total_depth_events="$(extract_metric "$clean_line" "total_depth_events")"
  sequence_gap_events="$(extract_metric "$clean_line" "sequence_gap_events")"
  loss_rate="$(extract_metric "$clean_line" "loss_rate")"
  signals_emitted="$(extract_metric "$clean_line" "signals_emitted")"
  stale_signals_emitted="$(extract_metric "$clean_line" "stale_signals_emitted")"
  p99_raw="$(extract_metric "$clean_line" "p99_latency_ms")"
  p99_latency_ms=""
  if [[ "$p99_raw" =~ ^Some\(([0-9]+)\)$ ]]; then
    p99_latency_ms="${BASH_REMATCH[1]}"
  elif [[ "$p99_raw" =~ ^[0-9]+$ ]]; then
    p99_latency_ms="$p99_raw"
  fi

  telemetry_observed=false
  depth_events_observed=false
  loss_rate_pass=false
  p99_latency_pass=false
  profile_pass=false

  if (( telemetry_lines > 0 )); then
    telemetry_observed=true
    profiles_telemetry_ok=$((profiles_telemetry_ok + 1))
  fi

  if [[ -n "$total_depth_events" ]] && (( total_depth_events > 0 )); then
    depth_events_observed=true
    profiles_depth_ok=$((profiles_depth_ok + 1))
  fi

  if [[ -n "$loss_rate" ]] && float_leq "$loss_rate" "$LOSS_RATE_MAX"; then
    loss_rate_pass=true
    profiles_loss_ok=$((profiles_loss_ok + 1))
  fi

  if [[ -n "$p99_latency_ms" ]] && float_leq "$p99_latency_ms" "$P99_LATENCY_MAX_MS"; then
    p99_latency_pass=true
    profiles_p99_ok=$((profiles_p99_ok + 1))
  fi

  if [[ "$telemetry_observed" == true && "$depth_events_observed" == true && "$loss_rate_pass" == true && "$p99_latency_pass" == true ]]; then
    profile_pass=true
  fi

  total_depth_json=${total_depth_events:-null}
  sequence_gap_json=${sequence_gap_events:-null}
  loss_rate_json=${loss_rate:-null}
  signals_json=${signals_emitted:-null}
  stale_json=${stale_signals_emitted:-null}
  p99_json=${p99_latency_ms:-null}

  if [[ "$first_profile_entry" == false ]]; then
    printf ',\n' >>"$profile_entries_file"
  fi
  first_profile_entry=false

  cat >>"$profile_entries_file" <<EOF
    {
      "profile": "$(json_escape "$profile_name")",
      "log_file": "$(json_escape "$log_file")",
      "telemetry_lines_observed": $telemetry_lines,
      "total_depth_events": $total_depth_json,
      "sequence_gap_events": $sequence_gap_json,
      "loss_rate": $loss_rate_json,
      "signals_emitted": $signals_json,
      "stale_signals_emitted": $stale_json,
      "p99_latency_ms": $p99_json,
      "telemetry_observed": $telemetry_observed,
      "depth_events_observed": $depth_events_observed,
      "loss_rate_pass": $loss_rate_pass,
      "p99_latency_pass": $p99_latency_pass,
      "overall_pass": $profile_pass
    }
EOF
done

gate_telemetry_observed=false
gate_depth_events_observed=false
gate_loss_rate_pass=false
gate_p99_latency_pass=false

if (( profiles_total > 0 )) && (( profiles_telemetry_ok == profiles_total )); then
  gate_telemetry_observed=true
fi
if (( profiles_total > 0 )) && (( profiles_depth_ok == profiles_total )); then
  gate_depth_events_observed=true
fi
if (( profiles_total > 0 )) && (( profiles_loss_ok == profiles_total )); then
  gate_loss_rate_pass=true
fi
if (( profiles_total > 0 )) && (( profiles_p99_ok == profiles_total )); then
  gate_p99_latency_pass=true
fi

overall_pass=false
if [[ "$gate_telemetry_observed" == true && "$gate_depth_events_observed" == true && "$gate_loss_rate_pass" == true && "$gate_p99_latency_pass" == true ]]; then
  overall_pass=true
fi

finished_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
if [[ "$stop_reason" == "running" ]]; then
  stop_reason="process_exited"
fi

if [[ "$stop_reason" == "ctrl_c_requested" ]]; then
  process_exit_code=130
elif [[ "$overall_pass" == true ]]; then
  process_exit_code=0
else
  process_exit_code=2
fi

write_state

cat >"$REPORT_JSON" <<EOF
{
  "run_label": "$(json_escape "$RUN_LABEL")",
  "workspace_root": "$(json_escape "$WORKSPACE_ROOT")",
  "started_utc": "$(json_escape "$started_utc")",
  "finished_utc": "$(json_escape "$finished_utc")",
  "configured_duration_minutes": $DURATION_MINUTES,
  "command": "scripts/start_4pairs.sh --$MODE --profiles-dir $(json_escape "$PROFILES_DIR")",
  "multi_run_label": "$(json_escape "$multi_run_label")",
  "stop_reason": "$(json_escape "$stop_reason")",
  "stopped_by_user": $stopped_by_user,
  "process_exit_code": $process_exit_code,
  "state_file": "$(json_escape "$STATE_JSON")",
  "stdout_log": "$(json_escape "$STDOUT_LOG")",
  "stderr_log": "$(json_escape "$STDERR_LOG")",
  "telemetry_lines_observed": $telemetry_lines_observed,
  "profiles": [
$(cat "$profile_entries_file")
  ],
  "gate": {
    "profile_count": $profiles_total,
    "loss_rate_max": $LOSS_RATE_MAX,
    "p99_latency_max_ms": $P99_LATENCY_MAX_MS,
    "telemetry_observed": $gate_telemetry_observed,
    "depth_events_observed": $gate_depth_events_observed,
    "loss_rate_pass": $gate_loss_rate_pass,
    "p99_latency_pass": $gate_p99_latency_pass,
    "overall_pass": $overall_pass
  }
}
EOF

cat >"$REPORT_MD" <<EOF
# Soak Gate Report

- Run label: $RUN_LABEL
- Multi run label: $multi_run_label
- Started UTC: $started_utc
- Finished UTC: $finished_utc
- Configured duration minutes: $DURATION_MINUTES
- Command: scripts/start_4pairs.sh --$MODE --profiles-dir $PROFILES_DIR
- Stop reason: $stop_reason
- Stopped by user: $stopped_by_user
- Process exit code: $process_exit_code
- Telemetry lines observed: $telemetry_lines_observed
- Gate overall pass: $overall_pass

## Gate Checks
- telemetry_observed: $gate_telemetry_observed
- depth_events_observed: $gate_depth_events_observed
- loss_rate_pass: $gate_loss_rate_pass (<= $LOSS_RATE_MAX)
- p99_latency_pass: $gate_p99_latency_pass (<= $P99_LATENCY_MAX_MS ms)

## Outputs
- stdout log: $STDOUT_LOG
- stderr log: $STDERR_LOG
- json report: $REPORT_JSON
EOF

log_info "Soak JSON report written to $REPORT_JSON"
log_info "Soak markdown report written to $REPORT_MD"

exit $process_exit_code
