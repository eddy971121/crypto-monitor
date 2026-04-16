#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILES_DIR="${WORKSPACE_ROOT}/env/profiles"
TARGET_DATE="$(date -u +%Y-%m-%d)"
RUN_LABEL="$(date -u +%Y%m%d-%H%M%S)"
MODE="debug"
REQUIRE_UPLOAD_EVENTS=true

usage() {
  cat <<'USAGE'
usage: scripts/verify_s3_upload_4pairs.sh [options]

Options:
  --workspace-root DIR      Workspace root (default: repo root)
  --profiles-dir DIR        Profiles directory (default: env/profiles)
  --date YYYY-MM-DD         Target UTC date for upload-once (default: today)
  --run-label LABEL         Output report label (default: UTC timestamp)
  --release                 Use release binary
  --debug                   Use debug binary (default)
  --allow-noop              Treat zero-upload runs as pass when command succeeds
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workspace-root)
      WORKSPACE_ROOT="${2:?missing value for --workspace-root}"
      shift 2
      ;;
    --profiles-dir)
      PROFILES_DIR="${2:?missing value for --profiles-dir}"
      shift 2
      ;;
    --date)
      TARGET_DATE="${2:?missing value for --date}"
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
    --allow-noop)
      REQUIRE_UPLOAD_EVENTS=false
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

if ! [[ "$TARGET_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "--date must be in YYYY-MM-DD format" >&2
  exit 2
fi

WORKSPACE_ROOT="$(cd "$WORKSPACE_ROOT" && pwd)"
if [[ ! -d "$PROFILES_DIR" ]]; then
  echo "profiles directory not found: $PROFILES_DIR" >&2
  exit 1
fi

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

REPORT_DIR="${WORKSPACE_ROOT}/logs/s3-upload-check/${RUN_LABEL}"
mkdir -p "$REPORT_DIR"
REPORT_JSON="${REPORT_DIR}/s3-upload-report.json"
REPORT_MD="${REPORT_DIR}/s3-upload-report.md"

BUILD_ARGS=()
BIN_PATH="${WORKSPACE_ROOT}/target/debug/crypto-monitor"
if [[ "$MODE" == "release" ]]; then
  BUILD_ARGS+=(--release)
  BIN_PATH="${WORKSPACE_ROOT}/target/release/crypto-monitor"
fi

echo "Building binary once (${MODE}) before upload verification..."
(
  cd "$WORKSPACE_ROOT"
  cargo build "${BUILD_ARGS[@]}"
)

if [[ ! -x "$BIN_PATH" ]]; then
  echo "expected binary not found after build: $BIN_PATH" >&2
  exit 1
fi

mapfile -t PROFILE_FILES < <(find "$PROFILES_DIR" -maxdepth 1 -type f -name '*.env' | sort)
if [[ ${#PROFILE_FILES[@]} -eq 0 ]]; then
  echo "no profile files found in $PROFILES_DIR" >&2
  exit 1
fi

profiles_total=0
profiles_pass=0
profile_entries_file="${REPORT_DIR}/profiles.json.part"
: >"$profile_entries_file"
first_entry=true

for profile_file in "${PROFILE_FILES[@]}"; do
  profile_name="$(basename "$profile_file" .env)"
  profiles_total=$((profiles_total + 1))
  profile_log="${REPORT_DIR}/${profile_name}.log"

  env_dump="$({
    set -a
    if [[ -f "${WORKSPACE_ROOT}/.env" ]]; then
      # shellcheck disable=SC1091
      source "${WORKSPACE_ROOT}/.env"
    fi
    # shellcheck disable=SC1091
    source "$profile_file"
    set +a

    printf '%s\t%s\t%s\t%s\n' \
      "${APP_EXCHANGE:-binance}" \
      "${APP_MARKET_NAME:-unknown}" \
      "${APP_SYMBOL:-unknown}" \
      "${APP_S3_BUCKET:-}"
  })"

  IFS=$'\t' read -r exchange market_name symbol s3_bucket <<<"$env_dump"

  s3_config_present=false
  if [[ -n "$s3_bucket" ]]; then
    s3_config_present=true
  fi

  set +e
  (
    cd "$WORKSPACE_ROOT"
    set -a
    if [[ -f "${WORKSPACE_ROOT}/.env" ]]; then
      # shellcheck disable=SC1091
      source "${WORKSPACE_ROOT}/.env"
    fi
    # shellcheck disable=SC1091
    source "$profile_file"
    set +a

    "$BIN_PATH" upload-once --date "$TARGET_DATE"
  ) >"$profile_log" 2>&1
  command_rc=$?
  set -e

  raw_part_uploads="$(grep -c "uploaded raw bookTicker parquet part" "$profile_log" || true)"
  depth_part_uploads="$(grep -c "uploaded raw depth-delta parquet part" "$profile_log" || true)"
  snapshot_part_uploads="$(grep -c "uploaded raw snapshot parquet part" "$profile_log" || true)"
  metrics_uploads="$(grep -c "uploaded metrics jsonl file and removed local source" "$profile_log" || true)"
  legacy_uploads="$(grep -c "uploaded legacy raw bookTicker parquet and removed local source files" "$profile_log" || true)"

  upload_events=$((raw_part_uploads + depth_part_uploads + snapshot_part_uploads + metrics_uploads + legacy_uploads))

  put_object_failures="$(grep -c "S3 put_object failed\|head_object verification failed\|uploaded object size mismatch" "$profile_log" || true)"

  profile_pass=false
  if [[ "$s3_config_present" == true ]] && (( command_rc == 0 )) && (( put_object_failures == 0 )); then
    if [[ "$REQUIRE_UPLOAD_EVENTS" == true ]]; then
      if (( upload_events > 0 )); then
        profile_pass=true
      fi
    else
      profile_pass=true
    fi
  fi

  if [[ "$profile_pass" == true ]]; then
    profiles_pass=$((profiles_pass + 1))
  fi

  if [[ "$first_entry" == false ]]; then
    printf ',\n' >>"$profile_entries_file"
  fi
  first_entry=false

  cat >>"$profile_entries_file" <<EOF
    {
      "profile": "$(json_escape "$profile_name")",
      "exchange": "$(json_escape "$exchange")",
      "market": "$(json_escape "$market_name")",
      "symbol": "$(json_escape "$symbol")",
      "target_date": "$(json_escape "$TARGET_DATE")",
      "s3_config_present": $s3_config_present,
      "command_exit_code": $command_rc,
      "raw_part_uploads": $raw_part_uploads,
      "depth_part_uploads": $depth_part_uploads,
      "snapshot_part_uploads": $snapshot_part_uploads,
      "metrics_uploads": $metrics_uploads,
      "legacy_uploads": $legacy_uploads,
      "upload_events": $upload_events,
      "put_object_failures": $put_object_failures,
      "log_file": "$(json_escape "$profile_log")",
      "overall_pass": $profile_pass
    }
EOF

done

overall_pass=false
if (( profiles_total > 0 && profiles_pass == profiles_total )); then
  overall_pass=true
fi

cat >"$REPORT_JSON" <<EOF
{
  "run_label": "$(json_escape "$RUN_LABEL")",
  "workspace_root": "$(json_escape "$WORKSPACE_ROOT")",
  "profiles_dir": "$(json_escape "$PROFILES_DIR")",
  "target_date": "$(json_escape "$TARGET_DATE")",
  "mode": "$(json_escape "$MODE")",
  "require_upload_events": $REQUIRE_UPLOAD_EVENTS,
  "profiles": [
$(cat "$profile_entries_file")
  ],
  "gate": {
    "profile_count": $profiles_total,
    "profiles_passed": $profiles_pass,
    "overall_pass": $overall_pass
  }
}
EOF

cat >"$REPORT_MD" <<EOF
# S3 Upload Verification Report

- Run label: $RUN_LABEL
- Target date: $TARGET_DATE
- Mode: $MODE
- Require upload events: $REQUIRE_UPLOAD_EVENTS
- Profiles passed: $profiles_pass / $profiles_total
- Overall pass: $overall_pass

## Outputs
- JSON report: $REPORT_JSON
- Markdown report: $REPORT_MD
EOF

echo "S3 upload JSON report written to $REPORT_JSON"
echo "S3 upload markdown report written to $REPORT_MD"

if [[ "$overall_pass" == true ]]; then
  exit 0
fi

exit 2
