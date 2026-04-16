#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILES_DIR="${WORKSPACE_ROOT}/env/profiles"
TARGET_DATE="$(date -u +%Y-%m-%d)"
RUN_LABEL="$(date -u +%Y%m%d-%H%M%S)"
REQUIRE_S3_CONFIG=false

usage() {
  cat <<'USAGE'
usage: scripts/verify_archive_integrity_4pairs.sh [options]

Options:
  --workspace-root DIR    Workspace root (default: repo root)
  --profiles-dir DIR      Profiles directory (default: env/profiles)
  --date YYYY-MM-DD       Target UTC date to verify (default: today)
  --run-label LABEL       Output report label (default: UTC timestamp)
  --require-s3-config     Fail profiles with empty APP_S3_BUCKET
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
    --require-s3-config)
      REQUIRE_S3_CONFIG=true
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

REPORT_DIR="${WORKSPACE_ROOT}/logs/archive-check/${RUN_LABEL}"
mkdir -p "$REPORT_DIR"
REPORT_JSON="${REPORT_DIR}/archive-integrity-report.json"
REPORT_MD="${REPORT_DIR}/archive-integrity-report.md"

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

  env_dump="$({
    set -a
    if [[ -f "${WORKSPACE_ROOT}/.env" ]]; then
      # shellcheck disable=SC1091
      source "${WORKSPACE_ROOT}/.env"
    fi
    # shellcheck disable=SC1091
    source "$profile_file"
    set +a

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "${APP_EXCHANGE:-binance}" \
      "${APP_MARKET:-unknown}" \
      "${APP_SYMBOL:-unknown}" \
      "${APP_DATA_ROOT:-data}" \
      "${APP_RAW_SPOOL_DIR:-}" \
      "${APP_METRICS_DIR:-}" \
      "${APP_S3_BUCKET:-}"
  })"

  IFS=$'\t' read -r exchange market symbol data_root raw_spool_dir metrics_dir s3_bucket <<<"$env_dump"

  if [[ -z "$raw_spool_dir" ]]; then
    raw_spool_dir="${data_root}/raw"
  fi
  if [[ -z "$metrics_dir" ]]; then
    metrics_dir="${data_root}/metrics"
  fi

  stream_symbol="$(printf '%s' "$symbol" | tr '[:upper:]' '[:lower:]')"
  partition_dir="${WORKSPACE_ROOT}/${raw_spool_dir}/exchange=${exchange}/symbol=${stream_symbol}/date=${TARGET_DATE}"

  book_manifest="${partition_dir}/bookticker_manifest.json"
  depth_manifest="${partition_dir}/depth-delta_manifest.json"
  snapshot_manifest="${partition_dir}/snapshot_manifest.json"

  partition_exists=false
  book_manifest_exists=false
  depth_manifest_exists=false
  snapshot_manifest_exists=false
  parquet_parts_count=0
  metrics_file_exists=false
  s3_config_present=false

  if [[ -d "$partition_dir" ]]; then
    partition_exists=true
    parquet_parts_count="$(find "$partition_dir" -maxdepth 1 -type f -name '*part-*.parquet' | wc -l | tr -d ' ')"
  fi

  [[ -f "$book_manifest" ]] && book_manifest_exists=true
  [[ -f "$depth_manifest" ]] && depth_manifest_exists=true
  [[ -f "$snapshot_manifest" ]] && snapshot_manifest_exists=true

  metrics_new="${WORKSPACE_ROOT}/${metrics_dir}/metrics-${exchange}-${market}-${stream_symbol}-${TARGET_DATE}.jsonl"
  metrics_legacy="${WORKSPACE_ROOT}/${metrics_dir}/metrics-${TARGET_DATE}.jsonl"
  if [[ -f "$metrics_new" || -f "$metrics_legacy" ]]; then
    metrics_file_exists=true
  fi

  if [[ -n "$s3_bucket" ]]; then
    s3_config_present=true
  fi

  profile_pass=false
  if [[ "$partition_exists" == true && "$book_manifest_exists" == true && "$depth_manifest_exists" == true && "$snapshot_manifest_exists" == true && "$metrics_file_exists" == true && "$parquet_parts_count" -gt 0 ]]; then
    profile_pass=true
  fi
  if [[ "$REQUIRE_S3_CONFIG" == true && "$s3_config_present" == false ]]; then
    profile_pass=false
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
      "market": "$(json_escape "$market")",
      "symbol": "$(json_escape "$symbol")",
      "stream_symbol": "$(json_escape "$stream_symbol")",
      "target_date": "$(json_escape "$TARGET_DATE")",
      "partition_dir": "$(json_escape "$partition_dir")",
      "partition_exists": $partition_exists,
      "book_manifest_exists": $book_manifest_exists,
      "depth_manifest_exists": $depth_manifest_exists,
      "snapshot_manifest_exists": $snapshot_manifest_exists,
      "parquet_parts_count": $parquet_parts_count,
      "metrics_file_exists": $metrics_file_exists,
      "s3_config_present": $s3_config_present,
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
  "require_s3_config": $REQUIRE_S3_CONFIG,
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
# Archive Integrity Check Report

- Run label: $RUN_LABEL
- Target date: $TARGET_DATE
- Require S3 config: $REQUIRE_S3_CONFIG
- Profiles passed: $profiles_pass / $profiles_total
- Overall pass: $overall_pass

## Outputs
- JSON report: $REPORT_JSON
- Markdown report: $REPORT_MD
EOF

echo "Archive integrity JSON report written to $REPORT_JSON"
echo "Archive integrity markdown report written to $REPORT_MD"

if [[ "$overall_pass" == true ]]; then
  exit 0
fi

exit 2
