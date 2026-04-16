#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ROOT="${ROOT_DIR}/logs/multi"
RUN_LABEL="latest"
FORCE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-label)
      RUN_LABEL="${2:?missing value for --run-label}"
      shift 2
      ;;
    --force)
      FORCE=true
      shift
      ;;
    *)
      echo "unknown argument: $1" >&2
      echo "usage: scripts/stop_4pairs.sh [--run-label LABEL] [--force]" >&2
      exit 2
      ;;
  esac
done

if [[ "${RUN_LABEL}" == "latest" ]]; then
  PIDS_FILE="${RUN_ROOT}/latest/pids.tsv"
else
  PIDS_FILE="${RUN_ROOT}/${RUN_LABEL}/pids.tsv"
fi

if [[ ! -f "${PIDS_FILE}" ]]; then
  echo "pid file not found: ${PIDS_FILE}" >&2
  exit 1
fi

TERMINATED=0
ALREADY_STOPPED=0

while IFS=$'\t' read -r PID PROFILE LOG_FILE; do
  [[ -z "${PID}" || "${PID}" == \#* ]] && continue

  if kill -0 "${PID}" 2>/dev/null; then
    if [[ "${FORCE}" == true ]]; then
      kill -9 "${PID}" 2>/dev/null || true
      printf 'killed %-31s pid=%s\n' "${PROFILE}" "${PID}"
    else
      kill "${PID}" 2>/dev/null || true
      printf 'terminated %-27s pid=%s\n' "${PROFILE}" "${PID}"
    fi
    TERMINATED=$((TERMINATED + 1))
  else
    printf 'already stopped %-23s pid=%s\n' "${PROFILE}" "${PID}"
    ALREADY_STOPPED=$((ALREADY_STOPPED + 1))
  fi
done <"${PIDS_FILE}"

echo "terminated=${TERMINATED} already_stopped=${ALREADY_STOPPED}"
