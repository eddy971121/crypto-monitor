#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ROOT="${ROOT_DIR}/logs/multi"
RUN_LABEL="latest"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-label)
      RUN_LABEL="${2:?missing value for --run-label}"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      echo "usage: scripts/status_4pairs.sh [--run-label LABEL]" >&2
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

RUNNING=0
STOPPED=0

printf 'status from %s\n' "${PIDS_FILE}"
while IFS=$'\t' read -r PID PROFILE LOG_FILE; do
  [[ -z "${PID}" || "${PID}" == \#* ]] && continue

  if kill -0 "${PID}" 2>/dev/null; then
    STATE="running"
    RUNNING=$((RUNNING + 1))
  else
    STATE="stopped"
    STOPPED=$((STOPPED + 1))
  fi

  printf '%-30s pid=%-8s state=%-8s log=%s\n' "${PROFILE}" "${PID}" "${STATE}" "${LOG_FILE}"
done <"${PIDS_FILE}"

echo "running=${RUNNING} stopped=${STOPPED}"
