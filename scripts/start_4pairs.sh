#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILES_DIR="${ROOT_DIR}/env/profiles"
MODE="release"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --debug)
      MODE="debug"
      shift
      ;;
    --release)
      MODE="release"
      shift
      ;;
    --profiles-dir)
      PROFILES_DIR="${2:?missing value for --profiles-dir}"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      echo "usage: scripts/start_4pairs.sh [--release|--debug] [--profiles-dir DIR]" >&2
      exit 2
      ;;
  esac
done

if [[ ! -d "${PROFILES_DIR}" ]]; then
  echo "profiles directory not found: ${PROFILES_DIR}" >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required but was not found on PATH" >&2
  exit 127
fi

BUILD_ARGS=()
BIN_PATH="${ROOT_DIR}/target/debug/crypto-monitor"
if [[ "${MODE}" == "release" ]]; then
  BUILD_ARGS+=(--release)
  BIN_PATH="${ROOT_DIR}/target/release/crypto-monitor"
fi

echo "building binary once (${MODE}) before launching profiles..."
cargo build "${BUILD_ARGS[@]}"

if [[ ! -x "${BIN_PATH}" ]]; then
  echo "expected binary not found after build: ${BIN_PATH}" >&2
  exit 1
fi

mapfile -t PROFILE_FILES < <(find "${PROFILES_DIR}" -maxdepth 1 -type f -name '*.env' | sort)
if [[ ${#PROFILE_FILES[@]} -eq 0 ]]; then
  echo "no profile files were found under ${PROFILES_DIR}" >&2
  exit 1
fi

RUN_LABEL="$(date -u +%Y%m%d-%H%M%S)"
RUN_DIR="${ROOT_DIR}/logs/multi/${RUN_LABEL}"
mkdir -p "${RUN_DIR}"
PIDS_FILE="${RUN_DIR}/pids.tsv"

{
  echo "# run_label=${RUN_LABEL}"
  printf '# pid\tprofile\tlog_file\n'
} >"${PIDS_FILE}"

for PROFILE_FILE in "${PROFILE_FILES[@]}"; do
  PROFILE_NAME="$(basename "${PROFILE_FILE}" .env)"
  LOG_FILE="${RUN_DIR}/${PROFILE_NAME}.log"

  (
    set -a
    if [[ -f "${ROOT_DIR}/.env" ]]; then
      # shellcheck disable=SC1091
      source "${ROOT_DIR}/.env"
    fi
    # shellcheck disable=SC1091
    source "${PROFILE_FILE}"
    set +a

    "${BIN_PATH}"
  ) >"${LOG_FILE}" 2>&1 &

  PID="$!"
  printf '%s\t%s\t%s\n' "${PID}" "${PROFILE_NAME}" "${LOG_FILE}" >>"${PIDS_FILE}"
  printf 'started %-30s pid=%s\n' "${PROFILE_NAME}" "${PID}"
done

ln -sfn "${RUN_DIR}" "${ROOT_DIR}/logs/multi/latest"

echo "run_label=${RUN_LABEL}"
echo "pids_file=${PIDS_FILE}"
echo "latest_logs_link=${ROOT_DIR}/logs/multi/latest"
