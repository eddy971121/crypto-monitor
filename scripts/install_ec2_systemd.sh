#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_SOURCE="${ROOT_DIR}/deploy/systemd/crypto-monitor@.service"
PROFILES_DIR="${ROOT_DIR}/env/profiles"
SYSTEMD_DIR="/etc/systemd/system"
ENV_TARGET_DIR="/etc/crypto-monitor"
WORKING_DIR="/var/lib/crypto-monitor"
PROFILE_LIST=""
START_NOW=true

usage() {
  cat <<'USAGE'
usage: scripts/install_ec2_systemd.sh [options]

Options:
  --unit-source PATH        Source unit file (default: deploy/systemd/crypto-monitor@.service)
  --profiles-dir PATH       Source profile env directory (default: env/profiles)
  --systemd-dir PATH        systemd unit directory (default: /etc/systemd/system)
  --env-target-dir PATH     Destination env directory (default: /etc/crypto-monitor)
  --working-dir PATH        Runtime working directory (default: /var/lib/crypto-monitor)
  --profiles a,b,c          Comma-separated profile names (without .env); default is all env files
  --no-start                Enable units but do not start immediately
  -h, --help                Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --unit-source)
      UNIT_SOURCE="${2:?missing value for --unit-source}"
      shift 2
      ;;
    --profiles-dir)
      PROFILES_DIR="${2:?missing value for --profiles-dir}"
      shift 2
      ;;
    --systemd-dir)
      SYSTEMD_DIR="${2:?missing value for --systemd-dir}"
      shift 2
      ;;
    --env-target-dir)
      ENV_TARGET_DIR="${2:?missing value for --env-target-dir}"
      shift 2
      ;;
    --working-dir)
      WORKING_DIR="${2:?missing value for --working-dir}"
      shift 2
      ;;
    --profiles)
      PROFILE_LIST="${2:?missing value for --profiles}"
      shift 2
      ;;
    --no-start)
      START_NOW=false
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

if [[ ! -f "$UNIT_SOURCE" ]]; then
  echo "unit file not found: $UNIT_SOURCE" >&2
  exit 1
fi

if [[ ! -d "$PROFILES_DIR" ]]; then
  echo "profiles directory not found: $PROFILES_DIR" >&2
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl is required but was not found on PATH" >&2
  exit 127
fi

SUDO=""
if [[ "$(id -u)" -ne 0 ]]; then
  if ! command -v sudo >/dev/null 2>&1; then
    echo "this script needs root privileges (run as root or install sudo)" >&2
    exit 1
  fi
  SUDO="sudo"
fi

UNIT_TARGET="${SYSTEMD_DIR}/crypto-monitor@.service"

$SUDO install -D -m 0644 "$UNIT_SOURCE" "$UNIT_TARGET"
$SUDO mkdir -p "$ENV_TARGET_DIR" "$WORKING_DIR"
$SUDO cp "$PROFILES_DIR"/*.env "$ENV_TARGET_DIR/"
$SUDO systemctl daemon-reload

resolve_profiles() {
  if [[ -n "$PROFILE_LIST" ]]; then
    IFS=',' read -r -a profiles <<<"$PROFILE_LIST"
    printf '%s\n' "${profiles[@]}"
    return
  fi

  local env_file
  for env_file in "$PROFILES_DIR"/*.env; do
    [[ -f "$env_file" ]] || continue
    basename "$env_file" .env
  done
}

mapfile -t PROFILES < <(resolve_profiles)
if [[ ${#PROFILES[@]} -eq 0 ]]; then
  echo "no profiles resolved for installation" >&2
  exit 1
fi

echo "installed unit: $UNIT_TARGET"
echo "copied env files to: $ENV_TARGET_DIR"
echo "working directory: $WORKING_DIR"

for profile in "${PROFILES[@]}"; do
  unit_name="crypto-monitor@${profile}"
  if [[ "$START_NOW" == true ]]; then
    $SUDO systemctl enable --now "$unit_name"
    echo "enabled+started: $unit_name"
  else
    $SUDO systemctl enable "$unit_name"
    echo "enabled: $unit_name"
  fi
done

echo "done"
