#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/backup.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[backup] missing env file: $ENV_FILE" >&2
  exit 1
fi

source "$ENV_FILE"

: "${PROJECT_DIR:?PROJECT_DIR is required}"
: "${NAS_BACKUP_DIR:?NAS_BACKUP_DIR is required}"

if [[ ! -d "$PROJECT_DIR" ]]; then
  echo "[backup] missing project dir: $PROJECT_DIR" >&2
  exit 1
fi

if [[ ! -d "$NAS_BACKUP_DIR" ]]; then
  echo "[backup] missing NAS backup dir: $NAS_BACKUP_DIR" >&2
  exit 1
fi

STAMP="$(date '+%Y%m%d-%H%M%S')"
SNAPSHOT_DIR="$NAS_BACKUP_DIR/snapshots/$STAMP"
LATEST_DIR="$NAS_BACKUP_DIR/latest"
TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/economy-agent-backup.XXXXXX")"
cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

mkdir -p "$SNAPSHOT_DIR" "$LATEST_DIR"

# Prefer a consistent SQLite backup artifact when sqlite3 is available.
if [[ -f "$PROJECT_DIR/failed_url_queue.sqlite3" ]]; then
  if command -v sqlite3 >/dev/null 2>&1; then
    sqlite3 "$PROJECT_DIR/failed_url_queue.sqlite3" ".backup '$TEMP_DIR/failed_url_queue.sqlite3'"
  else
    cp "$PROJECT_DIR/failed_url_queue.sqlite3" "$TEMP_DIR/failed_url_queue.sqlite3"
  fi
fi

rsync -a \
  --delete \
  --exclude '__pycache__/' \
  --exclude '.DS_Store' \
  --exclude '.agent.py.swp' \
  --exclude 'agent.launchd.log' \
  --exclude 'agent.launchd.err.log' \
  --exclude 'agent.launchd.out.log' \
  --exclude 'agent.log' \
  --exclude '.git/' \
  "$PROJECT_DIR/.env" \
  "$PROJECT_DIR/credentials.json" \
  "$PROJECT_DIR/ops_events.jsonl" \
  "$PROJECT_DIR/person_review_memory.json" \
  "$PROJECT_DIR/CLAUDE_CODE_HANDOFF.md" \
  "$PROJECT_DIR/FINAL_ALGORITHM_SPEC.md" \
  "$PROJECT_DIR/ROADMAP_CHECKLIST.md" \
  "$PROJECT_DIR/backups" \
  "$TEMP_DIR/failed_url_queue.sqlite3" \
  "$SNAPSHOT_DIR/"

find "$SNAPSHOT_DIR" -maxdepth 2 -type f | sort > "$SNAPSHOT_DIR/manifest.txt"
printf '%s\n' "$STAMP" > "$LATEST_DIR/last_successful_backup.txt"
rsync -a --delete "$SNAPSHOT_DIR/" "$LATEST_DIR/"

echo "[backup] completed: $SNAPSHOT_DIR"
