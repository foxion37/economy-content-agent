#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/backup.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[restore] missing env file: $ENV_FILE" >&2
  exit 1
fi

source "$ENV_FILE"

: "${PROJECT_DIR:?PROJECT_DIR is required}"
: "${NAS_BACKUP_DIR:?NAS_BACKUP_DIR is required}"

RESTORE_SOURCE="${1:-$NAS_BACKUP_DIR/latest}"

if [[ ! -d "$RESTORE_SOURCE" ]]; then
  echo "[restore] missing restore source: $RESTORE_SOURCE" >&2
  exit 1
fi

mkdir -p "$PROJECT_DIR"

rsync -a \
  "$RESTORE_SOURCE/.env" \
  "$RESTORE_SOURCE/credentials.json" \
  "$RESTORE_SOURCE/failed_url_queue.sqlite3" \
  "$RESTORE_SOURCE/ops_events.jsonl" \
  "$RESTORE_SOURCE/person_review_memory.json" \
  "$RESTORE_SOURCE/CLAUDE_CODE_HANDOFF.md" \
  "$RESTORE_SOURCE/FINAL_ALGORITHM_SPEC.md" \
  "$RESTORE_SOURCE/ROADMAP_CHECKLIST.md" \
  "$RESTORE_SOURCE/backups" \
  "$PROJECT_DIR/"

echo "[restore] completed from: $RESTORE_SOURCE"
