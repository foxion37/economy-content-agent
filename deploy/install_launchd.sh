#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
APP_LABEL="com.barq.economy-agent"
BACKUP_LABEL="com.barq.economy-agent-backup"

mkdir -p "$LAUNCH_AGENTS_DIR"

cp "$SCRIPT_DIR/com.barq.economy-agent.plist" "$LAUNCH_AGENTS_DIR/$APP_LABEL.plist"
cp "$SCRIPT_DIR/com.barq.economy-agent-backup.plist" "$LAUNCH_AGENTS_DIR/$BACKUP_LABEL.plist"

chmod 644 "$LAUNCH_AGENTS_DIR/$APP_LABEL.plist" "$LAUNCH_AGENTS_DIR/$BACKUP_LABEL.plist"

launchctl bootout "gui/$(id -u)/$APP_LABEL" 2>/dev/null || true
launchctl bootout "gui/$(id -u)/$BACKUP_LABEL" 2>/dev/null || true

launchctl bootstrap "gui/$(id -u)" "$LAUNCH_AGENTS_DIR/$APP_LABEL.plist"
launchctl bootstrap "gui/$(id -u)" "$LAUNCH_AGENTS_DIR/$BACKUP_LABEL.plist"

echo "[launchd] installed:"
echo "  - $LAUNCH_AGENTS_DIR/$APP_LABEL.plist"
echo "  - $LAUNCH_AGENTS_DIR/$BACKUP_LABEL.plist"

echo "[launchd] loaded labels:"
echo "  - $APP_LABEL"
echo "  - $BACKUP_LABEL"
