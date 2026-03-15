#!/bin/zsh
set -euo pipefail

LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
APP_LABEL="com.barq.economy-agent"
BACKUP_LABEL="com.barq.economy-agent-backup"

launchctl bootout "gui/$(id -u)/$APP_LABEL" 2>/dev/null || true
launchctl bootout "gui/$(id -u)/$BACKUP_LABEL" 2>/dev/null || true

rm -f "$LAUNCH_AGENTS_DIR/$APP_LABEL.plist"
rm -f "$LAUNCH_AGENTS_DIR/$BACKUP_LABEL.plist"

echo "[launchd] removed:"
echo "  - $LAUNCH_AGENTS_DIR/$APP_LABEL.plist"
echo "  - $LAUNCH_AGENTS_DIR/$BACKUP_LABEL.plist"
