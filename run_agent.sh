#!/bin/zsh
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/opt/homebrew/opt/python@3.10/bin"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
unset TELEGRAM_BOT_TOKEN TELEGRAM_CHANNEL_ID

if [[ -n "${PYTHON_BIN:-}" ]]; then
  CANDIDATES=("$PYTHON_BIN")
else
  CANDIDATES=(
    "$SCRIPT_DIR/.venv/bin/python"
    python3.13
    python3.12
    python3.11
    python3.10
    /opt/homebrew/bin/python3.13
    /opt/homebrew/bin/python3.12
    /opt/homebrew/bin/python3.11
    /opt/homebrew/bin/python3.10
    python3
  )
fi

PYTHON_CMD=""
for candidate in "${CANDIDATES[@]}"; do
  if [[ "$candidate" = /* ]]; then
    [[ -x "$candidate" ]] || continue
    resolved="$candidate"
  else
    resolved="$(command -v "$candidate" 2>/dev/null || true)"
    [[ -n "$resolved" ]] || continue
  fi

  if "$resolved" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)' >/dev/null 2>&1; then
    PYTHON_CMD="$resolved"
    break
  fi
done

if [[ -z "$PYTHON_CMD" ]]; then
  echo "Python 3.10+ interpreter not found. Set PYTHON_BIN or install python3.10+." >&2
  exit 1
fi

exec "$PYTHON_CMD" "$SCRIPT_DIR/agent.py" >> /tmp/economy-agent.log 2>> /tmp/economy-agent.err.log
