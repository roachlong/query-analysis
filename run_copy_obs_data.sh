#!/bin/bash
set -euo pipefail
LOG=/var/log/crdb/copy_obs_data.log

set -a
. /etc/copy_obs_data.env
set +a

{
# Simple lock so runs don’t overlap
LOCKDIR="/tmp/copy_obs_data.lock"
if mkdir "$LOCKDIR" 2>/dev/null; then
  trap 'rmdir "$LOCKDIR"' EXIT
  echo "==== $(date -u '+%Y-%m-%dT%H:%M:%SZ') starting ===="
  echo "DSN=${CRDB_DSN:-<unset>}"
  # Use absolute paths in cron
  /opt/anaconda3/bin/python -c "import sys; print(sys.executable)"
  /opt/anaconda3/bin/python /usr/local/bin/copy_obs_data.py
  echo "exit code: $?"
  echo "==== $(date -u '+%Y-%m-%dT%H:%M:%SZ') done ===="
else
  echo "==== $(date -u '+%Y-%m-%dT%H:%M:%SZ') skipped: previous run still active ===="
fi
} >> "$LOG" 2>&1
