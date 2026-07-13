#!/bin/bash
set -e

pip install -r requirements.txt --quiet

# Push to GitHub and send a Telegram alert if it fails
PUSH_ERROR=$(git push origin main 2>&1) || {
    echo "[post-merge] GitHub push failed: $PUSH_ERROR"
    python3 scripts/notify_push_error.py "$PUSH_ERROR" || true
    exit 1
}

echo "[post-merge] GitHub push succeeded"
