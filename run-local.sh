#!/bin/sh
# Local dev runner (macOS): pulls the Tractive password from Keychain when
# it isn't already set via .env — so credentials never live in a file.
#   one-time setup: security add-generic-password -s tractive -a <email> -w
cd "$(dirname "$0")" || exit 1
if [ -z "$TRACTIVE_PASSWORD" ] && ! grep -q '^TRACTIVE_PASSWORD=' .env 2>/dev/null; then
  TRACTIVE_PASSWORD="$(security find-generic-password -s tractive -w 2>/dev/null)" && export TRACTIVE_PASSWORD
  if [ -z "$TRACTIVE_EMAIL" ] && ! grep -q '^TRACTIVE_EMAIL=' .env 2>/dev/null; then
    TRACTIVE_EMAIL="$(security find-generic-password -s tractive 2>/dev/null | awk -F'"' '/"acct"/{print $4}')" && export TRACTIVE_EMAIL
  fi
fi
exec .venv/bin/python bridge.py "$@"
