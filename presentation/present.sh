#!/usr/bin/env bash
# Serves the deck and opens it. The speaker-notes window needs a real HTTP origin,
# so opening index.html straight off disk will not work.
set -euo pipefail

PORT="${PORT:-8777}"
cd "$(dirname "$0")"

python3 -m http.server "$PORT" --bind 127.0.0.1 >/dev/null 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT

sleep 1
open "http://127.0.0.1:$PORT/index.html"

cat <<EOF

  Deck:  http://127.0.0.1:$PORT/index.html

  S            open the speaker-notes window (drag it to your laptop screen)
  F            fullscreen the deck on the projector
  arrows       next / previous
  ESC          slide overview

  Ctrl-C to stop the server.

EOF

wait $SERVER
