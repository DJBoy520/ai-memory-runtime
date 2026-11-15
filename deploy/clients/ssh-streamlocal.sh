#!/bin/sh
# Forward a local Unix socket to the Ubuntu adapter for a remote/Windows client.
# Keep this process running; the plugin configures the local path.
set -eu
LOCAL="${1:-/tmp/amr2-doubao.sock}"
REMOTE="${2:-/run/user/1000/amr2-doubao.sock}"
HOST="${AMR_SSH_HOST:-192.168.30.166}"
ssh -N -o ExitOnForwardFailure=yes -L "$LOCAL:$REMOTE" "$HOST"
