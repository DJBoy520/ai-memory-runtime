#!/bin/sh
# Run once per client; the token stays in the environment/file, not in git.
set -eu
: "${AMR_TOKEN:?set AMR_TOKEN to the per-agent token}"
: "${AMR_CA:=/etc/amr/tls/ca.crt}"
NODE_EXTRA_CA_CERTS="$AMR_CA" claude mcp add --transport http amr \
  https://amr.internal:8443/mcp \
  --header "Authorization: Bearer $AMR_TOKEN"
