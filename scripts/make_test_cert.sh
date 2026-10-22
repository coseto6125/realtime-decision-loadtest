#!/usr/bin/env bash
# Test-only self-signed certificate for the T7 TLS terminator (tls/*.pem is git-ignored).
set -euo pipefail
cd "$(dirname "$0")/../tls"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 7 \
  -subj "/CN=lt-model-tls" -addext "subjectAltName=DNS:lt-model-tls" -keyout key.pem -out cert.pem 2>/dev/null
chmod 644 key.pem cert.pem
