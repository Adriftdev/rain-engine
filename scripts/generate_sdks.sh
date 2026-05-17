#!/bin/bash
set -euo pipefail

# Non-mutating SDK generation. This script must never rewrite Rust sources as a
# preprocessing step; SDK drift should be fixed in stable DTOs or typeshare
# annotations instead.

if ! command -v typeshare > /dev/null 2>&1; then
    echo "typeshare could not be found. Install it with: cargo install typeshare-cli" >&2
    exit 1
fi

mkdir -p sdk/typescript sdk/python

echo "Generating TypeScript SDK..."
typeshare . --lang=typescript --output-file=sdk/typescript/rain-engine.ts

cat << 'EOF' >> sdk/typescript/rain-engine.ts

// Automatically injected wrapper types
export type Result<T, E> = { Ok: T } | { Err: E };
export type BTreeSet<T> = T[];
export type SocketAddr = string;

export type BlobBootstrapConfig =
  | { type: "InMemory" }
  | { type: "LocalDirectory", payload: { path: string } }
  | { type: "S3", payload: { bucket: string, region?: string } }
  | { type: "Gcs", payload: { bucket: string } };
EOF

echo "Generating Python SDK (experimental)..."
if ! typeshare . --lang=python --output-file=sdk/python/rain_engine.py; then
    echo "Python generation failed for complex enum shapes; leaving existing SDK untouched." >&2
    rm -f sdk/python/rain_engine.py
fi

echo "SDK generation complete."
