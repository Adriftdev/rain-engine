#!/bin/bash
set -e

# Ensure typeshare-cli is installed
if ! command -v typeshare &> /dev/null
then
    echo "typeshare could not be found. Installing..."
    cargo install typeshare-cli
fi

echo "Creating SDK directories..."
mkdir -p sdk/typescript
mkdir -p sdk/python

echo "Applying temporary type replacements for typeshare AST parser..."
# Backup original files
cp rain-engine-core/src/types.rs rain-engine-core/src/types.rs.bak
cp rain-engine-runtime/src/lib.rs rain-engine-runtime/src/lib.rs.bak

# Replace unsupported types with supported ones just for the typeshare run
python3 -c '
import re

def replace_in_file(path):
    with open(path, "r") as f:
        content = f.read()
    
    replacements = {
        r"\busize\b": "u32",
        r"\bu64\b": "u32",
        r"\bi64\b": "u32",
        r"\bSystemTime\b": "String",
        r"\bInstant\b": "String",
        r"\bDuration\b": "u32",
        r"\bValue\b": "String",
    }
    
    for pat, rep in replacements.items():
        content = re.sub(pat, rep, content)
        
    with open(path, "w") as f:
        f.write(content)

replace_in_file("rain-engine-core/src/types.rs")
replace_in_file("rain-engine-runtime/src/lib.rs")
'

echo "Generating TypeScript SDK..."
if typeshare . --lang=typescript --output-file=sdk/typescript/rain-engine.ts; then
  echo "TypeScript SDK generated successfully."
  
  # Append missing generic types
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
else
  echo "TypeScript generation failed."
fi

echo "Generating Python SDK (Experimental)..."
if typeshare . --lang=python --output-file=sdk/python/rain_engine.py; then
  echo "Python SDK generated successfully."
else
  echo "Python generation failed (expected with complex enums). Skipping."
  rm -f sdk/python/rain_engine.py
fi

echo "Restoring original files..."
mv rain-engine-core/src/types.rs.bak rain-engine-core/src/types.rs
mv rain-engine-runtime/src/lib.rs.bak rain-engine-runtime/src/lib.rs

echo "SDK generation complete!"
