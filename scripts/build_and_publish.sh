#!/bin/bash
# ==========================================================================
# GraphMana Build and Publish Script
#
# Builds the Java JAR, bundles it, builds the Python wheel, and optionally
# uploads to PyPI.
#
# Usage:
#   bash scripts/build_and_publish.sh          # build only
#   bash scripts/build_and_publish.sh --upload  # build + upload to PyPI
# ==========================================================================
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLI_DIR="$PROJECT_ROOT/graphmana-cli"
JAVA_DIR="$PROJECT_ROOT/graphmana-procedures"
DATA_DIR="$CLI_DIR/src/graphmana/data"

echo "=== GraphMana Build ==="
echo "Project root: $PROJECT_ROOT"
echo ""

# Step 1: Build Java procedures JAR
echo "[1/4] Building Java procedures JAR..."
if command -v mvn &>/dev/null; then
    cd "$JAVA_DIR"
    mvn clean package -DskipTests -q
    echo "  JAR built: $(ls target/graphmana-procedures-*.jar | head -1)"
else
    echo "  WARNING: Maven not found. Using existing JAR (if available)."
fi

# Step 2: Bundle JAR in Python package
echo "[2/4] Bundling JAR..."
mkdir -p "$DATA_DIR"
if ls "$JAVA_DIR/target/graphmana-procedures-"*.jar &>/dev/null 2>&1; then
    cp "$JAVA_DIR/target/graphmana-procedures-"*.jar "$DATA_DIR/graphmana-procedures.jar"
    echo "  Bundled: $DATA_DIR/graphmana-procedures.jar ($(du -h "$DATA_DIR/graphmana-procedures.jar" | cut -f1))"
elif [ -f "$DATA_DIR/graphmana-procedures.jar" ]; then
    echo "  Using existing bundled JAR."
else
    echo "  ERROR: No JAR found. Build with Maven first."
    exit 1
fi

# Step 3: Build Python wheel
echo "[3/4] Building Python wheel..."
cd "$CLI_DIR"
pip install --quiet build
python -m build --wheel --outdir "$PROJECT_ROOT/dist/"
echo "  Wheel: $(ls "$PROJECT_ROOT/dist/"*.whl | tail -1)"

# Step 4: Upload (if --upload flag)
if [ "${1:-}" = "--upload" ]; then
    echo "[4/4] Uploading to PyPI..."
    pip install --quiet twine
    twine upload "$PROJECT_ROOT/dist/"*
    echo "  Uploaded to PyPI."
else
    echo "[4/4] Skipping upload (use --upload to publish to PyPI)."
fi

echo ""
echo "=== Build Complete ==="
echo "To install locally: pip install $PROJECT_ROOT/dist/graphmana-*.whl"
echo "To upload to PyPI:  bash scripts/build_and_publish.sh --upload"
