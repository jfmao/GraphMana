#!/bin/bash
# ==========================================================================
# GraphMana Single-Script Installer
#
# Installs GraphMana and all dependencies without admin privileges.
# Detects and installs conda (Miniforge) if not present.
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/jfmao/GraphMana/main/install.sh | bash
#   # or
#   bash install.sh
# ==========================================================================
set -euo pipefail

GRAPHMANA_ENV="graphmana"
MINIFORGE_VERSION="24.11.3-0"

echo "============================================"
echo "  GraphMana Installer"
echo "============================================"
echo ""

# --- Step 1: Check for conda ---
if command -v conda &>/dev/null; then
    echo "[1/4] conda found: $(conda --version)"
else
    echo "[1/4] conda not found. Installing Miniforge..."
    PLATFORM=$(uname -s)
    ARCH=$(uname -m)

    if [ "$PLATFORM" = "Linux" ]; then
        INSTALLER="Miniforge3-${MINIFORGE_VERSION}-Linux-${ARCH}.sh"
    elif [ "$PLATFORM" = "Darwin" ]; then
        INSTALLER="Miniforge3-${MINIFORGE_VERSION}-MacOSX-${ARCH}.sh"
    else
        echo "Error: Unsupported platform $PLATFORM"
        exit 1
    fi

    INSTALLER_URL="https://github.com/conda-forge/miniforge/releases/download/${MINIFORGE_VERSION}/${INSTALLER}"
    echo "  Downloading ${INSTALLER}..."
    curl -sSL -o "/tmp/${INSTALLER}" "${INSTALLER_URL}"
    bash "/tmp/${INSTALLER}" -b -p "$HOME/miniforge3"
    rm -f "/tmp/${INSTALLER}"

    # Initialize conda
    eval "$($HOME/miniforge3/bin/conda shell.bash hook)"
    conda init bash 2>/dev/null || true
    echo "  Miniforge installed at $HOME/miniforge3"
fi

# Ensure conda is active
eval "$(conda shell.bash hook 2>/dev/null || $HOME/miniforge3/bin/conda shell.bash hook)"

# --- Step 2: Create environment ---
if conda env list | grep -q "^${GRAPHMANA_ENV} "; then
    echo "[2/4] Environment '${GRAPHMANA_ENV}' already exists."
else
    echo "[2/4] Creating conda environment '${GRAPHMANA_ENV}'..."
    conda create -n "${GRAPHMANA_ENV}" -c conda-forge -c bioconda \
        python=3.12 cyvcf2 numpy click pyliftover openjdk=21 \
        -y -q
fi

conda activate "${GRAPHMANA_ENV}"

# --- Step 3: Install GraphMana ---
echo "[3/4] Installing GraphMana..."
if pip show graphmana &>/dev/null; then
    echo "  GraphMana already installed. Upgrading..."
    pip install --upgrade graphmana 2>/dev/null || \
        pip install --upgrade git+https://github.com/jfmao/GraphMana.git#subdirectory=graphmana-cli
else
    pip install graphmana 2>/dev/null || \
        pip install git+https://github.com/jfmao/GraphMana.git#subdirectory=graphmana-cli
fi

# --- Step 4: Setup Neo4j ---
echo "[4/4] Setting up Neo4j..."
NEO4J_DIR="${HOME}/neo4j"

if [ -d "${NEO4J_DIR}" ] && [ -f "${NEO4J_DIR}/bin/neo4j" ]; then
    echo "  Neo4j already installed at ${NEO4J_DIR}"
else
    graphmana setup-neo4j --install-dir "${NEO4J_DIR}" --memory-auto
fi

echo ""
echo "============================================"
echo "  GraphMana Installation Complete!"
echo "============================================"
echo ""
echo "Quick start:"
echo "  conda activate graphmana"
echo "  graphmana version"
echo "  graphmana ingest --help"
echo ""
echo "To start Neo4j:"
echo "  graphmana neo4j-start --neo4j-home ~/neo4j --wait"
echo ""
echo "Full documentation:"
echo "  https://github.com/jfmao/GraphMana/tree/main/docs"
echo ""
