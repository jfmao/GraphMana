#!/bin/bash
#SBATCH --job-name=graphmana-load
#SBATCH --cpus-per-task=4
#SBATCH --mem=128G
#SBATCH --time=2:00:00
#SBATCH --output=graphmana_load_%j.log

# GraphMana — Step 2: Load pre-generated CSVs into Neo4j
#
# This requires Neo4j to be available. Run on a node with local SSD
# for the Neo4j data directory.
#
# Usage:
#   sbatch slurm_load_csv.sh /scratch/user/graphmana_csv /home/user/neo4j

set -euo pipefail

CSV_DIR="${1:?Usage: sbatch slurm_load_csv.sh <csv_dir> <neo4j_home>}"
NEO4J_HOME="${2:?Usage: sbatch slurm_load_csv.sh <csv_dir> <neo4j_home>}"
NEO4J_DATA="${3:-/scratch/$USER/graphmana_db}"

echo "=== GraphMana load-csv ==="
echo "CSV dir:        $CSV_DIR"
echo "Neo4j home:     $NEO4J_HOME"
echo "Neo4j data:     $NEO4J_DATA"
echo "Started:        $(date)"

# Check filesystem before proceeding
graphmana check-filesystem --neo4j-data-dir "$NEO4J_DATA"

graphmana load-csv \
    --csv-dir "$CSV_DIR" \
    --neo4j-home "$NEO4J_HOME" \
    --neo4j-data-dir "$NEO4J_DATA"

echo "=== Completed: $(date) ==="
