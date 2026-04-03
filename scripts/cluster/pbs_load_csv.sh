#!/bin/bash
#PBS -N graphmana-load
#PBS -l ncpus=4
#PBS -l mem=128GB
#PBS -l walltime=2:00:00
#PBS -o graphmana_load.log
#PBS -j oe

# GraphMana — Step 2: Load pre-generated CSVs into Neo4j (PBS/Torque)
#
# This requires Neo4j to be available. Run on a node with local SSD
# for the Neo4j data directory.
#
# Usage:
#   qsub -v CSV_DIR=/scratch/user/graphmana_csv,NEO4J_HOME=/home/user/neo4j pbs_load_csv.sh

set -euo pipefail

CSV_DIR="${CSV_DIR:?Set CSV_DIR via qsub -v CSV_DIR=...}"
NEO4J_HOME="${NEO4J_HOME:?Set NEO4J_HOME via qsub -v NEO4J_HOME=...}"
NEO4J_DATA="${NEO4J_DATA:-/scratch/$USER/graphmana_db}"

echo "=== GraphMana load-csv (PBS) ==="
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
