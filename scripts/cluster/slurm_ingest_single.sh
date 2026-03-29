#!/bin/bash
#SBATCH --job-name=graphmana-ingest
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=8:00:00
#SBATCH --output=graphmana_ingest_%j.log

# GraphMana — Combined import in one job (CSV generation + Neo4j load)
#
# This combines both steps. Neo4j is started and stopped automatically.
# Requires Neo4j to be installed in user space (see graphmana setup-neo4j).
#
# Usage:
#   sbatch slurm_ingest_single.sh /path/to/data.vcf.gz /path/to/pops.tsv

set -euo pipefail

VCF_INPUT="${1:?Usage: sbatch slurm_ingest_single.sh <vcf> <population_map>}"
POP_MAP="${2:?Usage: sbatch slurm_ingest_single.sh <vcf> <population_map>}"
NEO4J_HOME="${3:-$HOME/neo4j}"
NEO4J_DATA="${4:-/scratch/$USER/graphmana_db}"

echo "=== GraphMana ingest (single job) ==="
echo "Input VCF:      $VCF_INPUT"
echo "Population map: $POP_MAP"
echo "Neo4j home:     $NEO4J_HOME"
echo "Neo4j data:     $NEO4J_DATA"
echo "Threads:        $SLURM_CPUS_PER_TASK"
echo "Started:        $(date)"

# Check filesystem
graphmana check-filesystem --neo4j-data-dir "$NEO4J_DATA"

# Start Neo4j
graphmana neo4j-start --neo4j-home "$NEO4J_HOME" --data-dir "$NEO4J_DATA" --wait

# Run import
graphmana ingest \
    --input "$VCF_INPUT" \
    --population-map "$POP_MAP" \
    --threads "$SLURM_CPUS_PER_TASK" \
    --verbose

# Stop Neo4j
graphmana neo4j-stop --neo4j-home "$NEO4J_HOME"

echo "=== Completed: $(date) ==="
