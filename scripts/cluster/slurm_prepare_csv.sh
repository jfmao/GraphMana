#!/bin/bash
#SBATCH --job-name=graphmana-csv
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=4:00:00
#SBATCH --output=graphmana_csv_%j.log

# GraphMana — Step 1: Generate CSV files from VCF (no Neo4j needed)
#
# This runs on any compute node. Embarrassingly parallel by chromosome.
# Adjust --cpus-per-task and --threads to match.
#
# Usage:
#   sbatch slurm_prepare_csv.sh /path/to/data.vcf.gz /path/to/pops.tsv

set -euo pipefail

VCF_INPUT="${1:?Usage: sbatch slurm_prepare_csv.sh <vcf> <population_map>}"
POP_MAP="${2:?Usage: sbatch slurm_prepare_csv.sh <vcf> <population_map>}"
OUTPUT_DIR="${3:-/scratch/$USER/graphmana_csv}"

echo "=== GraphMana prepare-csv ==="
echo "Input VCF:      $VCF_INPUT"
echo "Population map: $POP_MAP"
echo "Output dir:     $OUTPUT_DIR"
echo "Threads:        $SLURM_CPUS_PER_TASK"
echo "Started:        $(date)"

graphmana prepare-csv \
    --input "$VCF_INPUT" \
    --population-map "$POP_MAP" \
    --output-dir "$OUTPUT_DIR" \
    --threads "$SLURM_CPUS_PER_TASK" \
    --verbose

echo "=== Completed: $(date) ==="
echo "CSV files written to: $OUTPUT_DIR"
ls -lh "$OUTPUT_DIR"
