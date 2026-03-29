#!/bin/bash
#PBS -N graphmana-csv
#PBS -l ncpus=16
#PBS -l mem=64GB
#PBS -l walltime=4:00:00
#PBS -o graphmana_csv.log
#PBS -j oe

# GraphMana — Step 1: Generate CSV files from VCF (PBS/Torque)
#
# Usage:
#   qsub -v VCF_INPUT=/path/to/data.vcf.gz,POP_MAP=/path/to/pops.tsv pbs_prepare_csv.sh

set -euo pipefail

VCF_INPUT="${VCF_INPUT:?Set VCF_INPUT via qsub -v}"
POP_MAP="${POP_MAP:?Set POP_MAP via qsub -v}"
OUTPUT_DIR="${OUTPUT_DIR:-/scratch/$USER/graphmana_csv}"
THREADS="${NCPUS:-16}"

cd "$PBS_O_WORKDIR" 2>/dev/null || true

echo "=== GraphMana prepare-csv (PBS) ==="
echo "Input VCF:      $VCF_INPUT"
echo "Population map: $POP_MAP"
echo "Output dir:     $OUTPUT_DIR"
echo "Threads:        $THREADS"
echo "Started:        $(date)"

graphmana prepare-csv \
    --input "$VCF_INPUT" \
    --population-map "$POP_MAP" \
    --output-dir "$OUTPUT_DIR" \
    --threads "$THREADS" \
    --verbose

echo "=== Completed: $(date) ==="
