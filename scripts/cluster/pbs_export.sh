#!/bin/bash
#PBS -N graphmana-export
#PBS -l ncpus=8
#PBS -l mem=64GB
#PBS -l walltime=2:00:00
#PBS -o graphmana_export.log
#PBS -j oe

# GraphMana — Export data from Neo4j graph (PBS/Torque)
#
# Neo4j must be running. Start it first with graphmana neo4j-start,
# or use an interactive session.
#
# Usage:
#   qsub -v FORMAT=treemix,OUTPUT=output.treemix.gz pbs_export.sh

set -euo pipefail

FORMAT="${FORMAT:?Set FORMAT via qsub -v FORMAT=...}"
OUTPUT="${OUTPUT:?Set OUTPUT via qsub -v OUTPUT=...}"

NCPUS="${PBS_NCPUS:-${NCPUS:-8}}"

echo "=== GraphMana export (PBS) ==="
echo "Format:         $FORMAT"
echo "Output:         $OUTPUT"
echo "Threads:        $NCPUS"
echo "Started:        $(date)"

graphmana export \
    --format "$FORMAT" \
    --output "$OUTPUT" \
    --threads "$NCPUS" \
    ${EXTRA_ARGS:-}

echo "=== Completed: $(date) ==="
ls -lh "$OUTPUT"*
