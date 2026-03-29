#!/bin/bash
#SBATCH --job-name=graphmana-export
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=2:00:00
#SBATCH --output=graphmana_export_%j.log

# GraphMana — Export data from Neo4j graph
#
# Neo4j must be running. Start it first with graphmana neo4j-start,
# or use an interactive session.
#
# Usage:
#   sbatch slurm_export.sh treemix output.treemix.gz

set -euo pipefail

FORMAT="${1:?Usage: sbatch slurm_export.sh <format> <output_file> [extra_args...]}"
OUTPUT="${2:?Usage: sbatch slurm_export.sh <format> <output_file> [extra_args...]}"
shift 2

echo "=== GraphMana export ==="
echo "Format:         $FORMAT"
echo "Output:         $OUTPUT"
echo "Threads:        $SLURM_CPUS_PER_TASK"
echo "Extra args:     $*"
echo "Started:        $(date)"

graphmana export \
    --format "$FORMAT" \
    --output "$OUTPUT" \
    --threads "$SLURM_CPUS_PER_TASK" \
    "$@"

echo "=== Completed: $(date) ==="
ls -lh "$OUTPUT"*
