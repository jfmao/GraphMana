#!/bin/bash
# GraphMana — Interactive session setup for PBS/Torque clusters
#
# This script is sourced (not submitted) to set up an interactive session
# with Neo4j running on a compute node.
#
# Usage:
#   qsub -I -l ncpus=16,mem=128GB,walltime=8:00:00
#   source pbs_interactive.sh [neo4j_home] [data_dir]
#
# After sourcing, Neo4j is running and you can use graphmana commands directly.
# When done, run: graphmana neo4j-stop --neo4j-home "$NEO4J_HOME"

NEO4J_HOME="${1:-$HOME/neo4j}"
NEO4J_DATA="${2:-/scratch/$USER/graphmana_db}"

echo "=== GraphMana Interactive Session (PBS) ==="
echo "Neo4j home:  $NEO4J_HOME"
echo "Data dir:    $NEO4J_DATA"
echo "Node:        $(hostname)"

# Check filesystem
graphmana check-filesystem --neo4j-data-dir "$NEO4J_DATA"

# Start Neo4j
graphmana neo4j-start --neo4j-home "$NEO4J_HOME" --data-dir "$NEO4J_DATA" --wait

echo ""
echo "Neo4j is running. Use graphmana commands freely."
echo "When done: graphmana neo4j-stop --neo4j-home $NEO4J_HOME"
echo ""

# Check database status
graphmana status
