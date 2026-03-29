#!/bin/bash
set -e

# Start Neo4j in the background using its official entrypoint
/startup/docker-entrypoint.sh neo4j &
NEO4J_PID=$!

# Wait for Neo4j to become ready (up to 120 seconds)
echo "Waiting for Neo4j to start..."
for i in $(seq 1 120); do
    if cypher-shell -u neo4j -p graphmana "RETURN 1" >/dev/null 2>&1; then
        echo "Neo4j is ready."
        break
    fi
    if [ "$i" -eq 120 ]; then
        echo "ERROR: Neo4j did not start within 120 seconds."
        exit 1
    fi
    sleep 1
done

# Check if the database already has data
VARIANT_COUNT=$(cypher-shell -u neo4j -p graphmana \
    "MATCH (v:Variant) RETURN count(v) AS c" --format plain 2>/dev/null | tail -1 || echo "0")

if [ "$VARIANT_COUNT" = "0" ] || [ -z "$VARIANT_COUNT" ]; then
    echo "Empty database detected. Loading demo data..."
    graphmana ingest \
        --input /demo_data/demo_chr22.vcf \
        --population-map /demo_data/population_map.tsv \
        --neo4j-home /var/lib/neo4j \
        --reference GRCh38 \
        --neo4j-uri bolt://localhost:7687 \
        --neo4j-user neo4j \
        --neo4j-password graphmana \
        --verbose
    echo "Demo data loaded successfully."
    graphmana status \
        --neo4j-uri bolt://localhost:7687 \
        --neo4j-user neo4j \
        --neo4j-password graphmana
else
    echo "Database already contains $VARIANT_COUNT variants. Skipping demo import."
fi

# Keep the container alive by waiting on Neo4j
wait $NEO4J_PID
