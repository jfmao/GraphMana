#!/usr/bin/env bash
# GraphMana Export Benchmark Suite — 1KGP Full-Scale
# Run from /mnt/data/GraphMana after Neo4j is up.
# Usage: bash results/bench/run_benchmarks.sh 2>&1 | tee results/bench/benchmark_run.log

set -euo pipefail

NEO4J_PASS="graphpop"
OUT="results/bench"
CONN="--neo4j-uri bolt://localhost:7687 --neo4j-user neo4j --neo4j-password $NEO4J_PASS"

echo "=== GraphMana Benchmark Suite ==="
echo "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

run_bench() {
    local label="$1"
    local cmd="$2"
    local log="${OUT}/${label}.log"
    echo "--- $label ---"
    echo "CMD: $cmd"
    /usr/bin/time -v bash -c "$cmd" 2>"$log"
    local exit_code=$?
    echo "Exit: $exit_code"
    if [ -f "$log" ]; then
        grep -E "Elapsed|Maximum resident" "$log" || true
    fi
    echo ""
}

# ── Phase 1: FAST PATH (all 22 chromosomes) ─────────────────────────────────

run_bench "treemix_allchr" \
    "graphmana export --format treemix \
     --output $OUT/treemix_1kgp_allchr.treemix.gz \
     $CONN 2>&1 | tee -a $OUT/treemix_allchr.log"

run_bench "sfs_dadi_3pop_folded" \
    "graphmana export --format sfs-dadi \
     --output $OUT/sfs_dadi_3pop_folded.fs \
     --sfs-populations YRI --sfs-populations CEU --sfs-populations CHB --sfs-folded \
     $CONN 2>&1"

run_bench "sfs_dadi_5pop_folded" \
    "graphmana export --format sfs-dadi \
     --output $OUT/sfs_dadi_5pop_folded.fs \
     --sfs-populations YRI --sfs-populations CEU --sfs-populations CHB --sfs-populations GIH --sfs-populations MXL --sfs-folded \
     $CONN 2>&1"

run_bench "sfs_fsc_3pop" \
    "graphmana export --format sfs-fsc \
     --output $OUT/sfs_fsc_3pop \
     --sfs-populations YRI --sfs-populations CEU --sfs-populations CHB \
     $CONN 2>&1"

run_bench "bed_allchr" \
    "graphmana export --format bed \
     --output $OUT/1kgp_allchr.bed \
     $CONN 2>&1"

run_bench "tsv_allchr" \
    "graphmana export --format tsv \
     --output $OUT/1kgp_af_allchr.tsv \
     --tsv-columns variantId chr pos ref alt af_total ac_total an_total \
     $CONN 2>&1"

# ── Phase 2: FULL PATH (chr22 only) ─────────────────────────────────────────

run_bench "vcf_chr22" \
    "graphmana export --format vcf \
     --output $OUT/1kgp_chr22.vcf.gz --output-type z \
     --chromosomes chr22 \
     $CONN 2>&1"

run_bench "plink_chr22_snp" \
    "graphmana export --format plink \
     --output $OUT/1kgp_chr22_plink \
     --chromosomes chr22 --filter-variant-type SNP \
     $CONN 2>&1"

run_bench "eigenstrat_chr22" \
    "graphmana export --format eigenstrat \
     --output $OUT/1kgp_chr22_eigenstrat \
     --chromosomes chr22 \
     $CONN 2>&1"

run_bench "treemix_chr22" \
    "graphmana export --format treemix \
     --output $OUT/treemix_1kgp_chr22.treemix.gz \
     --chromosomes chr22 \
     $CONN 2>&1"

echo "=== All benchmarks complete ==="
echo "Finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
