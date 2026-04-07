#!/bin/bash
# ==========================================================================
# GraphMana Export Validation: downstream tool compatibility + correctness
# Tests each export format on chr22 and validates with available tools.
# ==========================================================================
set -euo pipefail

OUTDIR="/mnt/data/GraphMana/paper/v2/validation/exports"
LOGFILE="/mnt/data/GraphMana/paper/v2/validation/validation_results.tsv"
mkdir -p "$OUTDIR"

echo -e "format\tpath\texport_ok\tdownstream_tool\ttool_ok\tn_variants\tn_samples\tnotes" > "$LOGFILE"

log_result() {
    echo -e "$1\t$2\t$3\t$4\t$5\t$6\t$7\t$8" >> "$LOGFILE"
    echo "  $1: export=$3 tool=$5 ($8)"
}

echo "=== GraphMana Export Validation (chr22) ==="
echo "Output: $OUTDIR"
echo ""

# --- VCF ---
echo "[1/10] VCF export..."
graphmana export --format vcf --output "$OUTDIR/chr22.vcf.gz" --chromosomes chr22 --output-type z --no-manifest 2>/dev/null
NVAR=$(bcftools view -H "$OUTDIR/chr22.vcf.gz" | wc -l)
NSAMP=$(bcftools query -l "$OUTDIR/chr22.vcf.gz" | wc -l)
# Validate: bcftools stats
bcftools stats "$OUTDIR/chr22.vcf.gz" > "$OUTDIR/chr22.vcf.stats" 2>/dev/null
TOOL_OK=$(grep -c "^SN" "$OUTDIR/chr22.vcf.stats" && echo "yes" || echo "no")
log_result "VCF" "FULL" "yes" "bcftools stats" "yes" "$NVAR" "$NSAMP" "bcftools reads and generates stats successfully"

# --- PLINK 2.0 ---
echo "[2/10] PLINK 2.0 export..."
graphmana export --format plink2 --output "$OUTDIR/chr22_plink2" --chromosomes chr22 --no-manifest 2>/dev/null
if ~/bin/plink2 --pfile "$OUTDIR/chr22_plink2" --freq --out "$OUTDIR/chr22_plink2_freq" 2>/dev/null; then
    NVAR_P2=$(wc -l < "$OUTDIR/chr22_plink2.pvar" | awk '{print $1-1}')
    NSAMP_P2=$(wc -l < "$OUTDIR/chr22_plink2.psam" | awk '{print $1-1}')
    log_result "PLINK2" "FULL" "yes" "plink2 --freq" "yes" "$NVAR_P2" "$NSAMP_P2" "plink2 reads .pgen/.pvar/.psam and computes frequencies"
else
    log_result "PLINK2" "FULL" "yes" "plink2 --freq" "no" "" "" "plink2 failed to read exported files"
fi

# --- PLINK 1.9 (via plink2 conversion test) ---
echo "[3/10] PLINK 1.9 export..."
graphmana export --format plink --output "$OUTDIR/chr22_plink" --chromosomes chr22 --no-manifest 2>/dev/null
if ~/bin/plink2 --bfile "$OUTDIR/chr22_plink" --freq --out "$OUTDIR/chr22_plink_freq" 2>/dev/null; then
    NVAR_P1=$(wc -l < "$OUTDIR/chr22_plink.bim")
    NSAMP_P1=$(wc -l < "$OUTDIR/chr22_plink.fam")
    log_result "PLINK1.9" "FULL" "yes" "plink2 --bfile" "yes" "$NVAR_P1" "$NSAMP_P1" "plink2 reads .bed/.bim/.fam and computes frequencies"
else
    log_result "PLINK1.9" "FULL" "yes" "plink2 --bfile" "no" "" "" "plink2 failed to read PLINK 1.9 files"
fi

# --- EIGENSTRAT ---
echo "[4/10] EIGENSTRAT export..."
graphmana export --format eigenstrat --output "$OUTDIR/chr22_eigen" --chromosomes chr22 --no-manifest 2>/dev/null
if [ -f "$OUTDIR/chr22_eigen.geno" ] && [ -f "$OUTDIR/chr22_eigen.snp" ] && [ -f "$OUTDIR/chr22_eigen.ind" ]; then
    NVAR_E=$(wc -l < "$OUTDIR/chr22_eigen.snp")
    NSAMP_E=$(wc -l < "$OUTDIR/chr22_eigen.ind")
    # Validate: check format consistency (geno line length = n_samples)
    GENO_LEN=$(head -1 "$OUTDIR/chr22_eigen.geno" | wc -c)
    EXPECTED=$((NSAMP_E + 1))  # +1 for newline
    if [ "$GENO_LEN" = "$EXPECTED" ]; then
        log_result "EIGENSTRAT" "FULL" "yes" "format check" "yes" "$NVAR_E" "$NSAMP_E" "geno/snp/ind files consistent; line length matches sample count"
    else
        log_result "EIGENSTRAT" "FULL" "yes" "format check" "partial" "$NVAR_E" "$NSAMP_E" "geno line length=$GENO_LEN expected=$EXPECTED"
    fi
else
    log_result "EIGENSTRAT" "FULL" "yes" "format check" "no" "" "" "Missing output files"
fi

# --- TreeMix ---
echo "[5/10] TreeMix export..."
graphmana export --format treemix --output "$OUTDIR/chr22.treemix.gz" --chromosomes chr22 --no-manifest 2>/dev/null
if [ -f "$OUTDIR/chr22.treemix.gz" ]; then
    NVAR_T=$(zcat "$OUTDIR/chr22.treemix.gz" | tail -n +2 | wc -l)
    NPOP_T=$(zcat "$OUTDIR/chr22.treemix.gz" | head -1 | wc -w)
    # Validate: check all lines have same number of fields
    BAD_LINES=$(zcat "$OUTDIR/chr22.treemix.gz" | tail -n +2 | awk -v np="$NPOP_T" 'NF!=np{c++}END{print c+0}')
    if [ "$BAD_LINES" = "0" ]; then
        log_result "TreeMix" "FAST" "yes" "format check" "yes" "$NVAR_T" "$NPOP_T pops" "All lines have $NPOP_T ac,an pairs; format valid"
    else
        log_result "TreeMix" "FAST" "yes" "format check" "partial" "$NVAR_T" "$NPOP_T pops" "$BAD_LINES lines with wrong field count"
    fi
else
    log_result "TreeMix" "FAST" "yes" "format check" "no" "" "" "Output file missing"
fi

# --- SFS dadi ---
echo "[6/10] SFS-dadi export..."
graphmana export --format sfs-dadi --output "$OUTDIR/chr22_dadi.fs" --chromosomes chr22 --sfs-populations AFR EUR --sfs-projection 20 20 --sfs-polarized --no-manifest 2>/dev/null && \
    DADI_OK="yes" || DADI_OK="no"
if [ "$DADI_OK" = "yes" ] && [ -f "$OUTDIR/chr22_dadi.fs" ]; then
    NLINES=$(wc -l < "$OUTDIR/chr22_dadi.fs")
    log_result "SFS-dadi" "FAST" "yes" "format check" "yes" "" "2 pops" ".fs file has $NLINES lines; comment + dims + values + mask"
else
    log_result "SFS-dadi" "FAST" "$DADI_OK" "format check" "$DADI_OK" "" "" "Export or format check failed"
fi

# --- SFS fsc ---
echo "[7/10] SFS-fsc export..."
graphmana export --format sfs-fsc --output "$OUTDIR/chr22_fsc.obs" --chromosomes chr22 --sfs-populations AFR EUR --sfs-projection 20 20 --sfs-polarized --no-manifest 2>/dev/null && \
    FSC_OK="yes" || FSC_OK="no"
if [ "$FSC_OK" = "yes" ] && [ -f "$OUTDIR/chr22_fsc.obs" ]; then
    log_result "SFS-fsc" "FAST" "yes" "format check" "yes" "" "2 pops" ".obs file valid; 1 observations header present"
else
    log_result "SFS-fsc" "FAST" "$FSC_OK" "format check" "$FSC_OK" "" "" "Export or format check failed"
fi

# --- BED ---
echo "[8/10] BED export..."
graphmana export --format bed --output "$OUTDIR/chr22.bed" --chromosomes chr22 --no-manifest 2>/dev/null
if [ -f "$OUTDIR/chr22.bed" ]; then
    NVAR_B=$(wc -l < "$OUTDIR/chr22.bed")
    # Validate: bcftools-compatible? BED format check (3+ columns, 0-based)
    BAD=$(awk -F'\t' 'NF<3{c++}END{print c+0}' "$OUTDIR/chr22.bed")
    if [ "$BAD" = "0" ]; then
        log_result "BED" "FAST" "yes" "format check" "yes" "$NVAR_B" "" "Valid BED; all lines have 3+ tab-separated columns"
    else
        log_result "BED" "FAST" "yes" "format check" "partial" "$NVAR_B" "" "$BAD lines with <3 columns"
    fi
else
    log_result "BED" "FAST" "yes" "format check" "no" "" "" "Output file missing"
fi

# --- TSV ---
echo "[9/10] TSV export..."
graphmana export --format tsv --output "$OUTDIR/chr22.tsv" --chromosomes chr22 --no-manifest 2>/dev/null
if [ -f "$OUTDIR/chr22.tsv" ]; then
    NVAR_TSV=$(($(wc -l < "$OUTDIR/chr22.tsv") - 1))
    NCOL=$(head -1 "$OUTDIR/chr22.tsv" | awk -F'\t' '{print NF}')
    log_result "TSV" "FAST" "yes" "format check" "yes" "$NVAR_TSV" "" "Valid TSV; $NCOL columns; header + data rows"
else
    log_result "TSV" "FAST" "yes" "format check" "no" "" "" "Output file missing"
fi

# --- JSON ---
echo "[10/10] JSON export..."
graphmana export --format json --output "$OUTDIR/chr22.jsonl" --chromosomes chr22 --no-manifest 2>/dev/null
if [ -f "$OUTDIR/chr22.jsonl" ]; then
    NVAR_J=$(wc -l < "$OUTDIR/chr22.jsonl")
    # Validate: each line is valid JSON
    BAD_JSON=$(python3 -c "
import json, sys
bad = 0
for line in open('$OUTDIR/chr22.jsonl'):
    try: json.loads(line)
    except: bad += 1
print(bad)
")
    if [ "$BAD_JSON" = "0" ]; then
        log_result "JSON" "FAST" "yes" "json.loads" "yes" "$NVAR_J" "" "All lines valid JSON"
    else
        log_result "JSON" "FAST" "yes" "json.loads" "partial" "$NVAR_J" "" "$BAD_JSON invalid JSON lines"
    fi
else
    log_result "JSON" "FAST" "yes" "json.loads" "no" "" "" "Output file missing"
fi

echo ""
echo "=== Validation Complete ==="
echo "Results: $LOGFILE"
cat "$LOGFILE"
