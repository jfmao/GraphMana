# Supplementary Note 3: Concrete Workflow Example

This example demonstrates a typical multi-day project workflow using GraphMana
CLI commands. All operations run against a single persistent graph database.

## Day 1: Initial import

```bash
# Set up Neo4j (one-time)
graphmana setup-neo4j --install-dir ~/neo4j --memory-auto

# Import initial batch (2,500 samples from 22 VCF files)
graphmana ingest \
    --input data/1kgp_chr*.vcf.gz \
    --population-map data/1kgp_panel.tsv \
    --neo4j-home ~/neo4j --auto-start-neo4j \
    --threads 16

# Check what was imported
graphmana status --detailed
graphmana summary --output reports/day1_summary.txt
```

## Day 2: New samples arrive (incremental addition)

```bash
# Save state before the operation
graphmana save-state --output checkpoints/before_batch2.json

# Add 234 new samples without reprocessing existing data
graphmana ingest \
    --input data/batch2.vcf.gz \
    --population-map data/batch2_panel.tsv \
    --mode incremental \
    --neo4j-home ~/neo4j --auto-start-neo4j

# Verify what changed
graphmana diff --snapshot checkpoints/before_batch2.json
```

Output:
```
--- Count Changes ---
  Samples (active):          2,500 ->      2,734  (+234)
  Variants:              1,066,555 ->  1,066,557  (+2)
--- Population Changes ---
  ~ GBR: 91 -> 102 samples
  ~ CEU: 99 -> 111 samples
  ~ YRI: 108 -> 120 samples
```

## Day 3: Collaborator requests — multi-format export

```bash
# European EIGENSTRAT for AdmixTools (with provenance)
graphmana export \
    --format eigenstrat \
    --output exports/european_chr22 \
    --populations CEU --populations GBR --populations FIN \
    --populations IBS --populations TSI \
    --filter-maf-min 0.05

# TreeMix for population tree (FAST PATH — seconds)
graphmana export --format treemix --output exports/all_pops.treemix.gz

# SFS for demographic inference
graphmana export \
    --format sfs-dadi \
    --output exports/yri_ceu.fs \
    --sfs-populations YRI --sfs-populations CEU \
    --sfs-projection 20 --sfs-projection 20 \
    --sfs-folded

# Check manifest for any export
cat exports/european_chr22.manifest.json
```

## Day 4: Annotation update

```bash
# Load new ClinVar release (in-place, 3.5 seconds)
graphmana annotate load-clinvar \
    --input annotations/clinvar_20260401.vcf.gz \
    --version ClinVar_2026-04

# Verify genotype layer was not affected
graphmana db validate

# Check annotation versions
graphmana annotate list
```

## Day 5: QC and provenance audit

```bash
# Generate QC report
graphmana qc --type all --output reports/qc_report.html --format html

# Verify REF alleles against reference genome
graphmana ref-check --fasta genomes/GRCh38.fa --chromosomes chr22

# Search provenance: what was imported this week?
graphmana provenance search --since 2026-04-01 --until 2026-04-05

# Full provenance summary
graphmana provenance summary
```

## Key observations

1. **No file regeneration**: The TreeMix, SFS, and EIGENSTRAT exports all read
   from the same database that was incrementally updated. No VCF merging or
   format conversion scripts were needed.

2. **Automatic provenance**: Each export produced a `.manifest.json` sidecar
   recording the exact filters, sample set, and software version.

3. **Annotation independence**: The ClinVar update modified annotation edges
   without touching genotype data; `db validate` confirmed integrity.

4. **State tracking**: The `save-state` / `diff` workflow showed exactly what
   changed after the incremental addition.
