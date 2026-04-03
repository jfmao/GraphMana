# Functional Annotation

GraphMana supports seven annotation loaders that add functional information to
variant and gene nodes in the graph. Annotations are versioned, so you can
load multiple versions side by side, compare them, and cleanly swap one for
another.

This vignette covers loading each annotation type, listing and removing
annotation versions, and filtering exports by annotation properties.

## Prerequisites

A GraphMana database with imported variants and Neo4j running. See
[02-import-1kgp.md](02-import-1kgp.md) for a full import example.

## Annotation overview

| Loader | Command | Input format | Creates |
|--------|---------|-------------|---------|
| VEP/SnpEff | `annotate load` | Annotated VCF | `:HAS_CONSEQUENCE` edges to `:Gene` nodes |
| CADD | `annotate load-cadd` | CADD TSV | `cadd_phred`, `cadd_raw` on `:Variant` |
| ClinVar | `annotate load-clinvar` | ClinVar VCF | `clinvar_sig`, `clinvar_id` on `:Variant` |
| Gene constraint | `annotate load-constraint` | gnomAD TSV | `pli`, `loeuf`, `mis_z` on `:Gene` |
| GO terms | `annotate load-go` | GAF + OBO | `:HAS_GO_TERM` edges, `:GOTerm` nodes |
| Pathways | `annotate load-pathway` | TSV | `:IN_PATHWAY` edges, `:Pathway` nodes |
| BED regions | `annotate load-bed` | BED | `:RegulatoryElement` nodes, overlap edges |

All loaders record an `:AnnotationVersion` node tracking the source, version
label, load date, and edge count.

---

## 1. VEP / SnpEff consequence annotations

The primary annotation loader parses VEP or SnpEff annotations from a VCF
file and creates `:HAS_CONSEQUENCE` relationships between Variant and Gene
nodes. The annotation type is auto-detected from the VCF header.

### Annotate your VCF with VEP first

```bash
# Run VEP on your VCF (example -- not a GraphMana command)
vep --input_file chr22.vcf.gz \
    --output_file chr22.vep.vcf.gz \
    --vcf --cache --assembly GRCh38 \
    --sift b --polyphen b --symbol --canonical
```

### Load VEP annotations into GraphMana

```bash
graphmana annotate load \
    --input chr22.vep.vcf.gz \
    --version VEP_v112 \
    --mode add \
    --description "Ensembl VEP v112, GRCh38 cache" \
    --verbose
```

**Expected output:**

```
Annotation loaded: version=VEP_v112, mode=add, 4521 genes, 892340 edges (VEP)
```

### Load modes

- `--mode add` (default): Layer new annotations alongside existing ones. Both
  versions coexist. Useful for comparing annotation releases.
- `--mode update`: Merge with existing annotations of the same version.
  Updates properties on existing edges and adds new ones.
- `--mode replace`: Remove all existing HAS_CONSEQUENCE edges for matched
  variants, then load fresh. Clean swap for a new annotation release.

### Loading annotations at import time

You can also load VEP annotations during the initial import by passing
`--vep-vcf` to `prepare-csv` or `ingest`:

```bash
graphmana prepare-csv \
    --input chr22.vcf.gz \
    --population-map panel.tsv \
    --output-dir csv_out \
    --vep-vcf chr22.vep.vcf.gz \
    --annotation-version VEP_v112 \
    --reference GRCh38
```

---

## 2. CADD scores

CADD (Combined Annotation Dependent Depletion) scores quantify variant
deleteriousness. GraphMana loads CADD phred-scaled and raw scores directly
onto Variant nodes.

### Download CADD scores

```bash
# Download pre-scored CADD file for GRCh38 (example)
wget https://krishna.gs.washington.edu/download/CADD/v1.7/GRCh38/whole_genome_SNVs.tsv.gz
```

### Load CADD scores

```bash
graphmana annotate load-cadd \
    --input whole_genome_SNVs.tsv.gz \
    --version CADD_v1.7 \
    --description "CADD v1.7, GRCh38, whole genome SNVs" \
    --verbose
```

**Expected output:**

```
CADD loaded: version=CADD_v1.7, 8923100 parsed, 6541230 matched
```

The "matched" count reflects how many CADD records correspond to variants
already in your database. Unmatched records are silently skipped.

If your VCF uses `chr` prefixes but the CADD file does not (or vice versa),
use `--chr-prefix`:

```bash
graphmana annotate load-cadd \
    --input whole_genome_SNVs.tsv.gz \
    --version CADD_v1.7 \
    --chr-prefix chr
```

---

## 3. ClinVar clinical significance

ClinVar annotations add clinical significance classifications and ClinVar
accession IDs to Variant nodes.

```bash
graphmana annotate load-clinvar \
    --input clinvar_20260101.vcf.gz \
    --version ClinVar_2026-01 \
    --description "ClinVar January 2026 release" \
    --verbose
```

**Expected output:**

```
ClinVar loaded: version=ClinVar_2026-01, 1245000 parsed, 42310 matched
```

ClinVar properties added to matched Variant nodes:

- `clinvar_sig` -- clinical significance (e.g., "Pathogenic",
  "Likely_benign")
- `clinvar_id` -- ClinVar variation ID
- `clinvar_review` -- review status stars
- `clinvar_conditions` -- associated conditions

---

## 4. Gene constraint scores

Gene constraint metrics from gnomAD indicate tolerance to loss-of-function
and missense variation. These are loaded onto existing Gene nodes.

```bash
graphmana annotate load-constraint \
    --input gnomad.v4.1.constraint_metrics.tsv \
    --version gnomAD_v4.1 \
    --description "gnomAD v4.1 gene constraint metrics"
```

**Expected output:**

```
Constraint loaded: version=gnomAD_v4.1, 19704 parsed, 4412 matched
```

Properties added to Gene nodes:

- `pli` -- probability of loss-of-function intolerance
- `loeuf` -- loss-of-function observed/expected upper bound fraction
- `mis_z` -- missense Z-score
- `syn_z` -- synonymous Z-score

---

## 5. GO term annotations

Gene Ontology annotations link Gene nodes to GOTerm nodes. Optionally, load
the OBO ontology file to create the IS_A hierarchy between GO terms.

```bash
graphmana annotate load-go \
    --input goa_human.gaf.gz \
    --version GO_2026-03 \
    --obo-file go-basic.obo \
    --description "GO annotations March 2026, human"
```

**Expected output:**

```
GO loaded: version=GO_2026-03, 612430 annotations, 4218 edges, 45210 IS_A edges
```

This creates:

- `:GOTerm` nodes with `id`, `name`, `namespace` properties
- `:HAS_GO_TERM` edges from Gene to GOTerm
- `:IS_A` edges between GOTerm nodes (if OBO file provided)

---

## 6. Pathway annotations

Load gene-to-pathway mappings from a tab-separated file. The expected columns
are `gene_symbol`, `pathway_id`, `pathway_name`, and optionally `source`.

```bash
graphmana annotate load-pathway \
    --input kegg_pathways.tsv \
    --version KEGG_2026 \
    --source KEGG \
    --description "KEGG pathway mappings, March 2026"
```

**Expected output:**

```
Pathways loaded: version=KEGG_2026, 8923 parsed, 3421 edges
```

This creates `:Pathway` nodes and `:IN_PATHWAY` edges from Gene to Pathway.

---

## 7. BED region annotations

Overlay genomic regions (enhancers, promoters, regulatory elements) from BED
files. Variants falling within the regions are linked to RegulatoryElement
nodes.

```bash
graphmana annotate load-bed \
    --input encode_enhancers.bed \
    --version ENCODE_v3 \
    --region-type enhancer \
    --description "ENCODE enhancer regions, GRCh38"
```

**Expected output:**

```
BED regions loaded: version=ENCODE_v3, 312450 regions, 89230 edges
```

The `--region-type` label is stored on the RegulatoryElement node and can be
used to distinguish different region sets (enhancer, promoter, CTCF, etc.).

---

## Listing annotation versions

```bash
graphmana annotate list
```

**Expected output:**

```
  VEP_v112       (VEP, 892340 edges, 2026-03-27T14:30:00) — Ensembl VEP v112, GRCh38 cache
  CADD_v1.7      (CADD, 6541230 edges, 2026-03-27T14:45:00) — CADD v1.7, GRCh38, whole genome SNVs
  ClinVar_2026-01 (ClinVar, 42310 edges, 2026-03-27T15:00:00) — ClinVar January 2026 release
  gnomAD_v4.1    (constraint, 4412 edges, 2026-03-27T15:10:00) — gnomAD v4.1 gene constraint metrics
  GO_2026-03     (GO, 4218 edges, 2026-03-27T15:20:00) — GO annotations March 2026, human
  KEGG_2026      (pathway, 3421 edges, 2026-03-27T15:30:00) — KEGG pathway mappings, March 2026
  ENCODE_v3      (BED, 89230 edges, 2026-03-27T15:40:00) — ENCODE enhancer regions, GRCh38
```

## Removing an annotation version

When a new annotation release is available, you can remove the old version
cleanly. This deletes all edges and orphaned nodes associated with that
version.

```bash
graphmana annotate remove --version VEP_v112
```

**Expected output:**

```
Annotation removed: version=VEP_v112, 892340 edges deleted, 312 orphan genes removed
```

Orphan genes are Gene nodes that no longer have any HAS_CONSEQUENCE edges
after the removal.

---

## Filtering exports by annotation

Once annotations are loaded, export filters can select variants based on
functional properties.

### Filter by consequence type

```bash
graphmana export \
    --format vcf \
    --output exports/missense_chr22.vcf \
    --chromosomes chr22 \
    --filter-consequence missense_variant
```

Multiple consequence types can be specified:

```bash
graphmana export \
    --format vcf \
    --output exports/lof_chr22.vcf \
    --chromosomes chr22 \
    --filter-consequence stop_gained \
    --filter-consequence frameshift_variant \
    --filter-consequence splice_donor_variant \
    --filter-consequence splice_acceptor_variant
```

### Filter by impact level

VEP impact levels: HIGH, MODERATE, LOW, MODIFIER.

```bash
graphmana export \
    --format plink \
    --output exports/high_impact \
    --filter-impact HIGH
```

### Filter by gene

Export variants annotated to specific genes:

```bash
graphmana export \
    --format vcf \
    --output exports/brca_variants.vcf \
    --filter-gene BRCA1 \
    --filter-gene BRCA2
```

### Filter by CADD score

```bash
graphmana export \
    --format tsv \
    --output exports/deleterious.tsv \
    --filter-cadd-min 20 \
    --tsv-columns chr pos ref alt cadd_phred consequence gene_symbol
```

### Filter by annotation version

When multiple annotation versions coexist, restrict to a specific version:

```bash
graphmana export \
    --format vcf \
    --output exports/vep112_high.vcf \
    --filter-impact HIGH \
    --filter-annotation-version VEP_v112
```

### Combining annotation and population filters

All filters compose. This example exports high-impact missense variants in
European samples with CADD > 25:

```bash
graphmana export \
    --format vcf \
    --output exports/eur_pathogenic.vcf \
    --filter-cohort european_samples \
    --filter-consequence missense_variant \
    --filter-impact HIGH MODERATE \
    --filter-cadd-min 25
```

---

## Annotation update workflow

A typical annotation lifecycle:

```bash
# 1. Load initial annotations
graphmana annotate load --input chr22.vep_v111.vcf.gz --version VEP_v111

# 2. New VEP release available -- load alongside
graphmana annotate load --input chr22.vep_v112.vcf.gz --version VEP_v112 --mode add

# 3. Compare: export with each version
graphmana export --format tsv --output v111.tsv \
    --filter-impact HIGH --filter-annotation-version VEP_v111
graphmana export --format tsv --output v112.tsv \
    --filter-impact HIGH --filter-annotation-version VEP_v112

# 4. Satisfied with v112 -- remove v111
graphmana annotate remove --version VEP_v111

# 5. Verify
graphmana annotate list
```

## See also

- [01-quickstart.md](01-quickstart.md) -- Quick start with demo data
- [03-export-formats.md](03-export-formats.md) -- Export format reference and filtering
- [04-cohort-management.md](04-cohort-management.md) -- Combining cohort and annotation filters
