# GraphMana Tutorial

This tutorial walks through a complete workflow: starting the database, importing VCF data, querying the graph, and exporting to multiple formats.

## Prerequisites

- Docker and Docker Compose (for the quickstart path), OR
- Neo4j Community 5.x, Java 21+, Python 3.11+ (for source installation)

## 1. Start with Docker

```bash
git clone https://github.com/your-org/GraphMana.git
cd GraphMana
docker compose up --build -d
```

This builds and starts a container with Neo4j, the GraphMana Java plugin, and the Python CLI. A demo dataset (100 SNPs on chr22, 20 samples in 4 populations) is automatically loaded on first start.

Wait ~30 seconds for Neo4j to initialize, then verify:

```bash
docker compose exec graphmana graphmana status
```

Expected output:
```
GraphMana v0.9.0
Connected to: bolt://localhost:7687

Node counts:
  Variant              100
  Sample                20
  Population             4
  Chromosome             1
  Gene                   0
  VCFHeader              1
```

## 2. Check Database Status

From inside the container (or with the CLI installed locally):

```bash
graphmana status --detailed
```

The `--detailed` flag adds total node and relationship counts. Use `--json` for machine-readable output.

## 3. Import Your Own Data

To import your own VCF, you need:
1. A VCF/BCF file (bgzipped or plain)
2. A population map TSV with columns: `sample`, `population`, and optionally `superpopulation`

```bash
graphmana ingest \
    --input my_variants.vcf.gz \
    --population-map populations.tsv \
    --neo4j-home /var/lib/neo4j \
    --reference GRCh38 \
    --verbose
```

The `ingest` command runs two phases internally:
1. **CSV generation**: Parses the VCF, packs genotypes into 2-bit arrays, computes population statistics, and writes Neo4j-compatible CSV files.
2. **Database load**: Uses `neo4j-admin database import` to bulk-load the CSVs into Neo4j, then creates indexes and schema metadata.

### Import with Filters

```bash
graphmana ingest \
    --input my_variants.vcf.gz \
    --population-map populations.tsv \
    --neo4j-home /var/lib/neo4j \
    --reference GRCh38 \
    --filter-min-qual 30 \
    --filter-min-call-rate 0.90 \
    --filter-variant-type SNP \
    --verbose
```

## 4. Explore with Cypher

Open Neo4j Browser at http://localhost:7474 (credentials: neo4j/graphmana) and try:

```cypher
// Count variants per chromosome
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome)
RETURN c.chromosomeId, count(v) ORDER BY c.chromosomeId

// Find high-frequency variants
MATCH (v:Variant) WHERE v.af_total > 0.5
RETURN v.variantId, v.af_total LIMIT 10

// List samples by population
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
RETURN p.populationId, collect(s.sampleId) AS samples

// Walk the NEXT chain (first 5 variants on chr22)
MATCH path = (v:Variant)-[:NEXT*4]->(v2:Variant)
WHERE v.chr = 'chr22'
RETURN [n IN nodes(path) | n.variantId] AS chain
LIMIT 1
```

## 5. Export VCF

Round-trip export back to VCF format:

```bash
graphmana export --format vcf --output exported.vcf
```

With population and MAF filters:

```bash
graphmana export --format vcf --output filtered.vcf \
    --populations POP_A POP_B \
    --filter-maf-min 0.01 --filter-maf-max 0.99
```

## 6. Export TreeMix

TreeMix export uses the FAST PATH (pre-computed population arrays) and completes in seconds regardless of sample count:

```bash
graphmana export --format treemix --output treemix_input.gz
```

The output is a gzipped allele count matrix ready for TreeMix analysis.

## 7. Export PLINK

```bash
graphmana export --format plink --output plink_data \
    --filter-variant-type SNP
```

This creates `plink_data.bed`, `plink_data.bim`, and `plink_data.fam` files compatible with PLINK 1.9.

## 8. Export EIGENSTRAT

```bash
graphmana export --format eigenstrat --output eigen_data
```

Creates `.geno`, `.snp`, and `.ind` files for smartPCA and AdmixTools (f-statistics).

## 9. Filtering

All export commands support a rich set of filters:

```bash
# Population + variant type + MAF filter
graphmana export --format vcf --output subset.vcf \
    --populations POP_A POP_C \
    --filter-variant-type SNP \
    --filter-maf-min 0.05

# Consequence/impact filter (requires annotation)
graphmana export --format tsv --output missense.tsv \
    --filter-consequence missense_variant \
    --filter-impact HIGH MODERATE

# Sample list filter
graphmana export --format vcf --output selected.vcf \
    --filter-sample-list sample_ids.txt
```

## 10. Cohorts

Define reusable sample subsets as Cypher queries:

```bash
# Define a cohort
graphmana cohort define \
    --name "high_het" \
    --query "MATCH (s:Sample) WHERE s.heterozygosity > 0.3 RETURN s.sampleId"

# List cohorts
graphmana cohort list

# Count samples in a cohort
graphmana cohort count --name "high_het"

# Export using a cohort filter
graphmana export --format vcf --output high_het.vcf \
    --filter-cohort high_het
```

## 11. Annotations

Load functional annotations from VEP, ClinVar, CADD, or custom sources:

```bash
# Load VEP annotations
graphmana annotate load \
    --input vep_annotated.vcf \
    --version VEP_v110 \
    --mode add

# Load CADD scores
graphmana annotate load-cadd \
    --input whole_genome_SNVs.tsv.gz \
    --version CADD_v1.7

# List annotation versions
graphmana annotate list

# Export with annotation filters
graphmana export --format tsv --output high_impact.tsv \
    --filter-impact HIGH \
    --filter-annotation-version VEP_v110
```

## 12. Provenance

Track the history of all imports:

```bash
# List all ingestion events
graphmana provenance list

# Show details of a specific import
graphmana provenance show <log_id>

# Aggregate summary
graphmana provenance summary
```

## 13. Incremental Import

Add new samples to an existing database without re-processing:

```bash
graphmana ingest \
    --input new_samples.vcf.gz \
    --population-map new_populations.tsv \
    --mode incremental \
    --neo4j-uri bolt://localhost:7687 \
    --verbose
```

Incremental mode extends the packed genotype arrays for existing variants and creates new Variant nodes for variants not seen before.

## 14. SFS Export

Export site frequency spectra for demographic inference:

```bash
# dadi format
graphmana export --format sfs-dadi --output sfs_dadi.txt \
    --sfs-populations POP_A POP_B \
    --sfs-projection 20 20 \
    --sfs-polarized

# fastsimcoal2 format
graphmana export --format sfs-fsc --output sfs_fsc \
    --sfs-populations POP_A POP_B \
    --sfs-projection 20 20 \
    --sfs-folded
```

## Next Steps

- **HPC deployment**: See [docs/cluster.md](cluster.md) for SLURM/PBS workflows
- **Schema details**: See [docs/schema.md](schema.md) for the full graph schema
- **Liftover**: Use `graphmana liftover` to convert between reference assemblies
- **Snapshots**: Use `graphmana snapshot` for database backups
