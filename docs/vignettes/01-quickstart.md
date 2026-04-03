# Getting Started with GraphMana

This vignette walks through a complete GraphMana workflow in under 10 minutes:
install, import a small VCF, check the database, and export to two formats.

## Prerequisites

- Linux or macOS (x86_64 or arm64)
- Python 3.11+
- Java 21+ (for Neo4j)
- 4 GB RAM, 1 GB free disk

## Step 1: Install GraphMana

```bash
# Create and activate a conda environment
conda create -n graphmana python=3.11 -y
conda activate graphmana

# Install GraphMana CLI
cd /path/to/GraphMana/graphmana-cli
pip install -e ".[dev]"

# Verify installation
graphmana --version
```

**Expected output:**

```
graphmana, version 1.0.0-dev
```

## Step 2: Initialize a project

The `init` command creates a standard directory layout and optionally installs
Neo4j into the project directory.

```bash
graphmana init my_project --install-neo4j
```

**Expected output:**

```
Project initialized: /home/user/my_project
  data/       - input VCF and population files
  exports/    - export output files
  csv_out/    - intermediate CSV files
  logs/       - log files
  snapshots/  - database snapshots
  graphmana.env - project environment variables

Installing Neo4j...
  Neo4j 5.26.2 installed at /home/user/my_project/neo4j
  Password set to: graphmana

Quick start:
  source /home/user/my_project/graphmana.env
  graphmana prepare-csv --input data/your.vcf.gz --population-map data/pops.tsv \
      --output-dir csv_out --reference GRCh38 --threads 8
  graphmana load-csv --csv-dir csv_out --neo4j-home $GRAPHMANA_NEO4J_HOME \
      --auto-start-neo4j --neo4j-password $GRAPHMANA_NEO4J_PASSWORD
  graphmana status --neo4j-password $GRAPHMANA_NEO4J_PASSWORD
```

Source the environment file so subsequent commands pick up Neo4j paths and
password automatically:

```bash
source my_project/graphmana.env
```

## Step 3: Prepare the demo data

GraphMana ships with a small demo VCF (20 samples, 4 populations, chr22
subset) and a matching population map.

```bash
# Copy demo files into the project data directory
cp examples/demo_data/demo_chr22.vcf my_project/data/
cp examples/demo_data/population_map.tsv my_project/data/
```

Inspect the population map -- it is a tab-separated file with three columns:

```bash
head my_project/data/population_map.tsv
```

**Expected output:**

```
sample	population	superpopulation
S01	POP_A	GRP1
S02	POP_A	GRP1
S03	POP_A	GRP1
...
S20	POP_D	GRP2
```

## Step 4: Generate CSV files (no Neo4j needed)

The `prepare-csv` step parses the VCF, packs genotypes into byte arrays,
computes per-population allele counts, and writes CSV files that `neo4j-admin
import` can load. This step does not require a running Neo4j instance.

```bash
graphmana prepare-csv \
    --input my_project/data/demo_chr22.vcf \
    --population-map my_project/data/population_map.tsv \
    --output-dir my_project/csv_out \
    --reference GRCh38
```

**Expected output:**

```
CSV generation complete: 1000 variants, 20 samples, 2 populations
Output: my_project/csv_out
```

The output directory now contains CSV files for each node and relationship
type:

```bash
ls my_project/csv_out/
```

**Expected output:**

```
chromosomes.csv    populations.csv    samples.csv    variants_000.csv
metadata.json      relationships_NEXT_000.csv       relationships_ON_CHROMOSOME_000.csv
relationships_IN_POPULATION.csv
```

## Step 5: Load CSVs into Neo4j

The `load-csv` step runs `neo4j-admin database import` to bulk-load the CSV
files into a new Neo4j database. Use `--auto-start-neo4j` to let GraphMana
start and stop the server as needed.

```bash
graphmana load-csv \
    --csv-dir my_project/csv_out \
    --neo4j-home $GRAPHMANA_NEO4J_HOME \
    --auto-start-neo4j
```

**Expected output:**

```
neo4j-admin import completed successfully.
Schema metadata and indexes created.
```

## Step 6: Start Neo4j and check status

```bash
graphmana neo4j-start --neo4j-home $GRAPHMANA_NEO4J_HOME --wait

graphmana status
```

**Expected output:**

```
GraphMana v1.0.0-dev
Connected to: bolt://localhost:7687

Node counts:
  Variant              1,000
  Sample                  20
  Population               2
  Chromosome               1
  Gene                     0
  VCFHeader                1

Schema version:   0.1.0
Reference genome: GRCh38
```

## Step 7: Export to TreeMix (FAST PATH)

TreeMix export reads pre-computed population allele count arrays. It does not
unpack per-sample genotypes, so it completes in seconds regardless of sample
count.

```bash
graphmana export \
    --format treemix \
    --output my_project/exports/demo.treemix.gz
```

**Expected output:**

```
Export complete (treemix): 1000 variants
```

The output file is a gzipped allele count matrix ready for TreeMix:

```bash
zcat my_project/exports/demo.treemix.gz | head -3
```

**Expected output:**

```
GRP1 GRP2
28,12 18,12
35,5 26,4
```

## Step 8: Export to VCF (FULL PATH)

VCF export unpacks the packed genotype arrays to reconstruct per-sample
genotype calls. This is the FULL PATH -- runtime is linear in the number of
samples.

```bash
graphmana export \
    --format vcf \
    --output my_project/exports/demo_export.vcf
```

**Expected output:**

```
Export complete (vcf): 1000 variants
Samples: 20
```

Verify the VCF with standard tools:

```bash
grep -c "^#" my_project/exports/demo_export.vcf    # header lines
grep -v "^#" my_project/exports/demo_export.vcf | wc -l  # data lines
```

## Step 9: Stop Neo4j when done

```bash
graphmana neo4j-stop --neo4j-home $GRAPHMANA_NEO4J_HOME
```

## What you learned

- `graphmana init` creates a project directory with Neo4j
- `graphmana prepare-csv` parses VCF into CSV files (no Neo4j needed)
- `graphmana load-csv` bulk-loads CSVs via `neo4j-admin import`
- `graphmana status` shows database contents at a glance
- `graphmana export --format treemix` uses the FAST PATH (seconds)
- `graphmana export --format vcf` uses the FULL PATH (linear in N)

## See also

- [02-import-1kgp.md](02-import-1kgp.md) -- Importing a real dataset (1000 Genomes, 3,202 samples)
- [03-export-formats.md](03-export-formats.md) -- All 17 export formats with examples
- [04-cohort-management.md](04-cohort-management.md) -- Defining and using named cohorts
- [05-annotation.md](05-annotation.md) -- Loading functional annotations
