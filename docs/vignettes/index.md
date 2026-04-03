# GraphMana Vignettes

Step-by-step tutorials demonstrating GraphMana workflows. Each vignette is a self-contained guide with copy-pasteable commands and expected output.

For per-command reference documentation, see the [Command Reference](../commands/).

## Tutorials

### Getting Started

| # | Vignette | Description | Time |
|---|----------|-------------|------|
| 1 | [Quick Start](01-quickstart.md) | Install, import demo data, first export — in 10 minutes | 10 min |
| 2 | [Importing 1000 Genomes](02-import-1kgp.md) | Full 1KGP dataset: 3,202 samples, 70.7M variants | 2-3 hr |

### Export and Analysis

| # | Vignette | Description | Time |
|---|----------|-------------|------|
| 3 | [Export Formats](03-export-formats.md) | All 17 formats with examples, timing, and tool compatibility | Reference |
| 4 | [Cohort Management](04-cohort-management.md) | Define named cohorts, filter exports by cohort | 15 min |
| 5 | [Functional Annotation](05-annotation.md) | VEP, CADD, ClinVar, GO, pathways — load and query | 30 min |

### Data Lifecycle

| # | Vignette | Description | Time |
|---|----------|-------------|------|
| 6 | [Sample Management](06-sample-lifecycle.md) | Add, remove, restore, reassign samples | 15 min |
| 7 | [Reference Liftover](07-liftover.md) | GRCh37 to GRCh38 coordinate conversion | 15 min |

### Deployment

| # | Vignette | Description | Time |
|---|----------|-------------|------|
| 8 | [HPC Cluster Deployment](08-cluster-hpc.md) | SLURM/PBS setup, job generation, multi-user | 30 min |
| 9 | [Python/Jupyter API](09-jupyter-api.md) | Interactive analysis with DataFrames | 15 min |

### Administration

| # | Vignette | Description | Time |
|---|----------|-------------|------|
| 10 | [Database Administration](10-database-admin.md) | Snapshots, QC, provenance, migration, merge | 20 min |

### Variant Representation

| # | Vignette | Description | Time |
|---|----------|-------------|------|
| 11 | [Multi-allelic Sites and Structural Variants](11-variant-representation.md) | Multi-allelic splitting, SV support, large indels | 15 min |

## Prerequisites

All vignettes assume:
- GraphMana CLI installed (`pip install graphmana-cli` or editable install)
- Neo4j 5.x available (via `graphmana setup-neo4j` or system install)
- Java 21+ on PATH
- conda or virtualenv with cyvcf2 and numpy

See [Vignette 1: Quick Start](01-quickstart.md) for installation instructions.
