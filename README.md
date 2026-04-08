# GraphMana

**Graph-native data management platform for population genomics.**

GraphMana stores VCF/GVCF data as a persistent, queryable graph database with packed genotype arrays on Variant nodes, pre-computed population statistics, incremental sample addition, integrated functional annotations, cohort management, reference genome liftover, annotation versioning, and multi-format export. Target scale: 100–50,000 samples on a single machine or HPC cluster node.

## Key Features

- **Packed genotype arrays** — 2-bit-per-sample storage (125× smaller than per-sample edges)
- **Pre-computed population statistics** — allele counts, frequencies, heterozygosity per population
- **Two access paths** — FAST PATH (pre-computed arrays, seconds) and FULL PATH (unpack genotypes, linear in N)
- **Incremental sample addition** — add new samples without re-processing existing data
- **17 export formats** — VCF, PLINK 1.9/2.0, EIGENSTRAT, TreeMix, SFS (dadi/fastsimcoal2), BED, TSV, Beagle, STRUCTURE, Genepop, haplotype, BGEN, GDS, Zarr, JSON
- **Cohort management** — define sample subsets as graph queries, not file extractions
- **Annotation versioning** — VEP, ClinVar, CADD, gene constraint, GO terms, pathways, regulatory BED
- **Automatic provenance** — every operation logged with parameters, timestamps, sample counts
- **Export manifests** — each export generates a `.manifest.json` sidecar for reproducibility
- **Reference genome liftover** — coordinate transformation across assemblies
- **58 CLI commands** — organized into 9 functional domains, no programming required
- **HPC cluster support** — two-step CSV pipeline, user-space Neo4j, SLURM/PBS scripts
- **Species-agnostic** — diploid, haploid, and mixed-ploidy chromosomes
- **No admin privileges needed** — installs entirely in user space

## Installation

> **Full installation guide:** [docs/INSTALL.md](docs/INSTALL.md)

### Quick Install (no admin needed)

```bash
curl -sSL https://raw.githubusercontent.com/jfmao/GraphMana/main/install.sh | bash
```

This installs conda (if needed), Python, Java, Neo4j, and GraphMana in one step.

### pip install

```bash
conda create -n graphmana -c conda-forge -c bioconda python=3.12 cyvcf2 openjdk=21 -y
conda activate graphmana
pip install graphmana
graphmana setup-neo4j --install-dir ~/neo4j --memory-auto
```

The Java procedures JAR is **bundled with the Python package** — no Maven build needed.
The `setup-neo4j` command automatically deploys the JAR to the Neo4j plugins directory.

### Docker

```bash
git clone https://github.com/jfmao/GraphMana.git
cd GraphMana
docker compose up --build
```

- Neo4j Browser: http://localhost:7474 (neo4j/graphmana)
- Bolt endpoint: bolt://localhost:7687

### HPC Cluster

```bash
conda activate graphmana
graphmana setup-neo4j --install-dir $HOME/neo4j --install-java --memory-auto
```

The `--install-java` flag downloads Eclipse Temurin JDK 21 to user space (no admin needed).
See [Vignette 08: HPC Cluster Deployment](docs/vignettes/08-cluster-hpc.md) for SLURM/PBS workflows.

### From Source (development)

```bash
git clone https://github.com/jfmao/GraphMana.git
cd GraphMana
conda create -n graphmana -c conda-forge -c bioconda python=3.12 cyvcf2 openjdk=21 maven -y
conda activate graphmana

# Build Java procedures (optional — JAR is pre-built and bundled)
cd graphmana-procedures && mvn clean package -DskipTests && cd ..

# Install Python CLI
cd graphmana-cli && pip install -e ".[dev]" && cd ..

# Run tests (1,439 tests)
cd graphmana-cli && pytest -v && cd ..

# Setup Neo4j
graphmana setup-neo4j --install-dir ~/neo4j --memory-auto
```

## Quick Start

```bash
# Start Neo4j
graphmana neo4j-start --neo4j-home ~/neo4j --wait

# Import VCF data
graphmana ingest \
    --input my_variants.vcf.gz \
    --population-map populations.tsv \
    --neo4j-home ~/neo4j \
    --reference GRCh38

# Check database status
graphmana status --detailed

# Export to TreeMix (FAST PATH — seconds at any sample count)
graphmana export --format treemix --output treemix.gz

# Export filtered VCF
graphmana export --format vcf --output filtered.vcf.gz \
    --populations POP_A POP_B --filter-maf-min 0.05

# Export PLINK for GWAS
graphmana export --format plink --output gwas_data \
    --filter-variant-type SNP --filter-min-call-rate 0.95
```

## CLI Commands (58 total)

GraphMana provides 58 commands organized into 9 functional domains.
See [Command Reference](docs/commands/index.md) for the full documentation.

| Domain | Key Commands |
|--------|-------------|
| **Data Import** | `ingest`, `prepare-csv`, `load-csv`, `merge`, `liftover` |
| **Annotation** | `annotate load`, `load-clinvar`, `load-cadd`, `load-go`, `load-bed` |
| **Export** | `export` (17 formats), `list-formats` |
| **Sample & Cohort** | `sample remove/restore/reassign`, `cohort define/list/show` |
| **Quality Control** | `qc`, `ref-check`, `db validate` |
| **Provenance** | `provenance list/show/search/summary` |
| **Database Admin** | `snapshot create/restore`, `db info/check`, `diff`, `save-state` |
| **Status** | `status`, `summary`, `version`, `config-show` |
| **Infrastructure** | `setup-neo4j`, `neo4j-start/stop`, `check-filesystem`, `cluster` |

## Export Formats

| Format | Access Path | Target Tool |
|--------|------------|-------------|
| TreeMix | FAST | TreeMix |
| SFS (dadi) | FAST | dadi, moments |
| SFS (fsc) | FAST | fastsimcoal2 |
| BED | FAST | bedtools, IGV |
| TSV | FAST | General analysis |
| JSON | FAST | Programmatic |
| VCF/BCF | FULL | bcftools, GATK |
| PLINK 1.9 | FULL | PLINK |
| PLINK 2.0 | FULL | PLINK2 |
| EIGENSTRAT | FULL | smartPCA, AdmixTools |
| Beagle | FULL | Beagle |
| STRUCTURE | FULL | STRUCTURE |
| Genepop | FULL | Genepop |
| Haplotype | FULL | selscan |
| BGEN | FULL | UK Biobank tools |
| GDS | FULL | SeqArray/R |
| Zarr | FULL | sgkit/Python |

## Documentation

- **[Installation Guide](docs/INSTALL.md)** — 5 installation methods, no admin needed
- **[Command Reference](docs/commands/index.md)** — 65 command reference pages
- **[Vignettes](docs/vignettes/index.md)** — 11 tutorial vignettes
- **[Cluster Deployment](docs/vignettes/08-cluster-hpc.md)** — SLURM/PBS guide

## Architecture

GraphMana is built on graph database technology (currently Neo4j Community Edition, free and open source). The companion [GraphPop](https://github.com/jfmao/GraphPop) engine provides graph-native analytical computation (population statistics, selection scans, annotation-conditioned queries) on the same persistent database.

## Software Stack

- **Database**: Neo4j Community 5.x (graph database)
- **Java plugin**: Pre-built JAR bundled with Python package (31 KB)
- **Python CLI**: Python 3.11+, cyvcf2, numpy, Click (21,267 lines)
- **Testing**: 1,439 unit and integration tests (pytest)

## Data and Benchmarks

Benchmark data and a pre-built 1000 Genomes Project chr22 database (3,202 samples, 1.07M variants) are deposited at Zenodo:

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.19472836.svg)](https://doi.org/10.5281/zenodo.19472836)

## License

MIT License. See [LICENSE](LICENSE).

## Citation

If you use GraphMana in your research, please cite:

> Mao, J. GraphMana: graph-native data management for population genomics projects. *bioRxiv* (2026). [DOI pending]
