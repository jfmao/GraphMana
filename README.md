# GraphMana

**Graph-native data management platform for variant genomics.**

GraphMana stores VCF/GVCF data as a persistent, queryable Neo4j graph with packed genotype arrays on Variant nodes, pre-computed population statistics, incremental sample addition, integrated functional annotations, cohort management, reference genome liftover, annotation versioning, and multi-format export. Target scale: 100--50,000 samples on a single machine or HPC cluster node.

## Key Features

- **Packed genotype arrays** -- 2-bit-per-sample storage (125x smaller than per-sample edges)
- **Pre-computed population statistics** -- allele counts, frequencies, heterozygosity per population
- **Two access paths** -- FAST PATH (pre-computed arrays, seconds) and FULL PATH (unpack genotypes, linear in N)
- **Incremental sample addition** -- add new samples without re-processing existing data
- **12+ export formats** -- VCF, PLINK 1.9/2.0, EIGENSTRAT, TreeMix, SFS (dadi/fastsimcoal2), BED, TSV, Beagle, STRUCTURE, Genepop, haplotype matrix
- **Cohort management** -- define sample subsets as Cypher queries, not file extractions
- **Annotation versioning** -- VEP, ClinVar, CADD, gene constraint, GO terms, pathways, regulatory BED
- **Reference genome liftover** -- coordinate transformation across assemblies
- **HPC cluster support** -- two-step CSV pipeline, user-space Neo4j, SLURM/PBS scripts
- **Species-agnostic** -- diploid, haploid, and mixed-ploidy chromosomes

## Installation

### Docker (recommended for trying GraphMana)

```bash
git clone https://github.com/your-org/GraphMana.git
cd GraphMana
docker compose up --build
```

This builds an all-in-one image with Neo4j, the Java plugin, the Python CLI, and a demo dataset (100 SNPs on chr22, 20 samples, 4 populations). The demo data is automatically loaded on first start.

- Neo4j Browser: http://localhost:7474 (neo4j/graphmana)
- Bolt endpoint: bolt://localhost:7687

### From Source

```bash
# 1. Start Neo4j (or use graphmana setup-neo4j for user-space install)
docker compose up -d

# 2. Build Java procedures
cd graphmana-procedures && mvn clean package -DskipTests
# Copy JAR to Neo4j plugins directory and restart Neo4j

# 3. Install Python CLI
cd graphmana-cli && pip install -e ".[dev]"

# 4. Verify
graphmana status
```

### HPC Cluster

```bash
# Install Neo4j in user space (no root needed)
graphmana setup-neo4j --install-dir $HOME/neo4j --memory-auto

# Install Python CLI
pip install --user ./graphmana-cli

# See docs/cluster.md for detailed SLURM/PBS workflows
```

## Quick Start

```bash
# 1. Import VCF data
graphmana ingest \
    --input my_variants.vcf.gz \
    --population-map populations.tsv \
    --neo4j-home /path/to/neo4j \
    --reference GRCh38

# 2. Check database status
graphmana status

# 3. Export to TreeMix (FAST PATH -- seconds at any sample count)
graphmana export --format treemix --output treemix.gz

# 4. Export filtered VCF
graphmana export --format vcf --output filtered.vcf.gz \
    --populations POP_A POP_B --filter-maf-min 0.05

# 5. Export PLINK for GWAS
graphmana export --format plink --output gwas_data \
    --filter-variant-type SNP --filter-min-call-rate 0.95
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `graphmana ingest` | Import VCF data (CSV generation + Neo4j load) |
| `graphmana prepare-csv` | Generate CSV files from VCF (no Neo4j needed) |
| `graphmana load-csv` | Load pre-generated CSVs into Neo4j |
| `graphmana export` | Export to VCF, PLINK, EIGENSTRAT, TreeMix, SFS, etc. |
| `graphmana status` | Show database status and node counts |
| `graphmana cohort` | Define/list/show/delete named cohorts |
| `graphmana sample` | Remove/restore/reassign/hard-remove samples |
| `graphmana annotate` | Load/list/remove annotation versions |
| `graphmana qc` | Run quality control checks |
| `graphmana liftover` | Convert coordinates between reference assemblies |
| `graphmana snapshot` | Create/restore/delete database snapshots |
| `graphmana provenance` | Query import history and audit trail |
| `graphmana migrate` | Apply pending schema migrations |
| `graphmana setup-neo4j` | Download and configure Neo4j for user-space operation |
| `graphmana neo4j-start` | Start Neo4j in user space |
| `graphmana neo4j-stop` | Stop a running Neo4j instance |
| `graphmana check-filesystem` | Check if Neo4j data dir is on suitable storage |

## Export Formats

| Format | Access Path | Speed at 50K WGS | Target Tool |
|--------|------------|-------------------|-------------|
| TreeMix | FAST | Seconds | TreeMix |
| SFS (dadi) | FAST | Seconds | dadi, moments |
| SFS (fsc) | FAST | Seconds | fastsimcoal2 |
| BED | FAST | Seconds | bedtools, IGV |
| VCF | FULL | Hours (parallel) | bcftools, GATK |
| PLINK 1.9 | FULL | Hours (parallel) | PLINK, GCTA |
| PLINK 2.0 | FULL | Hours (parallel) | PLINK2 |
| EIGENSTRAT | FULL | Hours (parallel) | smartPCA, AdmixTools |
| Beagle | FULL | Hours (parallel) | Beagle |
| STRUCTURE | FULL | Hours (parallel) | STRUCTURE |
| Genepop | FULL | Hours (parallel) | Genepop |
| Haplotype | FULL | Hours (parallel) | selscan |
| TSV | Either | Varies | custom analysis |

## Architecture

GraphMana uses a Neo4j property graph with packed byte arrays for genotype storage. Variant nodes carry `gt_packed` (2 bits/sample), `phase_packed` (1 bit/sample), and pre-computed population arrays (`ac[]`, `an[]`, `af[]`).

```
Variant --[NEXT]--> Variant        (chromosomal order)
Variant --[ON_CHROMOSOME]--> Chromosome
Variant --[HAS_CONSEQUENCE]--> Gene
Sample  --[IN_POPULATION]--> Population
Gene    --[IN_PATHWAY]--> Pathway
Gene    --[HAS_GO_TERM]--> GOTerm
```

See `docs/schema.md` for the full graph schema.

## Documentation

- [Tutorial](docs/tutorial.md) -- Step-by-step guide from Docker start to multi-format export
- [API Reference](docs/api-reference.md) -- Complete CLI, Python API, and export format reference
- [Cluster Deployment](docs/cluster.md) -- SLURM/PBS workflows, user-space Neo4j, filesystem guidance
- [Schema Reference](docs/schema.md) -- Full graph schema specification
- [GraphPop Compatibility](docs/graphpop-compat.md) -- Schema and encoding verification for GraphPop interop
- [AI Integration](docs/ai-integration.md) -- MCP server setup for Claude Desktop/Code

## Software Stack

- **Database**: Neo4j Community 5.x
- **Java plugin**: JDK 21+, Maven
- **Python CLI**: Python 3.11+, cyvcf2, numpy, Click
- **Testing**: pytest, JUnit 5

## License

MIT
