# Importing the 1000 Genomes Project

This vignette walks through a full import of the 1000 Genomes Project (1KGP)
phase 3 dataset: 3,202 samples across 26 populations, all 22 autosomes. This
is the demonstration dataset used for all GraphMana benchmarks.

## Prerequisites

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 32 GB | 64 GB |
| Disk (SSD) | 300 GB free | 500 GB free |
| CPU cores | 4 | 16+ |
| Neo4j | 5.x installed | 5.26+ |
| Java | 21+ | 21+ |
| Time | ~3 hours | ~2 hours |

Neo4j data directory **must** be on local SSD, not NFS or network storage.
Verify with:

```bash
graphmana check-filesystem --neo4j-data-dir /local/scratch/neo4j_data
```

## Step 1: Download 1KGP data

Download the per-chromosome VCF files and the sample panel from the 1KGP FTP
site:

```bash
mkdir -p 1kgp/vcf

# Download all 22 autosomes
for chr in $(seq 1 22); do
    wget -P 1kgp/vcf \
        "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000G_2504_high_coverage/working/20220422_3202_phased_SNV_INDEL_SV/1kGP_high_coverage_Illumina.chr${chr}.filtered.SNV_INDEL_SV_phased_panel.vcf.gz"
done

# Download the sample panel (PED-format population map)
wget -O 1kgp/1kGP.panel \
    "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000G_2504_high_coverage/working/1kGP.3202_samples.pedigree_info.txt"
```

## Step 2: Prepare the population map

The 1KGP panel file maps each sample to a population and superpopulation.
GraphMana expects a tab-separated file with columns `sample`, `population`,
and `superpopulation`:

```bash
head -5 1kgp/1kGP.panel
```

**Expected output:**

```
sample	population	superpopulation	sex
HG00096	GBR	EUR	male
HG00097	GBR	EUR	female
HG00099	GBR	EUR	female
HG00100	GBR	EUR	female
```

GraphMana reads the first three columns and ignores additional columns. The
file can be used directly.

## Step 3: Create the input file list

Rather than passing 22 `--input` flags, create a file listing all VCF paths:

```bash
ls 1kgp/vcf/*.vcf.gz > 1kgp/vcf_list.txt
wc -l 1kgp/vcf_list.txt
```

**Expected output:**

```
22 1kgp/vcf_list.txt
```

## Step 4: Generate CSV files with prepare-csv

The `prepare-csv` step runs without Neo4j. It parses all 22 VCFs, packs
genotypes into byte arrays, computes per-population statistics, and writes CSV
files. With 16 threads, VCFs are processed in parallel.

```bash
graphmana prepare-csv \
    --input-list 1kgp/vcf_list.txt \
    --population-map 1kgp/1kGP.panel \
    --output-dir 1kgp/csv_out \
    --reference GRCh38 \
    --stratify-by superpopulation \
    --threads 16 \
    --verbose
```

**Expected output (abbreviated):**

```
Processing chr1 (16 threads)...
  chr1: 6,468,094 variants, 3,202 samples
Processing chr2...
  chr2: 6,082,751 variants, 3,202 samples
...
Processing chr22...
  chr22: 1,073,621 variants, 3,202 samples
CSV generation complete: 70,726,881 variants, 3,202 samples, 26 populations
Output: 1kgp/csv_out
```

**Timing**: ~2 hours on 16 cores. The bottleneck is VCF parsing and genotype
packing, which is CPU-bound. Each chromosome is processed independently.

**Disk usage**: The CSV output directory will be approximately 150-200 GB.

```bash
du -sh 1kgp/csv_out/
```

**Expected output:**

```
180G    1kgp/csv_out/
```

## Step 5: Load CSVs into Neo4j

The `load-csv` step uses `neo4j-admin database import` for bulk loading. This
is dramatically faster than transactional Cypher imports. Neo4j must be
**stopped** before running this command.

```bash
graphmana neo4j-stop --neo4j-home $GRAPHMANA_NEO4J_HOME

graphmana load-csv \
    --csv-dir 1kgp/csv_out \
    --neo4j-home $GRAPHMANA_NEO4J_HOME \
    --auto-start-neo4j
```

**Expected output:**

```
neo4j-admin import completed successfully.
Schema metadata and indexes created.
```

**Timing**: ~3 minutes. The `neo4j-admin import` tool is optimized for bulk
loading and bypasses the transaction layer entirely.

## Step 6: Start Neo4j and verify

```bash
graphmana neo4j-start --neo4j-home $GRAPHMANA_NEO4J_HOME --wait

graphmana status --detailed
```

**Expected output:**

```
GraphMana v1.0.0-dev
Connected to: bolt://localhost:7687

Node counts:
  Variant          70,726,881
  Sample                3,202
  Population               26
  Chromosome               22
  Gene                      0
  VCFHeader                22

Schema version:   0.1.0
Reference genome: GRCh38

Total nodes:      70,730,131
Relationships:    70,730,079
```

## Step 7: Verify with a quick export

Run a fast sanity check by exporting TreeMix format. This reads only the
pre-computed population arrays (FAST PATH), so it completes quickly even for
70M+ variants.

```bash
graphmana export \
    --format treemix \
    --output 1kgp/exports/1kgp_all.treemix.gz
```

**Expected output:**

```
Export complete (treemix): 70,726,881 variants
```

## Database size on disk

After import and Neo4j compaction, the database directory will be
approximately 130-200 GB for the full 1KGP dataset.

```bash
du -sh $GRAPHMANA_NEO4J_DATA_DIR
```

**Expected output:**

```
168G    /local/scratch/neo4j_data
```

## Incremental addition

After the initial import, new samples can be added incrementally without
re-processing the existing data. This uses the `ingest` command in
incremental mode, which connects to a running Neo4j instance via Bolt
(unlike initial mode, which uses `neo4j-admin import`):

```bash
graphmana ingest \
    --input new_samples.vcf.gz \
    --population-map new_samples_panel.tsv \
    --mode incremental \
    --neo4j-home $GRAPHMANA_NEO4J_HOME \
    --auto-start-neo4j
```

Incremental mode extends the packed genotype arrays on existing Variant nodes
and creates new Variant nodes for sites not seen before. Population statistics
are recomputed automatically.

## Single-chromosome import (for testing)

To import only chr22 for quick testing (1.07M variants, completes in
minutes):

```bash
graphmana prepare-csv \
    --input 1kgp/vcf/1kGP_high_coverage_Illumina.chr22.filtered.SNV_INDEL_SV_phased_panel.vcf.gz \
    --population-map 1kgp/1kGP.panel \
    --output-dir 1kgp/csv_chr22 \
    --reference GRCh38 \
    --stratify-by superpopulation \
    --threads 4

graphmana load-csv \
    --csv-dir 1kgp/csv_chr22 \
    --neo4j-home $GRAPHMANA_NEO4J_HOME \
    --auto-start-neo4j
```

## HPC / cluster deployment

On HPC systems, the two-step split is the recommended approach:

1. Run `prepare-csv` as a batch job on any compute node (no Neo4j needed)
2. Copy the CSV output to a node with local SSD
3. Run `load-csv` on that node

See `scripts/cluster/` for SLURM and PBS example job scripts.

```bash
# Example: SLURM batch job for CSV generation
sbatch scripts/cluster/slurm_prepare_csv.sh
```

## What you learned

- The two-step import (`prepare-csv` + `load-csv`) separates CPU-intensive
  parsing from fast bulk loading
- `prepare-csv` parallelizes across chromosomes and does not need Neo4j
- `load-csv` completes in minutes via `neo4j-admin import`
- The full 1KGP (3,202 samples, 70.7M variants) imports in ~2 hours
- Incremental addition extends existing arrays without re-processing

## See also

- [01-quickstart.md](01-quickstart.md) -- Quick start with demo data
- [03-export-formats.md](03-export-formats.md) -- Exporting the imported data
- [04-cohort-management.md](04-cohort-management.md) -- Defining population subsets
- [05-annotation.md](05-annotation.md) -- Adding VEP, CADD, and ClinVar annotations
