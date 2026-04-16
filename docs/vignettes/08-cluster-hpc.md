# Vignette 08: HPC Cluster Deployment

This vignette provides an end-to-end walkthrough of running GraphMana on an HPC cluster managed by SLURM. It covers environment setup, all four deployment models, resource estimation, and multi-user scenarios.

For the full reference, see [docs/cluster.md](../cluster.md).

## Prerequisites

- Access to a SLURM-managed cluster with node-local SSD or scratch storage
- Java 21+ and Python 3.11+ available (via `module load` or conda)
- The `graphmana` CLI installed (typically via pip in a conda environment)
- VCF files and population map accessible on shared storage

## Resource Estimation

Before submitting jobs, estimate your resource needs:

| Dataset | RAM | Scratch Disk | Wall Time (import) | Wall Time (export) |
|---------|-----|-------------|--------------------|--------------------|
| 3K samples, WGS | 64 GB | 500 GB | 4 hours | 30 min |
| 3K samples, chr22 | 16 GB | 20 GB | 10 min | 1 min |
| 10K samples, WGS | 128 GB | 1.5 TB | 8 hours | 2 hours |
| 50K samples, WGS | 256 GB | 4 TB | 24+ hours | 8+ hours |

Scratch disk must be local SSD or NVMe -- not NFS, Lustre, or GPFS. Neo4j performs extremely poorly on network filesystems due to random I/O patterns.

## Environment Setup

### Step 1: Create a Conda Environment

```bash
module load anaconda3
conda create -n graphmana python=3.11 -y
conda activate graphmana
pip install graphmana-cli
```

### Step 2: Install Neo4j in User Space

If the cluster has internet access:

```bash
graphmana setup-neo4j \
    --install-dir $HOME/neo4j \
    --data-dir /scratch/$USER/graphmana_db \
    --memory-auto \
    --password mypassword
```

**Air-gapped clusters (no internet):** download the Neo4j tarball on a
machine with access (from the
[GraphMana Zenodo deposit](https://doi.org/10.5281/zenodo.19472835) or
`https://dist.neo4j.org/neo4j-community-5.26.0-unix.tar.gz`), transfer it
to the cluster, and run:

```bash
graphmana setup-neo4j \
    --install-dir $HOME/neo4j \
    --neo4j-tarball /path/to/neo4j-community-5.26.0-unix.tar.gz \
    --data-dir /scratch/$USER/graphmana_db \
    --memory-auto \
    --password mypassword
```

See [INSTALL.md — Offline install](../INSTALL.md#offline--air-gapped-install)
for the full recipe.

Expected output:

```
Downloading Neo4j Community 5.26.2...
Extracting to /home/user/neo4j/neo4j-community-5.26.2
Configuring data directory: /scratch/user/graphmana_db
Auto-configured memory: heap=16g, pagecache=32g
Neo4j installed. Start with: graphmana neo4j-start --neo4j-home ...
```

### Step 3: Verify the Filesystem

```bash
graphmana check-filesystem --neo4j-data-dir /scratch/$USER/graphmana_db
```

Expected output:

```
Checking /scratch/user/graphmana_db ...
  Filesystem type: ext4
  Mount point:     /scratch
  Is network FS:   No
  Write test:      OK (42 MB/s random, 850 MB/s sequential)

Result: PASS -- suitable for Neo4j data storage.
```

If the check reports a network filesystem, move your data directory to node-local storage.

### Step 4: Verify the full installation

The `doctor` command runs all pre-flight checks in one shot — Java version,
Neo4j home, bolt port, plugin JAR, config file, data directory filesystem,
and password strength:

```bash
graphmana doctor
```

The `setup-neo4j` command writes `~/.graphmana/config.yaml` with the Neo4j
home, password, and ports, so subsequent commands (`neo4j-start`, `ingest`,
`export`, etc.) no longer need `--neo4j-home` or `--neo4j-password` on every
invocation.

## Deployment Model 1: Dedicated Node

Best for labs with sysadmin support. Neo4j runs as a persistent service on a fixed node; users connect from any cluster node.

```bash
# One-time setup on the dedicated node
graphmana setup-neo4j --install-dir /opt/neo4j --memory-auto
graphmana neo4j-start \
    --neo4j-home /opt/neo4j/neo4j-community-5.26.2 \
    --data-dir /local-ssd/graphmana_db \
    --wait

# From any cluster node, import data
graphmana ingest \
    --input /shared/data/cohort.vcf.gz \
    --population-map /shared/data/pops.tsv \
    --neo4j-uri bolt://dedicated-node:7687 \
    --reference GRCh38 \
    --threads 16
```

## Deployment Model 2: Interactive Job

Request an interactive session, start Neo4j manually, run commands, stop when done.

```bash
# Request an interactive node
srun --nodes=1 --cpus-per-task=16 --mem=128G --time=8:00:00 --pty bash

# Load environment
module load java/21 anaconda3
conda activate graphmana

# Start Neo4j on node-local scratch
graphmana neo4j-start \
    --neo4j-home $HOME/neo4j/neo4j-community-5.26.2 \
    --data-dir /scratch/$USER/graphmana_db \
    --wait

# Run your workflow
graphmana ingest \
    --input /shared/data/chr22.vcf.gz \
    --population-map /shared/data/pops.tsv \
    --reference GRCh38 \
    --threads 16

graphmana export --format treemix --output /shared/results/treemix.gz
graphmana export --format vcf --output /shared/results/exported.vcf.gz --threads 16

# Stop Neo4j before the job ends
graphmana neo4j-stop --neo4j-home $HOME/neo4j/neo4j-community-5.26.2
```

## Deployment Model 3: Batch Job with Auto Start/Stop

Submit a non-interactive SLURM job. The `--auto-start-neo4j` flag handles Neo4j lifecycle automatically.

Create a job script `ingest_job.sh`:

```bash
#!/bin/bash
#SBATCH --job-name=graphmana_ingest
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=8:00:00
#SBATCH --output=graphmana_ingest_%j.log

module load java/21 anaconda3
conda activate graphmana

graphmana ingest \
    --input /shared/data/cohort.vcf.gz \
    --population-map /shared/data/pops.tsv \
    --neo4j-home $HOME/neo4j/neo4j-community-5.26.2 \
    --neo4j-data-dir /scratch/$USER/graphmana_db \
    --auto-start-neo4j \
    --reference GRCh38 \
    --threads 16 \
    --verbose
```

Submit:

```bash
sbatch ingest_job.sh
```

The `--auto-start-neo4j` flag ensures Neo4j starts before the import and stops afterward, even if the import fails.

## Deployment Model 4: Two-Step Split (Recommended)

This is the recommended approach for large imports. It separates CPU-intensive CSV generation (runs on any compute node without Neo4j) from database loading (requires Neo4j).

### Step 1: Generate CSVs (Any Compute Node)

Create `prepare_csv.sh`:

```bash
#!/bin/bash
#SBATCH --job-name=graphmana_csv
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=4:00:00
#SBATCH --output=graphmana_csv_%j.log

module load anaconda3
conda activate graphmana

graphmana prepare-csv \
    --input /shared/data/cohort.vcf.gz \
    --population-map /shared/data/pops.tsv \
    --output-dir /shared/csv_staging/$USER/cohort_csv \
    --reference GRCh38 \
    --threads 16 \
    --verbose
```

Submit:

```bash
CSV_JOB=$(sbatch --parsable prepare_csv.sh)
echo "CSV job: $CSV_JOB"
```

Expected output (in the log file):

```
Parsing VCF: /shared/data/cohort.vcf.gz
  3,202 samples, 26 populations
Writing CSV files to /shared/csv_staging/user/cohort_csv/
  variants.csv: 1,072,533 rows
  samples.csv: 3,202 rows
  populations.csv: 26 rows
  chromosomes.csv: 1 rows
  schema_metadata.csv: 1 rows
Done. CSV files ready for load-csv.
```

### Step 2: Load CSVs into Neo4j

Create `load_csv.sh`:

```bash
#!/bin/bash
#SBATCH --job-name=graphmana_load
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=2:00:00
#SBATCH --output=graphmana_load_%j.log

module load java/21 anaconda3
conda activate graphmana

graphmana load-csv \
    --csv-dir /shared/csv_staging/$USER/cohort_csv \
    --neo4j-home $HOME/neo4j/neo4j-community-5.26.2 \
    --auto-start-neo4j
```

Submit with a dependency on the CSV job:

```bash
sbatch --dependency=afterok:$CSV_JOB load_csv.sh
```

### Why Two-Step is Recommended

1. **No Neo4j needed for Step 1.** CSV generation is pure Python -- it runs on any compute node. No Java, no Neo4j installation, no local SSD requirement.
2. **Embarrassingly parallel.** Step 1 scales linearly with `--threads`.
3. **Fault tolerance.** If loading fails (wrong Neo4j version, disk full), the CSVs remain and you can retry Step 2 without re-parsing the VCF.
4. **Staging flexibility.** CSVs can be written to shared storage and loaded from any node with Neo4j access.

## Multi-User Scenario

Multiple users can run independent GraphMana databases on the same cluster by using different Neo4j ports and data directories.

### User A:

```bash
# In neo4j.conf (or via setup-neo4j --data-dir):
# server.bolt.listen_address=:7687
graphmana neo4j-start \
    --neo4j-home $HOME/neo4j/neo4j-community-5.26.2 \
    --data-dir /scratch/userA/graphmana_db \
    --wait

graphmana status --neo4j-uri bolt://$(hostname):7687
```

### User B (different port):

Edit `$HOME/neo4j/neo4j-community-5.26.2/conf/neo4j.conf`:

```properties
server.bolt.listen_address=:7688
server.http.listen_address=:7475
```

Then:

```bash
graphmana neo4j-start \
    --neo4j-home $HOME/neo4j/neo4j-community-5.26.2 \
    --data-dir /scratch/userB/graphmana_db \
    --wait

graphmana status --neo4j-uri bolt://$(hostname):7688
```

Each user has a fully independent database. There is no shared state.

## Complete Pipeline Script

Here is a complete SLURM pipeline that chains all steps with dependencies:

```bash
#!/bin/bash
# submit_pipeline.sh -- Submit a full GraphMana import + export pipeline

INPUT_VCF="/shared/data/cohort.vcf.gz"
POP_MAP="/shared/data/pops.tsv"
CSV_DIR="/shared/csv_staging/$USER/cohort_csv"
NEO4J_HOME="$HOME/neo4j/neo4j-community-5.26.2"
RESULTS="/shared/results/$USER"

mkdir -p "$RESULTS"

# Step 1: Generate CSVs
CSV_JOB=$(sbatch --parsable <<'CSVEOF'
#!/bin/bash
#SBATCH --job-name=gm_csv
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=4:00:00
module load anaconda3 && conda activate graphmana
graphmana prepare-csv \
    --input $INPUT_VCF --population-map $POP_MAP \
    --output-dir $CSV_DIR --reference GRCh38 --threads 16
CSVEOF
)

# Step 2: Load into Neo4j
LOAD_JOB=$(sbatch --parsable --dependency=afterok:$CSV_JOB <<LOADEOF
#!/bin/bash
#SBATCH --job-name=gm_load
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=2:00:00
module load java/21 anaconda3 && conda activate graphmana
graphmana load-csv --csv-dir $CSV_DIR \
    --neo4j-home $NEO4J_HOME --auto-start-neo4j
LOADEOF
)

# Step 3: Export (runs after load completes)
sbatch --dependency=afterok:$LOAD_JOB <<EXPEOF
#!/bin/bash
#SBATCH --job-name=gm_export
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=2:00:00
module load java/21 anaconda3 && conda activate graphmana
graphmana export --format treemix --output $RESULTS/treemix.gz \
    --neo4j-home $NEO4J_HOME --auto-start-neo4j
graphmana export --format vcf --output $RESULTS/exported.vcf.gz \
    --neo4j-home $NEO4J_HOME --auto-start-neo4j --threads 8
EXPEOF

echo "Pipeline submitted: CSV=$CSV_JOB -> Load=$LOAD_JOB -> Export"
```

## Troubleshooting

**Neo4j extremely slow**: The data directory is likely on a network filesystem. Run `graphmana check-filesystem --neo4j-data-dir /path/to/data` and move to local SSD/scratch.

**Neo4j won't start**: Ensure Java 21 is on PATH (`module load java/21`). Check that ports 7474/7687 are not blocked or in use by another user.

**Out of disk on scratch**: Whole-genome at 50K samples requires up to 4 TB. Check your scratch allocation with your cluster's quota tools before starting.

**CSV files truncated**: If `load-csv` fails with parse errors, verify file sizes from `prepare-csv` output. Network storage can silently truncate large writes under quota pressure. Re-run `prepare-csv` if needed.

## See Also

- [Cluster Deployment Guide](../cluster.md) -- Full reference documentation
- [Tutorial](../tutorial.md) -- Getting started with GraphMana
- [Vignette 10: Database Administration](10-database-admin.md) -- Snapshots and maintenance
