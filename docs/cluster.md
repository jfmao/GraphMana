# GraphMana Cluster Deployment Guide

This guide covers deploying and using GraphMana on HPC clusters (SLURM, PBS/Torque) where users typically do not have root access or persistent services.

## Overview

GraphMana supports four deployment models on clusters, from simplest to most scalable:

| Model | Neo4j Lifecycle | Best For |
|-------|----------------|----------|
| Dedicated node | Persistent service | Labs with sysadmin support |
| Interactive job | Manual start/stop | Exploratory analysis |
| Batch job | Auto start/stop | Automated pipelines |
| Two-step split | CSV without Neo4j | Large imports, max parallelism |

## Model 1: Dedicated Node

A lab secures a persistent VM or node running Neo4j as a service. Users connect via Bolt from any cluster node.

```bash
# On the dedicated node (one-time setup)
graphmana setup-neo4j --install-dir /opt/neo4j --memory-auto
graphmana neo4j-start --neo4j-home /opt/neo4j/neo4j-community-* --wait

# From any cluster node
graphmana ingest --input data.vcf.gz --population-map pops.tsv \
    --neo4j-uri bolt://dedicated-node:7687
```

## Model 2: Interactive Job

Request an interactive node, start Neo4j in user space, run commands, then stop.

```bash
# Request resources
srun --nodes=1 --cpus-per-task=16 --mem=128G --time=8:00:00 --pty bash

# Start Neo4j
graphmana neo4j-start \
    --neo4j-home $HOME/neo4j/neo4j-community-5.26.2 \
    --data-dir /scratch/$USER/graphmana_db \
    --wait

# Run GraphMana commands
graphmana ingest --input data.vcf.gz --population-map pops.tsv \
    --neo4j-home $HOME/neo4j/neo4j-community-5.26.2 --threads 16
graphmana export --format treemix --output treemix.gz
graphmana export --format vcf --output exported.vcf.gz --threads 16

# Stop Neo4j
graphmana neo4j-stop --neo4j-home $HOME/neo4j/neo4j-community-5.26.2
```

## Model 3: Batch Job with Auto Start/Stop

Submit a SLURM job that automatically manages the Neo4j lifecycle:

```bash
#!/bin/bash
#SBATCH --job-name=graphmana_ingest
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=4:00:00

graphmana ingest \
    --input data.vcf.gz \
    --population-map pops.tsv \
    --neo4j-home $HOME/neo4j/neo4j-community-5.26.2 \
    --neo4j-data-dir /scratch/$USER/graphmana_db \
    --auto-start-neo4j \
    --threads 16
```

The `--auto-start-neo4j` flag starts Neo4j before the operation and stops it when done, even if an error occurs.

## Model 4: Two-Step Split (Recommended)

Separate CPU-intensive CSV generation from database loading. CSV generation needs no Neo4j and is embarrassingly parallel -- ideal for cluster compute nodes.

### Step 1: Generate CSVs (any compute node)

```bash
#!/bin/bash
#SBATCH --job-name=graphmana_csv
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=4:00:00

graphmana prepare-csv \
    --input data.vcf.gz \
    --population-map pops.tsv \
    --output-dir /scratch/$USER/csv_out \
    --reference GRCh38 \
    --threads 16
```

### Step 2: Load into Neo4j (needs Neo4j)

```bash
#!/bin/bash
#SBATCH --job-name=graphmana_load
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=1:00:00

graphmana load-csv \
    --csv-dir /scratch/$USER/csv_out \
    --neo4j-home $HOME/neo4j/neo4j-community-5.26.2 \
    --neo4j-data-dir /scratch/$USER/graphmana_db \
    --auto-start-neo4j
```

## Installation on a Cluster

### 1. Install Neo4j in User Space

```bash
# Load Java 21 (cluster-specific -- check `module avail`)
module load java/21

# Download and configure Neo4j
graphmana setup-neo4j \
    --install-dir $HOME/neo4j \
    --data-dir /scratch/$USER/graphmana_db \
    --memory-auto
```

This downloads Neo4j Community, extracts it, configures memory based on available RAM, and sets the data directory. No root access needed.

### 2. Install Python CLI

```bash
# With pip (user install)
pip install --user /path/to/graphmana-cli

# Or with a virtual environment
python -m venv ~/graphmana-env
source ~/graphmana-env/bin/activate
pip install /path/to/graphmana-cli
```

### 3. Verify Java Version

Neo4j 5.x requires Java 21+. Check with:

```bash
java -version
```

If the default Java is too old, load the correct module:

```bash
module load java/21    # SLURM clusters
module load jdk/21     # Some PBS systems
```

## Filesystem Guidance

**Critical**: Neo4j performs extremely poorly on network filesystems due to random I/O patterns.

| Component | Filesystem | Notes |
|-----------|-----------|-------|
| Neo4j data directory | **Local SSD/scratch only** | `/scratch`, `/tmp/local`, `/local` |
| CSV output directory | Shared OK | Sequential writes |
| VCF input files | Shared OK | Sequential reads |
| Export output | Shared OK | Sequential writes |

Use `graphmana check-filesystem` to verify:

```bash
graphmana check-filesystem --neo4j-data-dir /scratch/$USER/graphmana_db
# Output: Status: OK (local storage)

graphmana check-filesystem --neo4j-data-dir /home/$USER/data
# WARNING: /home is on nfs4 -- Neo4j will be extremely slow
```

## SLURM Job Scripts

Example scripts are provided in `scripts/cluster/`:

### Ingest (single job)

```bash
#!/bin/bash
#SBATCH --job-name=graphmana_ingest
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=8:00:00
#SBATCH --output=graphmana_ingest_%j.log

module load java/21
source ~/graphmana-env/bin/activate

graphmana ingest \
    --input $1 \
    --population-map $2 \
    --neo4j-home $HOME/neo4j/neo4j-community-5.26.2 \
    --neo4j-data-dir /scratch/$USER/graphmana_db \
    --auto-start-neo4j \
    --reference GRCh38 \
    --threads ${SLURM_CPUS_PER_TASK} \
    --verbose
```

Submit: `sbatch slurm_ingest.sh data.vcf.gz populations.tsv`

### Export

```bash
#!/bin/bash
#SBATCH --job-name=graphmana_export
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=4:00:00
#SBATCH --output=graphmana_export_%j.log

module load java/21
source ~/graphmana-env/bin/activate

graphmana export \
    --format $1 \
    --output /scratch/$USER/exports/$2 \
    --neo4j-home $HOME/neo4j/neo4j-community-5.26.2 \
    --neo4j-data-dir /scratch/$USER/graphmana_db \
    --auto-start-neo4j \
    --threads ${SLURM_CPUS_PER_TASK} \
    --verbose
```

Submit: `sbatch slurm_export.sh treemix treemix_output.gz`

## PBS/Torque Equivalents

```bash
#!/bin/bash
#PBS -N graphmana_csv
#PBS -l select=1:ncpus=16:mem=64gb
#PBS -l walltime=4:00:00

module load java/21
source ~/graphmana-env/bin/activate
cd $PBS_O_WORKDIR

graphmana prepare-csv \
    --input data.vcf.gz \
    --population-map pops.tsv \
    --output-dir /scratch/$USER/csv_out \
    --threads 16
```

## Resource Estimation

Before submitting a SLURM/PBS job, use these tables to determine what resources to request. All numbers are based on actual benchmarks with the 1000 Genomes Project dataset (3,202 samples, 70.7M whole-genome variants) on a 32-core, 64 GB RAM workstation with NVMe SSD.

### Choosing RAM, CPUs, and Walltime

| Operation | Dataset | CPUs | RAM | Walltime | Notes |
|-----------|---------|------|-----|----------|-------|
| `prepare-csv` | chr22 (1M variants) | 4 | 16 GB | 30 min | Single-chromosome |
| `prepare-csv` | WGS 22 chr (70M variants) | 16 | 64 GB | 2-3 hr | Multi-file parallel; 1 worker per chromosome |
| `load-csv` (neo4j-admin) | WGS (220 GB CSVs) | 4 | 32 GB | 5-10 min | CPU-light; mostly sequential I/O |
| Neo4j start + indexing | Any | 2 | 32 GB | 2-5 min | One-time after import |
| Export FAST PATH (TreeMix, SFS) | chr22 | 1 | 16 GB | 2 min | No genotype unpacking |
| Export FAST PATH (TreeMix, SFS) | All 22 chr | 1 | 32 GB | 100 min | I/O-bound; more RAM helps pagecache |
| Export FULL PATH (VCF) | chr22 | 1 | 32 GB | 11 min | Unpacks 3,202 genotypes per variant |
| Export FULL PATH (PLINK) | All 22 chr, 8 threads | 8 | 64 GB | 3 min | Parallel by chromosome |
| Export FULL PATH (EIGENSTRAT) | All 22 chr, 8 threads | 8 | 64 GB | 3.7 hr | Large output (184 GB .geno) |
| Export FULL PATH (VCF) | All 22 chr, 8 threads | 8 | 64 GB | 4-6 hr | Largest export |

### Neo4j Memory Configuration

Neo4j needs two memory pools: **heap** (JVM, for query execution) and **pagecache** (for disk-resident data). These compete with your export process for RAM.

| Total RAM | Neo4j Heap | Pagecache | Remaining for Export | Suitable For |
|-----------|-----------|-----------|---------------------|-------------|
| 32 GB | 4 GB | 8 GB | 20 GB | chr22 or exome-only |
| 64 GB | 16 GB | 16 GB | 32 GB | WGS up to 3K samples |
| 128 GB | 16 GB | 48 GB | 64 GB | WGS up to 10K samples |
| 256 GB | 16 GB | 128 GB | 112 GB | WGS up to 30K samples |
| 512 GB | 16 GB | 256 GB | 240 GB | WGS up to 50K samples |

Edit `$NEO4J_HOME/conf/neo4j.conf`:
```
server.memory.heap.initial_size=16g
server.memory.heap.max_size=16g
server.memory.pagecache.size=16g
```

**Rule of thumb**: Pagecache should be at least 10% of the database size on disk. More pagecache means fewer disk reads, which is critical for all-chromosome exports. Heap of 16 GB is sufficient for all operations; going higher provides diminishing returns.

### Storage Requirements

| Component | Formula | 3K samples WGS | 10K samples WGS |
|-----------|---------|----------------|-----------------|
| Input VCFs | varies | 28 GB | ~90 GB |
| CSV intermediates | ~3x DB size | 220 GB | ~700 GB |
| Neo4j database | see below | 166 GB | ~500 GB |
| Export outputs | varies by format | 1-200 GB per format | similar |
| **Total scratch needed** | | **~500 GB** | **~1.5 TB** |

Database size by sample count (whole-genome, ~70M biallelic variants):

| Samples | DB Size |
|---------|---------|
| 100 | ~20 GB |
| 1,000 | ~50 GB |
| 3,200 | ~166 GB |
| 10,000 | ~500 GB |
| 50,000 | ~2-3 TB |

Request sufficient scratch space before starting a large import. The CSV intermediates can be deleted after `load-csv` completes.

## End-to-End Workflow: 1000 Genomes on a SLURM Cluster

This walkthrough demonstrates importing and exporting the full 1KGP dataset (3,202 samples, 22 autosomes) using the recommended two-step split model. Adapt paths and resource requests to your cluster.

### Prerequisites

```bash
# One-time setup (login node)
module load java/21

# Install Miniforge if no conda available
curl -L -O https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh
bash Miniforge3-Linux-x86_64.sh -b -p $HOME/miniforge3
source $HOME/miniforge3/bin/activate

# Create environment
conda create -n graphmana -c conda-forge -c bioconda python=3.12 cyvcf2 numpy -y
conda activate graphmana
pip install /path/to/graphmana-cli

# Install Neo4j in user space
wget https://dist.neo4j.org/neo4j-community-5.26.0-unix.tar.gz
tar xzf neo4j-community-5.26.0-unix.tar.gz -C $HOME/
mv $HOME/neo4j-community-5.26.0 $HOME/neo4j

# Set initial password
$HOME/neo4j/bin/neo4j-admin dbms set-initial-password graphmana

# Verify filesystem for Neo4j data
graphmana check-filesystem --neo4j-data-dir /scratch/$USER/graphmana_db
```

### Step 1: Generate CSVs (compute node, no Neo4j)

Create a file listing your VCF paths:
```bash
ls /shared/data/1kgp/vcf/*.vcf.gz > vcf_list.txt
```

Submit the CSV generation job:
```bash
#!/bin/bash
#SBATCH --job-name=gm_csv
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=4:00:00
#SBATCH --output=gm_csv_%j.log

module load java/21
source $HOME/miniforge3/bin/activate graphmana

graphmana prepare-csv \
    --input-list vcf_list.txt \
    --population-map /shared/data/1kgp/population_panel.ped \
    --output-dir /scratch/$USER/1kgp_csv \
    --reference GRCh38 \
    --stratify-by population \
    --threads ${SLURM_CPUS_PER_TASK} \
    --verbose

echo "CSV generation complete. Output size:"
du -sh /scratch/$USER/1kgp_csv/
```

Expected: ~2 hours for 22 chromosomes with 16 threads. Output: ~220 GB of CSVs.

### Step 2: Import into Neo4j (needs Neo4j binary, not running)

Submit with SLURM dependency to run after Step 1:
```bash
#!/bin/bash
#SBATCH --job-name=gm_load
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=1:00:00
#SBATCH --output=gm_load_%j.log

module load java/21
source $HOME/miniforge3/bin/activate graphmana

# Configure Neo4j memory for this machine
cat > $HOME/neo4j/conf/neo4j.conf.d/memory.conf <<CONF
server.memory.heap.initial_size=16g
server.memory.heap.max_size=16g
server.memory.pagecache.size=16g
CONF

# Import (Neo4j must be stopped)
graphmana load-csv \
    --csv-dir /scratch/$USER/1kgp_csv \
    --neo4j-home $HOME/neo4j \
    --database neo4j \
    --verbose

echo "Import complete. Database size:"
du -sh $HOME/neo4j/data/databases/neo4j/
```

Chain with dependency: `sbatch --dependency=afterok:$CSV_JOBID slurm_load.sh`

Expected: ~5 minutes for neo4j-admin import of 220 GB CSVs.

### Step 3: Start Neo4j and apply indexes

```bash
#!/bin/bash
#SBATCH --job-name=gm_index
#SBATCH --cpus-per-task=4
#SBATCH --mem=64G
#SBATCH --time=0:30:00
#SBATCH --output=gm_index_%j.log

module load java/21
source $HOME/miniforge3/bin/activate graphmana

# Start Neo4j
$HOME/neo4j/bin/neo4j start
sleep 30

# Apply schema indexes and verify
graphmana status --detailed --neo4j-password graphmana

# Keep Neo4j running for exports (or stop here if submitting export jobs later)
```

### Step 4: Export (while Neo4j is running)

Submit export jobs. FAST PATH exports (TreeMix, SFS) need minimal resources. FULL PATH exports (VCF, PLINK) need more RAM.

```bash
#!/bin/bash
#SBATCH --job-name=gm_export
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=6:00:00
#SBATCH --output=gm_export_%j.log

module load java/21
source $HOME/miniforge3/bin/activate graphmana

OUTDIR=/scratch/$USER/exports
mkdir -p $OUTDIR
NEO4J="--neo4j-password graphmana"

# FAST PATH exports (minutes)
graphmana export --format treemix --output $OUTDIR/treemix.gz $NEO4J
graphmana export --format sfs-dadi --output $OUTDIR/sfs_yri_ceu.fs \
    --sfs-populations YRI --sfs-populations CEU \
    --sfs-projection 20 --sfs-projection 20 --sfs-folded $NEO4J

# FULL PATH exports (hours)
graphmana export --format plink --output $OUTDIR/plink_all \
    --filter-variant-type SNP --threads 8 $NEO4J
graphmana export --format vcf --output $OUTDIR/all.vcf.gz \
    --output-type z --threads 8 $NEO4J
```

### Step 5: Cleanup

```bash
# Stop Neo4j when done
$HOME/neo4j/bin/neo4j stop

# Delete CSV intermediates (no longer needed after import)
rm -rf /scratch/$USER/1kgp_csv

# Keep the database for future queries
echo "Database at: $HOME/neo4j/data/databases/neo4j/"
echo "To resume later: module load java/21 && $HOME/neo4j/bin/neo4j start"
```

### Chaining Jobs with SLURM Dependencies

```bash
# Submit the full pipeline with automatic dependencies
JOB1=$(sbatch --parsable slurm_prepare_csv.sh)
JOB2=$(sbatch --parsable --dependency=afterok:$JOB1 slurm_load_csv.sh)
JOB3=$(sbatch --parsable --dependency=afterok:$JOB2 slurm_index.sh)
JOB4=$(sbatch --parsable --dependency=afterok:$JOB3 slurm_export.sh)
echo "Pipeline submitted: CSV($JOB1) -> Load($JOB2) -> Index($JOB3) -> Export($JOB4)"
```

## Multi-User Scenarios

### Two Researchers on the Same Node

Each user needs a separate Neo4j instance with different ports and data directories:

```bash
# User A: default ports
$HOME/neo4j/bin/neo4j start    # Bolt: 7687, HTTP: 7474
graphmana export --format treemix --output treemix.gz --neo4j-password graphmana

# User B: custom ports — edit neo4j.conf first
cat >> $HOME/neo4j/conf/neo4j.conf <<CONF
server.bolt.listen_address=:7688
server.http.listen_address=:7475
CONF
$HOME/neo4j/bin/neo4j start    # Bolt: 7688, HTTP: 7475
graphmana export --format treemix --output treemix.gz \
    --neo4j-uri bolt://localhost:7688 --neo4j-password graphmana
```

### Shared Database on a Dedicated Node

One database serves multiple users via network. All users connect to the same Bolt endpoint:

```bash
# All users connect to the lab's Neo4j server
graphmana export --format plink --output my_plink \
    --neo4j-uri bolt://lab-server:7687 \
    --neo4j-password graphmana \
    --chromosomes chr22 \
    --filter-variant-type SNP
```

Note: Neo4j Community Edition supports only one database per instance and has no role-based access control. For multi-user access control, consider Neo4j Enterprise or use separate instances per project.

## Conda/Python Environment on Clusters

### Using Module System with Conda

Many clusters use `module` for system software. Combine with conda for Python dependencies:

```bash
# In your .bashrc or job scripts
module load java/21
source $HOME/miniforge3/bin/activate graphmana
```

### Shared Conda Environment for a Lab

A lab admin can create a shared environment accessible to all group members:

```bash
# As admin, install to shared location
conda create -p /shared/envs/graphmana -c conda-forge -c bioconda \
    python=3.12 cyvcf2 numpy -y
conda activate /shared/envs/graphmana
pip install /shared/software/graphmana-cli

# Users activate with:
conda activate /shared/envs/graphmana
```

### cyvcf2 Compilation Issues

cyvcf2 requires htslib, which needs zlib and libbz2. On older cluster nodes:

```bash
# If pip install fails with htslib errors, use conda instead of pip:
conda install -c bioconda cyvcf2

# If conda also fails (very old glibc), build from source:
conda install -c conda-forge htslib
pip install --no-binary cyvcf2 cyvcf2
```

## Troubleshooting

### Database and Connectivity

**Neo4j extremely slow**: Data directory is on a network filesystem (NFS/Lustre/GPFS). Move to local SSD/scratch. Run `graphmana check-filesystem` to verify.

**Neo4j won't start**: Java 21 not found. Check `module load java/21` or equivalent. Also check that Bolt port 7687 is not blocked or in use.

**Port already in use**: Another user or a previous Neo4j process may be running. Check with `lsof -i :7687` or `ss -tlnp | grep 7687`. Either stop the other process or change ports in neo4j.conf.

**Permission denied on Neo4j files**: Neo4j run directory defaults to a system path. Set `server.directories.run` to a user-writable location in neo4j.conf:
```
server.directories.run=/scratch/$USER/neo4j_run
```

### Memory Issues

**Out of memory (OOM kill)**: Increase `--mem` in SLURM. The combined memory of Neo4j (heap + pagecache) and the export process must fit within the allocation. For WGS with 3K+ samples, request 64+ GB.

**Neo4j GC pauses / connection timeouts**: Neo4j heap is too small for the query workload. Increase `server.memory.heap.max_size` to 16 GB. If exporting all chromosomes, GraphMana uses batched pagination to avoid sorting millions of rows in heap.

**Export process killed but Neo4j survives**: The export process exceeded its memory allocation while Neo4j's pre-allocated heap was protected. This typically happens with all-chromosome SFS exports on datasets with 50M+ variants. GraphMana's streaming SFS implementation avoids this; ensure you are running the latest version.

### Import Issues

**prepare-csv succeeds but load-csv fails**: CSV files may have been corrupted or truncated during transfer to shared storage. Verify file sizes: `variant_nodes.csv` should be the largest (roughly 3 KB per variant). Re-run `prepare-csv` if needed.

**load-csv reports "database is in use"**: Neo4j must be stopped before `neo4j-admin import`. Stop it with `$HOME/neo4j/bin/neo4j stop` and wait for it to fully shut down before running `load-csv`.

**Out of disk on scratch**: Use `du -sh /scratch/$USER/` to check usage. The CSV intermediates can be 1.5-3x the final database size. For 3K WGS samples, plan for 500+ GB total. Delete CSVs after successful `load-csv`.

### Job Timeout

**SLURM job times out during export**: All-chromosome FULL PATH exports (VCF, EIGENSTRAT) can take 4-6 hours for 70M variants. Request sufficient walltime. If a job does timeout, the partial output file is incomplete — re-run from the beginning. FAST PATH exports (TreeMix, SFS) take ~100 minutes for all chromosomes.

**Long-running Neo4j sessions drop**: If Neo4j times out the Bolt connection during a long export, this is typically caused by insufficient heap leading to GC pauses. Increase Neo4j heap to 16 GB.
