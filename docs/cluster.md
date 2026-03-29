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

## Resource Allocation

| Operation | CPUs | RAM | Time (chr22, 3K samples) | Time (WGS, 10K samples) |
|-----------|------|-----|--------------------------|--------------------------|
| prepare-csv | 16 | 64 GB | Minutes | 30-120 min |
| load-csv | 4 | 64 GB | Minutes | 10-30 min |
| ingest (combined) | 16 | 128 GB | Minutes | 30-120 min |
| export (FAST PATH) | 1 | 16 GB | Seconds | Seconds |
| export (FULL PATH) | 8-16 | 64 GB | Seconds | Minutes-hours |

### Storage Requirements

| Samples | WGS Variants | Estimated DB Size |
|---------|-------------|-------------------|
| 100 | 85M | ~20 GB |
| 1,000 | 85M | ~50 GB |
| 3,200 | 85M | ~130-200 GB |
| 10,000 | 85M | ~400-550 GB |
| 50,000 | 85M | ~2-3 TB |

Request sufficient scratch space before starting a large import.

## Troubleshooting

**Neo4j extremely slow**: Data directory is on a network filesystem (NFS/Lustre/GPFS). Move to local SSD/scratch. Run `graphmana check-filesystem` to verify.

**Neo4j won't start**: Java 21 not found. Check `module load java/21` or equivalent. Also check that Bolt port 7687 is not blocked or in use.

**Out of memory**: Increase `--mem` in SLURM. For 10K+ WGS samples, use 128+ GB RAM.

**Out of disk**: 50K WGS produces 2-3 TB. Ensure `/scratch` allocation is sufficient.

**prepare-csv succeeds but load-csv fails**: CSV files may have been corrupted on shared storage. Verify file sizes match expectations. Re-run prepare-csv if needed.

**Permission denied on Neo4j binary**: Ensure Neo4j was installed with `graphmana setup-neo4j` (sets correct permissions).

**Port already in use**: Another user may be running Neo4j on the same node. Edit `$NEO4J_HOME/conf/neo4j.conf` to change `server.bolt.listen_address` and `server.http.listen_address` to use different ports.
