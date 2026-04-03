# GraphMana Cluster Deployment Scripts

Example job scripts for running GraphMana on HPC clusters.

## Quick Start

### 1. Install Neo4j in user space (one-time)

```bash
graphmana setup-neo4j --install-dir $HOME/neo4j --memory-auto
```

### 2. Two-step workflow (recommended)

```bash
# Step 1: CSV generation — runs on any compute node, no Neo4j needed
sbatch slurm_prepare_csv.sh data.vcf.gz populations.tsv

# Step 2: Load into Neo4j — needs local SSD for data directory
sbatch slurm_load_csv.sh /scratch/$USER/graphmana_csv $HOME/neo4j
```

### 3. Single-step workflow

```bash
sbatch slurm_ingest_single.sh data.vcf.gz populations.tsv
```

### 4. Export

```bash
# Start Neo4j first (interactive session or in the script)
sbatch slurm_export.sh treemix output.treemix.gz
```

## Scripts

| Script | Scheduler | Purpose |
|--------|-----------|---------|
| `slurm_prepare_csv.sh` | SLURM | CSV generation (Step 1) |
| `slurm_load_csv.sh` | SLURM | Neo4j import (Step 2) |
| `slurm_ingest_single.sh` | SLURM | Combined import |
| `slurm_export.sh` | SLURM | Export from graph |
| `slurm_interactive.sh` | Source | Interactive session setup |
| `pbs_prepare_csv.sh` | PBS/Torque | CSV generation (Step 1) |
| `pbs_load_csv.sh` | PBS/Torque | Neo4j import (Step 2) |
| `pbs_export.sh` | PBS/Torque | Export from graph |
| `pbs_interactive.sh` | Source | Interactive session setup (PBS) |

## Key Notes

- **Neo4j data directory MUST be on local SSD/scratch** (not NFS/Lustre/GPFS).
  Use `graphmana check-filesystem --neo4j-data-dir /path` to verify.
- Adjust `--cpus-per-task` / `--mem` to match your cluster's resources.
- The `prepare-csv` step is embarrassingly parallel and benefits from more CPUs.
- Java 21+ must be available (`module load java/21` on most clusters).
