# GraphMana Installation Guide

GraphMana runs on Linux and macOS without admin privileges. Choose the
installation method that suits your environment.

## Quick Install (recommended)

One command installs everything (conda, Python, Java, Neo4j, GraphMana):

```bash
curl -sSL https://raw.githubusercontent.com/jfmao/GraphMana/main/install.sh | bash
```

After installation:

```bash
conda activate graphmana
graphmana version
```

## Method 1: pip install (requires Python 3.11+ and conda for cyvcf2)

```bash
# Create a conda environment with cyvcf2 (needs C libraries)
conda create -n graphmana -c conda-forge -c bioconda python=3.12 cyvcf2 openjdk=21 -y
conda activate graphmana

# Install GraphMana
pip install graphmana

# Setup Neo4j (downloads and configures, no admin needed)
graphmana setup-neo4j --install-dir ~/neo4j --memory-auto
```

## Method 2: conda install (when available on bioconda)

```bash
conda install -c bioconda -c conda-forge graphmana
graphmana setup-neo4j --install-dir ~/neo4j --memory-auto
```

## Method 3: Docker

```bash
docker pull ghcr.io/jfmao/graphmana:latest
docker run -p 7474:7474 -p 7687:7687 -v graphmana_data:/data ghcr.io/jfmao/graphmana:latest
```

Or build from source:

```bash
git clone https://github.com/jfmao/GraphMana.git
cd GraphMana
docker compose up --build
```

## Method 4: From source (development)

```bash
git clone https://github.com/jfmao/GraphMana.git
cd GraphMana

# Create conda environment
conda create -n graphmana -c conda-forge -c bioconda \
    python=3.12 cyvcf2 numpy click pyliftover openjdk=21 maven -y
conda activate graphmana

# Build Java procedures (optional — JAR is bundled)
cd graphmana-procedures && mvn clean package -DskipTests && cd ..

# Install Python CLI in development mode
cd graphmana-cli && pip install -e ".[dev]" && cd ..

# Run tests
cd graphmana-cli && pytest -v && cd ..

# Setup Neo4j
graphmana setup-neo4j --install-dir ~/neo4j --memory-auto
```

## Method 5: HPC cluster (no Docker)

```bash
# On a login node:
module load java/21  # or use --install-java flag below

graphmana setup-neo4j --install-dir $HOME/neo4j --install-java --memory-auto
graphmana check-filesystem --neo4j-data-dir /scratch/$USER/graphmana_db
```

See [Vignette 08: HPC Cluster Deployment](vignettes/08-cluster-hpc.md) for
SLURM/PBS job scripts.

## What gets installed

| Component | Size | Source |
|-----------|------|--------|
| GraphMana CLI (Python) | ~2 MB | pip / conda |
| GraphMana procedures (Java JAR) | ~31 KB | Bundled in Python package |
| Neo4j Community 5.x | ~130 MB | Downloaded by `setup-neo4j` |
| JDK 21 (optional) | ~200 MB | Downloaded by `--install-java` |
| cyvcf2 + htslib | ~15 MB | conda dependency |

Total: ~350 MB for a complete installation.

## No admin privileges needed

GraphMana installs entirely in user space:
- conda/miniforge installs to `~/miniforge3/`
- Neo4j installs to `~/neo4j/` (or any user-writable directory)
- Java (if needed) installs alongside Neo4j
- The bundled Java procedures JAR is automatically copied to Neo4j plugins

No `sudo`, no system packages, no Docker daemon required.

## Verifying the installation

```bash
graphmana version          # Shows all component versions
graphmana list-formats     # Lists 17 export formats
graphmana config-show      # Shows current configuration
graphmana --help           # Full command list
```

## First import

```bash
# Start Neo4j
graphmana neo4j-start --neo4j-home ~/neo4j --wait

# Import a VCF
graphmana ingest \
    --input your_data.vcf.gz \
    --population-map your_panel.tsv \
    --neo4j-home ~/neo4j

# Check status
graphmana status --detailed

# Export to TreeMix (seconds)
graphmana export --format treemix --output output.treemix.gz
```

## Using an existing Neo4j installation

If Neo4j 5.26.x is already installed on your machine, point GraphMana at it:

```bash
graphmana setup-neo4j --install-dir /path/to/existing/neo4j-parent --skip-download
```

This skips the download and only deploys the GraphMana procedures JAR, sets
the password, and writes `~/.graphmana/config.yaml`. Subsequent commands
(`neo4j-start`, `ingest`, `export`, etc.) read the config file automatically
— you no longer need `--neo4j-home` on every invocation.

If the existing Neo4j is **running**, use `--adopt` to deploy the plugin and
restart it:

```bash
graphmana setup-neo4j --install-dir /path/to/neo4j-parent --adopt
```

The `--adopt` path stops and restarts the instance (an interactive confirmation
is required unless `--i-understand-this-restarts-neo4j` is passed). It is
intended for user-owned, single-user installs only — not shared system Neo4j
at `/opt/neo4j` or container-managed instances. For those, install a separate
GraphMana-managed Neo4j on different ports (see below).

## Offline / air-gapped install

If the target machine cannot reach the internet, download the Neo4j tarball
on a machine that can and transfer it:

1. Download from the [GraphMana Zenodo deposit](https://doi.org/10.5281/zenodo.19603203)
   or from `https://dist.neo4j.org/neo4j-community-5.26.0-unix.tar.gz`.
2. Transfer to the target machine.
3. Run:

```bash
graphmana setup-neo4j \
    --install-dir ~/neo4j \
    --neo4j-tarball /path/to/neo4j-community-5.26.0-unix.tar.gz \
    --memory-auto
```

The `--neo4j-tarball` flag validates the filename against the pattern
`neo4j-community-5.26.x-unix.tar.gz` and extracts the version automatically.

To also deploy a local copy of the procedures JAR (instead of the bundled
one), add `--deploy-plugin /path/to/graphmana-procedures.jar`.

## Port conflicts and multiple Neo4j instances

If another Neo4j is already listening on port 7687, `setup-neo4j` will refuse
to proceed and print instructions:

```
Error: Bolt port 7687 is already in use (PID 12345).

Options:
  1. Stop the existing process:  kill 12345
  2. Install on different ports:
     graphmana setup-neo4j --bolt-port 7688 --http-port 7475 ...
  3. Adopt the running instance:
     graphmana setup-neo4j --adopt --install-dir <existing-neo4j-home> ...
```

To run GraphMana's Neo4j alongside another instance:

```bash
graphmana setup-neo4j --install-dir ~/graphmana-neo4j --bolt-port 7688 --http-port 7475
```

The chosen ports are stored in `~/.graphmana/config.yaml` and used by all
subsequent commands automatically.

To check the full installation health at any time:

```bash
graphmana doctor
```

This verifies Java, Neo4j home, running process, port reachability, plugin
deployment, config file, data directory filesystem, and password strength.

## Troubleshooting

**cyvcf2 installation fails**: Use conda (not pip) to install cyvcf2:
`conda install -c bioconda cyvcf2`

**Java not found**: Use `graphmana setup-neo4j --install-java` to download JDK 21
to user space, or `module load java/21` on HPC clusters.

**Neo4j extremely slow**: Ensure the data directory is on local SSD, not NFS.
Run `graphmana check-filesystem --neo4j-data-dir /path/to/data`.

**Port 7687 in use**: See "Port conflicts" section above, or run
`graphmana doctor` for a full diagnostic.
