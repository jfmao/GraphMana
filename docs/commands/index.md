# GraphMana Command Reference

Complete reference for all GraphMana CLI commands. Each page documents the command synopsis, all options with types and defaults, and usage examples.

For narrative tutorials and workflows, see the [Vignettes](../vignettes/).

## Command Overview

### Core Data Operations

| Command | Description |
|---------|-------------|
| [graphmana ingest](graphmana-ingest.md) | Import VCF data: generate CSVs and load into Neo4j |
| [graphmana prepare-csv](graphmana-prepare-csv.md) | Generate CSV files from VCF (no Neo4j needed) |
| [graphmana load-csv](graphmana-load-csv.md) | Load pre-generated CSVs into Neo4j |
| [graphmana export](graphmana-export.md) | Export data to 17 formats (VCF, PLINK, TreeMix, SFS, ...) |
| [graphmana merge](graphmana-merge.md) | Merge a source database into the target |
| [graphmana liftover](graphmana-liftover.md) | Convert coordinates between reference genomes |

### Query and Status

| Command | Description |
|---------|-------------|
| [graphmana status](graphmana-status.md) | Show database status and node counts |
| [graphmana summary](graphmana-summary.md) | Generate a human-readable dataset report |
| [graphmana query](graphmana-query.md) | Run Cypher queries from the command line |
| [graphmana qc](graphmana-qc.md) | Quality control checks and reports |
| [graphmana ref-check](graphmana-ref-check.md) | Verify REF alleles against a FASTA reference |
| [graphmana version](graphmana-version.md) | Show detailed version information |
| [graphmana list-formats](graphmana-list-formats.md) | List all 17 export formats |
| [graphmana config-show](graphmana-config-show.md) | Display current configuration |

### Sample Management

| Command | Description |
|---------|-------------|
| [graphmana sample](graphmana-sample.md) | Manage samples (remove, restore, reassign, list) |
| [graphmana sample remove](graphmana-sample-remove.md) | Soft-delete samples |
| [graphmana sample restore](graphmana-sample-restore.md) | Restore soft-deleted samples |
| [graphmana sample reassign](graphmana-sample-reassign.md) | Move samples between populations |
| [graphmana sample hard-remove](graphmana-sample-hard-remove.md) | Permanently remove samples |
| [graphmana sample list](graphmana-sample-list.md) | List samples with status |

### Cohort Management

| Command | Description |
|---------|-------------|
| [graphmana cohort](graphmana-cohort.md) | Manage named cohort definitions |
| [graphmana cohort define](graphmana-cohort-define.md) | Define or update a cohort |
| [graphmana cohort list](graphmana-cohort-list.md) | List all cohorts |
| [graphmana cohort count](graphmana-cohort-count.md) | Count samples in a cohort |
| [graphmana cohort show](graphmana-cohort-show.md) | Show cohort details |
| [graphmana cohort delete](graphmana-cohort-delete.md) | Delete a cohort |
| [graphmana cohort validate](graphmana-cohort-validate.md) | Validate a cohort query |

### Annotation

| Command | Description |
|---------|-------------|
| [graphmana annotate](graphmana-annotate.md) | Manage annotation versions |
| [graphmana annotate load](graphmana-annotate-load.md) | Load VEP/SnpEff annotations |
| [graphmana annotate load-cadd](graphmana-annotate-load-cadd.md) | Load CADD scores |
| [graphmana annotate load-clinvar](graphmana-annotate-load-clinvar.md) | Load ClinVar annotations |
| [graphmana annotate load-constraint](graphmana-annotate-load-constraint.md) | Load gene constraint scores |
| [graphmana annotate load-go](graphmana-annotate-load-go.md) | Load GO term annotations |
| [graphmana annotate load-pathway](graphmana-annotate-load-pathway.md) | Load pathway annotations |
| [graphmana annotate load-bed](graphmana-annotate-load-bed.md) | Load BED region annotations |
| [graphmana annotate list](graphmana-annotate-list.md) | List annotation versions |
| [graphmana annotate remove](graphmana-annotate-remove.md) | Remove an annotation version |

### Database Administration

| Command | Description |
|---------|-------------|
| [graphmana db](graphmana-db.md) | Database administration commands |
| [graphmana db info](graphmana-db-info.md) | Database size, location, and status |
| [graphmana db check](graphmana-db-check.md) | Consistency check |
| [graphmana db password](graphmana-db-password.md) | Change Neo4j password |
| [graphmana db copy](graphmana-db-copy.md) | Copy database to new location |
| [graphmana db validate](graphmana-db-validate.md) | Validate packed array and chain integrity |
| [graphmana diff](graphmana-diff.md) | Compare current state against a saved summary |
| [graphmana save-state](graphmana-save-state.md) | Save database state for later comparison |
| [graphmana snapshot](graphmana-snapshot.md) | Manage database snapshots |
| [graphmana snapshot create](graphmana-snapshot-create.md) | Create a snapshot |
| [graphmana snapshot restore](graphmana-snapshot-restore.md) | Restore from snapshot |
| [graphmana snapshot list](graphmana-snapshot-list.md) | List snapshots |
| [graphmana snapshot delete](graphmana-snapshot-delete.md) | Delete a snapshot |
| [graphmana provenance](graphmana-provenance.md) | Import provenance and audit trail |
| [graphmana provenance search](graphmana-provenance-search.md) | Search logs by date or dataset |
| [graphmana migrate](graphmana-migrate.md) | Apply schema migrations |

### Installation

For detailed installation instructions (pip, conda, Docker, HPC), see the
**[Installation Guide](../INSTALL.md)**.

### Setup and Infrastructure

| Command | Description |
|---------|-------------|
| [graphmana init](graphmana-init.md) | Initialize a new project directory |
| [graphmana setup-neo4j](graphmana-setup-neo4j.md) | Download and configure Neo4j |
| [graphmana neo4j-start](graphmana-neo4j-start.md) | Start Neo4j |
| [graphmana neo4j-stop](graphmana-neo4j-stop.md) | Stop Neo4j |
| [graphmana check-filesystem](graphmana-check-filesystem.md) | Check storage suitability |
| [graphmana cluster](graphmana-cluster.md) | Cluster deployment helpers |
| [graphmana cluster generate-job](graphmana-cluster-generate-job.md) | Generate SLURM/PBS job scripts |
| [graphmana cluster check-env](graphmana-cluster-check-env.md) | Verify cluster environment |
