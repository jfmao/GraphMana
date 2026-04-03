# Vignette 10: Database Administration

This vignette covers the operational side of running a GraphMana database: snapshots, consistency checks, schema migration, provenance auditing, QC reports, database merging, and ad-hoc Cypher queries.

## Prerequisites

- A running GraphMana database with imported data
- The `graphmana` CLI installed
- For snapshot and consistency check operations: access to the Neo4j installation directory

## Database Information

Get a quick overview of the database:

```bash
graphmana status
```

Expected output:

```
GraphMana v0.9.0
Connected to: bolt://localhost:7687

Node counts:
  Variant           1,072,533
  Sample                3,202
  Population               26
  Chromosome                1
  Gene                      0
  VCFHeader                 1

Schema version:   2
Reference genome: GRCh38
```

For more detail including relationship counts:

```bash
graphmana status --detailed
```

Expected output:

```
GraphMana v0.9.0
Connected to: bolt://localhost:7687

Node counts:
  Variant           1,072,533
  Sample                3,202
  Population               26
  Chromosome                1
  Gene                      0
  VCFHeader                 1

Schema version:   2
Reference genome: GRCh38

Total nodes:      1,075,763
Relationships:    2,148,270
```

For disk usage and Neo4j version, pass `--neo4j-home`:

```bash
graphmana db info --neo4j-home /var/lib/neo4j
```

## Snapshots

Snapshots use `neo4j-admin dump` to create a full backup of the database. Neo4j must be stopped for snapshot operations.

### Create a Snapshot

```bash
graphmana snapshot create \
    --name before_liftover \
    --neo4j-home /var/lib/neo4j
```

Expected output:

```
Snapshot created: before_liftover (2,451.3 MB)
Path: /home/user/.graphmana/snapshots/before_liftover.dump
```

### List Snapshots

```bash
graphmana snapshot list
```

Expected output:

```
  before_liftover                 2,451.3 MB  2026-03-25 14:30:22
  initial_import                  2,448.1 MB  2026-03-20 09:15:44
  pre_annotation_update             2,460.7 MB  2026-03-22 11:05:33
```

### Restore a Snapshot

Neo4j must be stopped before restoring:

```bash
graphmana neo4j-stop --neo4j-home /var/lib/neo4j

graphmana snapshot restore \
    --name before_liftover \
    --neo4j-home /var/lib/neo4j

graphmana neo4j-start --neo4j-home /var/lib/neo4j --wait
```

Expected output:

```
Snapshot restored: before_liftover -> database neo4j
```

### Delete a Snapshot

```bash
graphmana snapshot delete --name initial_import
```

By default, snapshots are stored in `~/.graphmana/snapshots/`. Override with `--snapshot-dir`.

## Consistency Check

Run the Neo4j built-in consistency checker to detect index corruption. Neo4j must be stopped:

```bash
graphmana neo4j-stop --neo4j-home /var/lib/neo4j
graphmana db check --neo4j-home /var/lib/neo4j
```

If the check fails, restore from a known-good snapshot.

## Schema Migration

When upgrading GraphMana to a new version, the database schema may need updating:

```bash
# Check what migrations are pending (dry run)
graphmana migrate --dry-run
```

Expected output:

```
Current schema version: 2
Target schema version:  3
Pending migrations:
  v2 -> v3: Add population_specificity index, update SchemaMetadata fields

DRY RUN -- no changes applied.
```

Apply the migration:

```bash
graphmana migrate --verbose
```

Expected output:

```
Applying migration v2 -> v3...
  Creating index on Variant.population_specificity...
  Updating SchemaMetadata...
Migration complete. Schema version: 3
```

Best practice: always snapshot before migrating.

```bash
graphmana snapshot create --name pre_migration_v3 --neo4j-home /var/lib/neo4j
graphmana migrate
```

## Provenance Auditing

Every import, merge, and liftover operation creates a provenance record (IngestionLog node). This provides a complete audit trail of how the database was built.

### Summary

```bash
graphmana provenance summary
```

Expected output:

```
  Total ingestions:   3
  Total samples:      3252
  Total variants:     1076855
  First import:       2026-03-20T09:15:44
  Last import:        2026-03-25T14:30:22
  Source files:        2
    - chr22_1kg.vcf.gz
    - new_samples_chr22.vcf.gz
```

### List All Ingestion Logs

```bash
graphmana provenance list
```

Expected output:

```
  ingest_001  initial       3202 samples  1072533 variants  2026-03-20T09:15:44
  ingest_002  incremental     50 samples     4322 variants  2026-03-25T14:30:22
  ingest_003  liftover         0 samples        0 variants  2026-03-25T15:00:11
```

### Show Details of a Specific Ingestion

```bash
graphmana provenance show ingest_001
```

Expected output:

```
  log_id: ingest_001
  source_file: chr22_1kg.vcf.gz
  dataset_id: 1kg_chr22
  mode: initial
  import_date: 2026-03-20T09:15:44
  n_samples: 3202
  n_variants: 1072533
  filters_applied: None
  fidelity: default
  reference_genome: GRCh38
```

### List VCF Headers

```bash
graphmana provenance headers
```

Expected output:

```
  1kg_chr22          chr22_1kg.vcf.gz           2026-03-20T09:15:44
  new_batch          new_samples_chr22.vcf.gz   2026-03-25T14:30:22
```

### JSON Output

All provenance commands support `--json` for programmatic access:

```bash
graphmana provenance summary --json | python -m json.tool
```

## QC Reports

GraphMana generates quality control reports covering sample-level, variant-level, and population-level statistics.

### Full QC Report (HTML)

```bash
graphmana qc --type all --output qc_report.html --format html
```

Expected output:

```
QC report written to qc_report.html (1,072,533 variants, 3,202 samples, 26 populations)
```

Open `qc_report.html` in a browser to see interactive tables and summary statistics.

### Variant-Only QC (TSV)

```bash
graphmana qc --type variant --output variant_qc.tsv --format tsv
```

The TSV output is suitable for downstream processing with R, Python, or spreadsheet tools.

### Sample-Only QC (JSON)

```bash
graphmana qc --type sample --output sample_qc.json --format json
```

JSON format is useful for programmatic integration.

QC types: `sample` (heterozygosity, call rate, rare variant burden), `variant` (call rate, allele frequency spectrum, HWE), `batch` (per-population summaries, batch effects).

## Database Merge

Merge two GraphMana databases into one. This is useful for combining independently imported datasets.

### Dry Run

```bash
graphmana merge \
    --source-uri bolt://localhost:7688 \
    --dry-run
```

Expected output:

```
Dry run complete -- no modifications made.
  Variants extended:            985,412
  Variants homref-extended:      87,121
  Variants created:              15,233
  Samples merged:                   500
  Populations created:                3
  Chromosomes processed:              1
```

### Execute Merge

```bash
graphmana merge \
    --source-uri bolt://localhost:7688 \
    --on-duplicate-sample skip \
    --verbose
```

Expected output:

```
Merge complete.
  Variants extended:            985,412
  Variants homref-extended:      87,121
  Variants created:              15,233
  Samples merged:                   500
  Populations created:                3
  Chromosomes processed:              1
```

The `--on-duplicate-sample` flag controls behavior when a sample ID exists in both databases:
- `error` (default): Abort the merge
- `skip`: Skip duplicate samples in the source

## Ad-Hoc Cypher Queries

The `graphmana query` command runs read-only Cypher queries against the database. Write operations are blocked for safety. Output formats: `--format table` (default), `--format csv`, `--format json`.

```bash
# Count variants per chromosome
graphmana query "MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome) RETURN c.chromosomeId AS chr, count(v) AS n ORDER BY chr"

# Export population counts as CSV
graphmana query \
    "MATCH (p:Population) RETURN p.populationId AS pop, p.n_samples AS n ORDER BY n DESC" \
    --format csv > populations.csv

# Read query from a file
graphmana query --file my_query.cypher --format json
```

## Best Practices

1. **Snapshot before destructive operations.** Always snapshot before liftover, hard-delete, merge, or schema migration:

   ```bash
   graphmana snapshot create --name before_<operation> --neo4j-home /var/lib/neo4j
   ```

2. **Check provenance after imports.** Verify that every import was recorded correctly:

   ```bash
   graphmana provenance summary
   ```

3. **Run QC after major changes.** After imports, merges, liftover, or sample changes:

   ```bash
   graphmana qc --type all --output post_change_qc.html --format html
   ```

4. **Use `--dry-run` first.** Merge, liftover, and migrate all support `--dry-run` to preview changes without committing them.

5. **Periodic consistency checks.** Run `graphmana db check` after unexpected shutdowns or disk events.

6. **Keep snapshots on separate storage.** Use `--snapshot-dir` to write backups to a different disk than the Neo4j data directory.

7. **Use `graphmana query` for exploration, not modification.** The query command blocks write operations. Use Neo4j Browser or Cypher Shell for administrative writes.

## Version and Configuration

Check the installed versions of GraphMana and all dependencies:

```bash
graphmana version
```

View the current configuration (connection settings, defaults, environment
variables):

```bash
graphmana config-show
```

This is especially useful when debugging connection issues on a new machine or
cluster node -- it shows which environment variables are active and what
defaults are in effect.

## Database Validation

Run a quick integrity check without stopping Neo4j:

```bash
graphmana db validate
```

This verifies that packed genotype array lengths match the active sample count,
that population statistic arrays are consistent, and that NEXT chains are
complete. It is much faster than `graphmana db check` (which runs the full
neo4j-admin consistency check and requires stopping Neo4j).

## State Tracking with Diff

Before a major operation (adding a batch, running liftover, loading new
annotations), save the current database state:

```bash
graphmana save-state --output before_batch3.summary.json
```

After the operation, compare:

```bash
graphmana diff --snapshot before_batch3.summary.json
```

The diff reports changes in sample counts, variant counts, populations,
variant type distribution, annotation versions, and reference genome. This is
invaluable for verifying that a batch import added the expected number of
samples, or that an annotation update didn't accidentally change variant counts.

## Provenance Search

Search ingestion logs by date range or dataset identifier:

```bash
# All imports in March 2026
graphmana provenance search --since 2026-03-01 --until 2026-03-31

# All imports from a specific dataset
graphmana provenance search --dataset-id 1kgp_batch3

# Machine-readable output
graphmana provenance search --since 2026-01-01 --json
```

This complements `graphmana provenance list` (which shows all logs) and
`graphmana provenance summary` (which shows aggregate statistics).

## Reference Allele Verification

After importing data or performing a coordinate liftover, verify that stored
REF alleles match the reference genome:

```bash
graphmana ref-check --fasta GRCh38.fa --output mismatches.tsv

# Quick spot-check on one chromosome
graphmana ref-check --fasta GRCh38.fa --chromosomes chr22 --max-mismatches 10
```

For best performance, ensure the FASTA has a `.fai` index (created by
`samtools faidx`).

## See Also

- [Tutorial](../tutorial.md) -- Getting started
- [Vignette 06: Sample Management](06-sample-lifecycle.md) -- Adding, removing, restoring samples
- [Vignette 07: Liftover](07-liftover.md) -- Reference genome conversion
- [Vignette 08: HPC Cluster Deployment](08-cluster-hpc.md) -- Running on SLURM clusters
- [Vignette 09: Jupyter API](09-jupyter-api.md) -- Interactive Python analysis
