# graphmana diff

Compare the current database state against a previously saved state summary.

Reports changes in sample counts, variant counts, population membership,
variant type distribution, annotation versions, and reference genome.
Use with `graphmana save-state` to track database evolution over time.

## Usage

```
graphmana diff --snapshot <path> [OPTIONS]
```

## Options

```
  --neo4j-uri TEXT       Neo4j Bolt URI.
  --neo4j-user TEXT      Neo4j username.
  --neo4j-password TEXT  Neo4j password.
  --database TEXT        Neo4j database name.
  --snapshot PATH        Path to a .summary.json file to compare against.
                         [required]
  --save-current PATH    Also save current state to this file.
```

## Workflow

```bash
# Before a batch operation, save the current state
graphmana save-state --output before_batch3.summary.json

# Run the operation
graphmana ingest --input batch3.vcf.gz --population-map batch3_panel.tsv ...

# Compare
graphmana diff --snapshot before_batch3.summary.json
```

## Example Output

```
--- Count Changes ---
  Samples (active):          3,202 ->      3,436  (+234)
  Variants:             70,692,015 -> 70,693,102  (+1,087)

--- Population Changes ---
  ~ AFR: 893 -> 961 samples
  ~ EUR: 633 -> 667 samples
  + SAS_NEW (34 samples)

--- Annotation Changes ---
  + VEP v110
```
