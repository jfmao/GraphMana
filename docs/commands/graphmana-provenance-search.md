# graphmana provenance search

Search ingestion logs by date range and/or dataset identifier.

Useful for batch tracking: "Show me everything imported in March 2026" or
"Show me all imports from dataset X".

## Usage

```
graphmana provenance search [OPTIONS]
```

## Options

```
  --neo4j-uri TEXT       Neo4j Bolt URI.
  --neo4j-user TEXT      Neo4j username.
  --neo4j-password TEXT  Neo4j password.
  --database TEXT        Neo4j database name.
  --since TEXT           Start date (ISO format, e.g. 2026-03-01).
  --until TEXT           End date (ISO format, e.g. 2026-03-31).
  --dataset-id TEXT      Filter by dataset identifier.
  --json                 Output as JSON.
```

## Examples

```bash
# All imports in March 2026
graphmana provenance search --since 2026-03-01 --until 2026-03-31

# All imports from a specific dataset
graphmana provenance search --dataset-id 1kgp_batch3

# JSON output for scripting
graphmana provenance search --since 2026-01-01 --json
```

## Example Output

```
Found 2 ingestion log(s):

  1kgp_batch3_2026-03-15T10:30:00+00:00
    Date:     2026-03-15T10:30:00+00:00
    Mode:     incremental
    Source:   batch3.vcf.gz
    Dataset:  1kgp_batch3
    Samples:  234
    Variants: 1066557

  1kgp_batch2_2026-03-01T09:15:00+00:00
    Date:     2026-03-01T09:15:00+00:00
    Mode:     incremental
    Source:   batch2.vcf.gz
    Dataset:  1kgp_batch2
    Samples:  234
    Variants: 1066557
```

## See Also

- `graphmana provenance list` — list all ingestion logs
- `graphmana provenance show` — show details of a single log
- `graphmana provenance summary` — aggregate provenance statistics
