# graphmana sample reassign

## Synopsis

```
Usage: graphmana sample reassign [OPTIONS]
```

## Help Output

```
Usage: graphmana sample reassign [OPTIONS]

  Move samples to a different population, updating all variant statistics.

Options:
  --sample-ids TEXT      Sample IDs to reassign (repeatable).
  --sample-list FILE     File with sample IDs (one per line).
  --new-population TEXT  Target population ID.  [required]
  --batch-size INTEGER   Variant batch size.
  --neo4j-uri TEXT       Neo4j Bolt URI.
  --neo4j-user TEXT      Neo4j username.
  --neo4j-password TEXT  Neo4j password.
  --database TEXT        Neo4j database name.
  --verbose / --quiet    Verbose logging.
  --help                 Show this message and exit.
```
