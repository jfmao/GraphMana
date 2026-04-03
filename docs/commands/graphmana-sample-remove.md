# graphmana sample remove

## Synopsis

```
Usage: graphmana sample remove [OPTIONS]
```

## Help Output

```
Usage: graphmana sample remove [OPTIONS]

  Soft-delete samples (set excluded=true).

Options:
  --sample-ids TEXT      Sample IDs to exclude (repeatable).
  --sample-list FILE     File with sample IDs (one per line).
  --reason TEXT          Exclusion reason.
  --neo4j-uri TEXT       Neo4j Bolt URI.
  --neo4j-user TEXT      Neo4j username.
  --neo4j-password TEXT  Neo4j password.
  --database TEXT        Neo4j database name.
  --help                 Show this message and exit.
```
