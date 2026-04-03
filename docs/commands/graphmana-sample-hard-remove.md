# graphmana sample hard-remove

## Synopsis

```
Usage: graphmana sample hard-remove [OPTIONS]
```

## Help Output

```
Usage: graphmana sample hard-remove [OPTIONS]

  Permanently remove samples by zeroing packed arrays and deleting nodes.

Options:
  --sample-ids TEXT       Sample IDs to permanently remove (repeatable).
  --sample-list FILE      File with sample IDs (one per line).
  --require-soft-deleted  Only remove samples already soft-deleted.
  --batch-size INTEGER    Variant batch size.
  --neo4j-uri TEXT        Neo4j Bolt URI.
  --neo4j-user TEXT       Neo4j username.
  --neo4j-password TEXT   Neo4j password.
  --database TEXT         Neo4j database name.
  --yes                   Skip confirmation prompt.
  --verbose / --quiet     Verbose logging.
  --help                  Show this message and exit.
```
