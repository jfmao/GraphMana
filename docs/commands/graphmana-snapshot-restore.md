# graphmana snapshot restore

## Synopsis

```
Usage: graphmana snapshot restore [OPTIONS]
```

## Help Output

```
Usage: graphmana snapshot restore [OPTIONS]

  Restore a database from a snapshot. Neo4j must be stopped.

Options:
  --name TEXT               Snapshot name to restore.  [required]
  --neo4j-home DIRECTORY    Neo4j installation directory.  [required]
  --database TEXT           Neo4j database name.
  --snapshot-dir DIRECTORY  Directory containing snapshots.
  --help                    Show this message and exit.
```
