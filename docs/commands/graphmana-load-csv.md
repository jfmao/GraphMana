# graphmana load-csv

## Synopsis

```
Usage: graphmana load-csv [OPTIONS]
```

## Help Output

```
Usage: graphmana load-csv [OPTIONS]

  Load pre-generated CSVs into Neo4j.

Options:
  --csv-dir DIRECTORY         Directory containing CSV files from prepare-csv.
                              [required]
  --neo4j-home DIRECTORY      Neo4j installation directory.  [required]
  --neo4j-data-dir DIRECTORY  Neo4j data directory.
  --database TEXT             Neo4j database name.
  --auto-start-neo4j          Auto start/stop Neo4j.
  --neo4j-uri TEXT            Neo4j Bolt URI (for post-import indexes).
  --neo4j-user TEXT           Neo4j username.
  --neo4j-password TEXT       Neo4j password.
  --verbose / --quiet         Verbose logging.
  --help                      Show this message and exit.
```
