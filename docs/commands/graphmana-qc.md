# graphmana qc

## Synopsis

```
Usage: graphmana qc [OPTIONS]
```

## Help Output

```
Usage: graphmana qc [OPTIONS]

  Run quality control checks and generate a report.

Options:
  --type [sample|variant|batch|all]
                                  QC type to run.
  --output PATH                   Output file path.  [required]
  --format [tsv|json|html]        Output format.
  --neo4j-uri TEXT                Neo4j Bolt URI.
  --neo4j-user TEXT               Neo4j username.
  --neo4j-password TEXT           Neo4j password.
  --database TEXT                 Neo4j database name.
  --verbose / --quiet             Verbose logging.
  --help                          Show this message and exit.
```
