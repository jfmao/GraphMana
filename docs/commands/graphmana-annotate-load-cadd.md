# graphmana annotate load-cadd

## Synopsis

```
Usage: graphmana annotate load-cadd [OPTIONS]
```

## Help Output

```
Usage: graphmana annotate load-cadd [OPTIONS]

  Load CADD scores from a TSV file.

Options:
  --input FILE           CADD TSV file (plain or gzipped).  [required]
  --version TEXT         Annotation version label (e.g. 'CADD_v1.7').
                         [required]
  --chr-prefix TEXT      Chromosome prefix (e.g. 'chr').
  --batch-size INTEGER   Records per batch.
  --description TEXT     Human-readable description.
  --neo4j-uri TEXT       Neo4j Bolt URI.
  --neo4j-user TEXT      Neo4j username.
  --neo4j-password TEXT  Neo4j password.
  --database TEXT        Neo4j database name.
  --verbose / --quiet    Verbose logging.
  --help                 Show this message and exit.
```
