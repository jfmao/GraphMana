# graphmana annotate load-go

## Synopsis

```
Usage: graphmana annotate load-go [OPTIONS]
```

## Help Output

```
Usage: graphmana annotate load-go [OPTIONS]

  Load GO term annotations from a GAF file.

Options:
  --input FILE           GO annotation file (GAF format).  [required]
  --version TEXT         Annotation version label.  [required]
  --obo-file FILE        OBO ontology file for GO term hierarchy (optional).
  --batch-size INTEGER   Records per batch.
  --description TEXT     Human-readable description.
  --neo4j-uri TEXT       Neo4j Bolt URI.
  --neo4j-user TEXT      Neo4j username.
  --neo4j-password TEXT  Neo4j password.
  --database TEXT        Neo4j database name.
  --verbose / --quiet    Verbose logging.
  --help                 Show this message and exit.
```
