# graphmana annotate load-pathway

## Synopsis

```
Usage: graphmana annotate load-pathway [OPTIONS]
```

## Help Output

```
Usage: graphmana annotate load-pathway [OPTIONS]

  Load pathway annotations from a TSV file.

Options:
  --input FILE           Pathway TSV file (gene_symbol, pathway_id,
                         pathway_name, source).  [required]
  --version TEXT         Annotation version label.  [required]
  --source TEXT          Pathway database source.
  --batch-size INTEGER   Records per batch.
  --description TEXT     Human-readable description.
  --neo4j-uri TEXT       Neo4j Bolt URI.
  --neo4j-user TEXT      Neo4j username.
  --neo4j-password TEXT  Neo4j password.
  --database TEXT        Neo4j database name.
  --verbose / --quiet    Verbose logging.
  --help                 Show this message and exit.
```
