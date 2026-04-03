# graphmana annotate load-constraint

## Synopsis

```
Usage: graphmana annotate load-constraint [OPTIONS]
```

## Help Output

```
Usage: graphmana annotate load-constraint [OPTIONS]

  Load gene constraint scores (pLI, LOEUF, mis_z, syn_z).

Options:
  --input FILE           gnomAD gene constraint TSV file.  [required]
  --version TEXT         Annotation version label (e.g. 'gnomAD_v4.1').
                         [required]
  --batch-size INTEGER   Records per batch.
  --description TEXT     Human-readable description.
  --neo4j-uri TEXT       Neo4j Bolt URI.
  --neo4j-user TEXT      Neo4j username.
  --neo4j-password TEXT  Neo4j password.
  --database TEXT        Neo4j database name.
  --verbose / --quiet    Verbose logging.
  --help                 Show this message and exit.
```
