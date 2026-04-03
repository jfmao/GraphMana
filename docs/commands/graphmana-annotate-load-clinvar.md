# graphmana annotate load-clinvar

## Synopsis

```
Usage: graphmana annotate load-clinvar [OPTIONS]
```

## Help Output

```
Usage: graphmana annotate load-clinvar [OPTIONS]

  Load ClinVar annotations from a VCF file.

Options:
  --input FILE           ClinVar VCF file.  [required]
  --version TEXT         Annotation version label (e.g. 'ClinVar_2024-01').
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
