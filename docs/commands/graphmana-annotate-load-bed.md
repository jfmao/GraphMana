# graphmana annotate load-bed

## Synopsis

```
Usage: graphmana annotate load-bed [OPTIONS]
```

## Help Output

```
Usage: graphmana annotate load-bed [OPTIONS]

  Load BED regions and link overlapping variants.

Options:
  --input FILE           BED file with genomic regions.  [required]
  --version TEXT         Annotation version label.  [required]
  --region-type TEXT     Type label for regions (e.g. 'enhancer', 'promoter',
                         'regulatory').
  --batch-size INTEGER   Regions per batch.
  --description TEXT     Human-readable description.
  --neo4j-uri TEXT       Neo4j Bolt URI.
  --neo4j-user TEXT      Neo4j username.
  --neo4j-password TEXT  Neo4j password.
  --database TEXT        Neo4j database name.
  --verbose / --quiet    Verbose logging.
  --help                 Show this message and exit.
```
