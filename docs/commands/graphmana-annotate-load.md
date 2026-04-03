# graphmana annotate load

## Synopsis

```
Usage: graphmana annotate load [OPTIONS]
```

## Help Output

```
Usage: graphmana annotate load [OPTIONS]

  Load annotations from a VEP/SnpEff VCF.

Options:
  --input FILE                 VEP/SnpEff annotated VCF file.  [required]
  --version TEXT               Annotation version label (e.g. 'VEP_v110').
                               [required]
  --mode [add|update|replace]  Load mode: add (layer), update (merge), replace
                               (clean swap).
  --type [auto|vep|snpeff]     Annotation type (auto-detected from VCF header
                               by default).
  --description TEXT           Human-readable description.
  --batch-size INTEGER         Edges per Cypher batch.
  --neo4j-uri TEXT             Neo4j Bolt URI.
  --neo4j-user TEXT            Neo4j username.
  --neo4j-password TEXT        Neo4j password.
  --database TEXT              Neo4j database name.
  --verbose / --quiet          Verbose logging.
  --help                       Show this message and exit.
```
