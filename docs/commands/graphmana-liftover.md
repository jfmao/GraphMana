# graphmana liftover

## Synopsis

```
Usage: graphmana liftover [OPTIONS]
```

## Help Output

```
Usage: graphmana liftover [OPTIONS]

  Convert variant coordinates between reference genome assemblies.

Options:
  --chain FILE              UCSC chain file for coordinate conversion (.chain
                            or .chain.gz).  [required]
  --target-reference TEXT   Target reference genome name (e.g. GRCh38).
                            [required]
  --reject-file FILE        Write unmapped/ambiguous variants to this TSV
                            file.
  --update-annotations      Attempt to liftover Gene coordinates (currently a
                            no-op).
  --backup-before           Create a database snapshot before liftover.
  --neo4j-home DIRECTORY    Neo4j installation directory (required for
                            --backup-before).
  --snapshot-dir DIRECTORY  Directory for snapshot storage.
  --neo4j-uri TEXT          Neo4j Bolt URI.
  --neo4j-user TEXT         Neo4j username.
  --neo4j-password TEXT     Neo4j password.
  --database TEXT           Neo4j database name.
  --batch-size INTEGER      Variants per database write batch.
  --dry-run                 Compute mappings without modifying the database.
  --verbose / --quiet       Verbose logging.
  --help                    Show this message and exit.
```
