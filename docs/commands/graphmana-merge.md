# graphmana merge

## Synopsis

```
Usage: graphmana merge [OPTIONS]
```

## Help Output

```
Usage: graphmana merge [OPTIONS]

  Merge a source GraphMana database into the target database.

Options:
  --source-uri TEXT               Source database Bolt URI (e.g.
                                  bolt://localhost:7688).  [required]
  --source-user TEXT              Source Neo4j username.
  --source-password TEXT          Source Neo4j password.
  --source-database TEXT          Source Neo4j database name.
  --neo4j-uri TEXT                Target database Bolt URI.
  --neo4j-user TEXT               Target Neo4j username.
  --neo4j-password TEXT           Target Neo4j password.
  --database TEXT                 Target Neo4j database name.
  --on-duplicate-sample [error|skip]
                                  How to handle sample IDs present in both
                                  databases.
  --batch-size INTEGER            Variants per transaction batch.
  --dry-run                       Validate only, don't modify target.
  --auto-start-neo4j              Auto start/stop Neo4j around merge.
  --neo4j-home DIRECTORY          Neo4j installation directory (required with
                                  --auto-start-neo4j).
  --neo4j-data-dir DIRECTORY      Neo4j data directory (required with --auto-
                                  start-neo4j).
  --verbose / --quiet             Enable verbose logging.
  --help                          Show this message and exit.
```
