# graphmana cohort define

## Synopsis

```
Usage: graphmana cohort define [OPTIONS]
```

## Help Output

```
Usage: graphmana cohort define [OPTIONS]

  Define or update a named cohort.

Options:
  --name TEXT            Cohort name (unique).  [required]
  --query TEXT           Cypher query returning sampleId.  [required]
  --description TEXT     Human-readable description.
  --neo4j-uri TEXT       Neo4j Bolt URI.
  --neo4j-user TEXT      Neo4j username.
  --neo4j-password TEXT  Neo4j password.
  --database TEXT        Neo4j database name.
  --help                 Show this message and exit.
```
