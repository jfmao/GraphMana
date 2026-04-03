# graphmana query

## Synopsis

```
Usage: graphmana query [OPTIONS] [CYPHER]
```

## Help Output

```
Usage: graphmana query [OPTIONS] [CYPHER]

  Run a Cypher query against the database.

  Pass the query as an argument or use --file to read from a file.

  Examples:
    graphmana query "MATCH (v:Variant) RETURN count(v) AS n"
    graphmana query "MATCH (p:Population) RETURN p.populationId, p.n_samples" --format csv
    graphmana query --file my_query.cypher --format json

Options:
  --file PATH                Read query from file.
  --neo4j-uri TEXT
  --neo4j-user TEXT
  --neo4j-password TEXT
  --database TEXT
  --format [table|json|csv]
  --limit INTEGER            Max rows to display. Default: 100.
  --help                     Show this message and exit.
```
