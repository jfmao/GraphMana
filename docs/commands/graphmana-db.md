# graphmana db

## Synopsis

```
Usage: graphmana db [OPTIONS] COMMAND [ARGS]...
```

## Help Output

```
Usage: graphmana db [OPTIONS] COMMAND [ARGS]...

  Database administration (info, check, password, compact, copy).

Options:
  --help  Show this message and exit.

Commands:
  check     Run Neo4j consistency check on the database.
  copy      Copy a database to a new location via neo4j-admin dump/load.
  info      Show database size, location, Neo4j version, and connection...
  password  Change the Neo4j password.
```

## Subcommands

- [graphmana db check](graphmana-db-check.md)
- [graphmana db copy](graphmana-db-copy.md)
- [graphmana db info](graphmana-db-info.md)
- [graphmana db password](graphmana-db-password.md)
