# graphmana snapshot

## Synopsis

```
Usage: graphmana snapshot [OPTIONS] COMMAND [ARGS]...
```

## Help Output

```
Usage: graphmana snapshot [OPTIONS] COMMAND [ARGS]...

  Manage database snapshots (create, list, restore, delete).

Options:
  --help  Show this message and exit.

Commands:
  create   Create a database snapshot via neo4j-admin dump.
  delete   Delete a snapshot.
  list     List all snapshots.
  restore  Restore a database from a snapshot.
```

## Subcommands

- [graphmana snapshot create](graphmana-snapshot-create.md)
- [graphmana snapshot delete](graphmana-snapshot-delete.md)
- [graphmana snapshot list](graphmana-snapshot-list.md)
- [graphmana snapshot restore](graphmana-snapshot-restore.md)
