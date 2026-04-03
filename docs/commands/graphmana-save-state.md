# graphmana save-state

Save a lightweight summary of the current database state to a JSON file.

The saved summary includes sample counts, variant counts, population
membership, variant type distribution, annotation versions, and reference
genome. Use with `graphmana diff` to compare database states before and
after operations.

## Usage

```
graphmana save-state --output <path> [OPTIONS]
```

## Options

```
  --neo4j-uri TEXT       Neo4j Bolt URI.
  --neo4j-user TEXT      Neo4j username.
  --neo4j-password TEXT  Neo4j password.
  --database TEXT        Neo4j database name.
  --output PATH          Output .summary.json path.  [required]
```

## Example

```
$ graphmana save-state --output checkpoint_2026-03-31.summary.json
Database state saved to: checkpoint_2026-03-31.summary.json
  Variants:    70,692,015
  Samples:     3,202
  Populations: 26
```

## See Also

- `graphmana diff` — compare current state against a saved summary
- `graphmana snapshot create` — full database backup via neo4j-admin dump
