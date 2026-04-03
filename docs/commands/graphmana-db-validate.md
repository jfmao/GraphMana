# graphmana db validate

Validate database integrity by checking packed array sizes, population array
consistency, and NEXT chain continuity.

This is a lightweight alternative to `graphmana db check` (which runs the
full neo4j-admin consistency check). It verifies GraphMana-specific data
structures without stopping Neo4j.

## Usage

```
graphmana db validate [OPTIONS]
```

## Options

```
  --neo4j-uri TEXT       Neo4j Bolt URI.
  --neo4j-user TEXT      Neo4j username.
  --neo4j-password TEXT  Neo4j password.
  --database TEXT        Neo4j database name.
  --fix                  Attempt to fix detected issues.
```

## Checks Performed

1. **Packed array lengths**: Verifies that `gt_packed` and `phase_packed` byte
   array sizes match the active sample count.
2. **Population array consistency**: Verifies that `pop_ids`, `ac`, `an`, and
   `af` arrays have matching lengths on every Variant node.
3. **NEXT chain continuity**: Spot-checks that NEXT relationship chains are
   complete (n_edges = n_variants - 1 per chromosome).

## Example

```
$ graphmana db validate
Validating database integrity...
  Active samples: 3,202
  gt_packed lengths: OK (801 bytes)
  phase_packed lengths: OK (401 bytes)
  Population array lengths: OK
  NEXT chains: OK (checked 5 chromosomes)

Database validation passed.
```
