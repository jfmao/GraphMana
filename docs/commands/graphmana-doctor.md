# graphmana doctor

## Synopsis

```
Usage: graphmana doctor [OPTIONS]
```

## Description

Run diagnostic checks on the GraphMana + Neo4j installation. Verifies Java,
Neo4j home, running process, port reachability, plugin deployment, config file
presence, data directory filesystem, and password strength. Prints a one-line
summary per check.

## Options

```
  --verbose / --quiet   Verbose output.
  --help                Show this message and exit.
```

## Output

Each check is printed as one line:

```
  [OK]   Java 21+ found: openjdk 21.0.5
  [OK]   Config file: /home/user/.graphmana/config.yaml
  [OK]   Neo4j home: /home/user/neo4j/neo4j-community-5.26.2
  [OK]   Neo4j process running (PID 12345)
  [OK]   Bolt port 7687 reachable
  [OK]   GraphMana procedures JAR deployed (31 KB)
  [OK]   Data dir: /scratch/user/db (ext4, local)
  [OK]   Password is set (non-default)
```

Possible statuses:

| Status | Meaning |
|--------|---------|
| `[OK]` | Check passed |
| `[WARN]` | Non-critical issue (e.g., Neo4j not running, default password) |
| `[FAIL]` | Critical issue (e.g., Java missing, plugin JAR absent) |

The command exits with code 1 if any check has `[FAIL]` status.

## Examples

```bash
# Basic health check
graphmana doctor

# Verbose output with additional details
graphmana doctor --verbose
```

## Notes

- Reads `~/.graphmana/config.yaml` to locate the Neo4j home directory,
  bolt port, and data directory. If no config file exists, several checks
  will show `[WARN]` instead of `[OK]`.
- Run `graphmana setup-neo4j` first to create the config file.
- On HPC clusters, `graphmana doctor` replaces the manual checklist of
  verifying Java modules, filesystem types, and port availability.

## See Also

- `graphmana setup-neo4j` — install and configure Neo4j
- `graphmana check-filesystem` — detailed filesystem check for the data directory
- `graphmana config-show` — display resolved configuration values
