# graphmana config-show

Display current configuration defaults and environment variable overrides.

Shows the active Neo4j connection settings, processing defaults, version
information, and which environment variables are set. Useful for debugging
connection issues or verifying that environment variables are picked up.

## Usage

```
graphmana config-show
```

## Options

No options. This command does not connect to Neo4j.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GRAPHMANA_NEO4J_PASSWORD` | Neo4j password | `graphmana` |
| `GRAPHMANA_NEO4J_URI` | Neo4j Bolt URI | `bolt://localhost:7687` |
| `NEO4J_HOME` | Neo4j installation directory | (not set) |

## Example

```
$ graphmana config-show
GraphMana Configuration
=======================

Connection:
  Neo4j URI:      bolt://localhost:7687
  Neo4j user:     neo4j
  Neo4j password: (default)
  Database:       neo4j

Processing:
  Batch size:     100,000
  Threads:        1

Version:
  GraphMana:      1.0.0-dev
  Schema:         0.1.0

Environment variables:
  GRAPHMANA_NEO4J_PASSWORD         (not set)
  GRAPHMANA_NEO4J_URI              (not set)
  NEO4J_HOME                       (not set)
```
