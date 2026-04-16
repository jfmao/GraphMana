# graphmana version

Show detailed version information for GraphMana and its dependencies.

Unlike `graphmana --version` (which prints only the package version), this
command reports versions of all key dependencies: Python, cyvcf2, NumPy,
Java, Neo4j, and bcftools.

## Usage

```
graphmana version
```

## Options

No options. This command does not connect to Neo4j.

## Example

```
$ graphmana version
GraphMana:      1.1.0
Schema version: 1.1
Python:         3.12.4
cyvcf2:         0.32.1
NumPy:          1.26.4
Java:           openjdk version "21.0.3" 2024-04-16
Neo4j:          5.26.0
bcftools:       bcftools 1.17
```
