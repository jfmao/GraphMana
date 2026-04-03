# graphmana cluster

## Synopsis

```
Usage: graphmana cluster [OPTIONS] COMMAND [ARGS]...
```

## Help Output

```
Usage: graphmana cluster [OPTIONS] COMMAND [ARGS]...

  Cluster deployment helpers (SLURM/PBS job scripts, environment checks).

Options:
  --help  Show this message and exit.

Commands:
  check-env     Verify cluster environment: Java, conda, Neo4j, ports,...
  generate-job  Generate a SLURM or PBS job script for a GraphMana...
```

## Subcommands

- [graphmana cluster check-env](graphmana-cluster-check-env.md)
- [graphmana cluster generate-job](graphmana-cluster-generate-job.md)
