# graphmana cluster generate-job

## Synopsis

```
Usage: graphmana cluster generate-job [OPTIONS]
```

## Help Output

```
Usage: graphmana cluster generate-job [OPTIONS]

  Generate a SLURM or PBS job script for a GraphMana operation.

Options:
  --scheduler [slurm|pbs]         Job scheduler type.
  --operation [prepare-csv|load-csv|ingest|export]
                                  GraphMana operation to generate a job script
                                  for.  [required]
  --input TEXT                    Input VCF file(s).
  --input-list PATH               File listing VCF paths.
  --population-map PATH           Population map file.
  --output-dir PATH               Output directory for CSVs or exports.
  --format TEXT                   Export format (for export operation).
  --output TEXT                   Export output file (for export operation).
  --reference TEXT                Reference genome. Default: GRCh38.
  --cpus INTEGER                  CPUs to request. Default: 16.
  --mem TEXT                      Memory to request. Default: 64G.
  --time TEXT                     Walltime limit. Default: 4:00:00.
  --neo4j-home TEXT               Neo4j installation directory.
  --neo4j-data-dir TEXT           Neo4j data dir.
  --neo4j-password TEXT           Neo4j password.
  --database TEXT                 Neo4j database name.
  --threads INTEGER               GraphMana threads (defaults to --cpus).
  --extra-args TEXT               Additional arguments to pass to the command.
  --output-script PATH            Write script to file instead of stdout.
  --help                          Show this message and exit.
```
