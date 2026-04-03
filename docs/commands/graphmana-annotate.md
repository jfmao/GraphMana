# graphmana annotate

## Synopsis

```
Usage: graphmana annotate [OPTIONS] COMMAND [ARGS]...
```

## Help Output

```
Usage: graphmana annotate [OPTIONS] COMMAND [ARGS]...

  Manage annotation versions (load, list, remove).

Options:
  --help  Show this message and exit.

Commands:
  list             List all annotation versions.
  load             Load annotations from a VEP/SnpEff VCF.
  load-bed         Load BED regions and link overlapping variants.
  load-cadd        Load CADD scores from a TSV file.
  load-clinvar     Load ClinVar annotations from a VCF file.
  load-constraint  Load gene constraint scores (pLI, LOEUF, mis_z, syn_z).
  load-go          Load GO term annotations from a GAF file.
  load-pathway     Load pathway annotations from a TSV file.
  remove           Remove an annotation version and its edges.
```

## Subcommands

- [graphmana annotate list](graphmana-annotate-list.md)
- [graphmana annotate load](graphmana-annotate-load.md)
- [graphmana annotate load-bed](graphmana-annotate-load-bed.md)
- [graphmana annotate load-cadd](graphmana-annotate-load-cadd.md)
- [graphmana annotate load-clinvar](graphmana-annotate-load-clinvar.md)
- [graphmana annotate load-constraint](graphmana-annotate-load-constraint.md)
- [graphmana annotate load-go](graphmana-annotate-load-go.md)
- [graphmana annotate load-pathway](graphmana-annotate-load-pathway.md)
- [graphmana annotate remove](graphmana-annotate-remove.md)
