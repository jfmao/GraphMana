# graphmana sample

## Synopsis

```
Usage: graphmana sample [OPTIONS] COMMAND [ARGS]...
```

## Help Output

```
Usage: graphmana sample [OPTIONS] COMMAND [ARGS]...

  Manage samples (remove, restore, reassign, hard-remove, list).

Options:
  --help  Show this message and exit.

Commands:
  hard-remove  Permanently remove samples by zeroing packed arrays and...
  list         List samples with status.
  reassign     Move samples to a different population, updating all...
  remove       Soft-delete samples (set excluded=true).
  restore      Restore soft-deleted samples (clear excluded flag).
```

## Subcommands

- [graphmana sample hard-remove](graphmana-sample-hard-remove.md)
- [graphmana sample list](graphmana-sample-list.md)
- [graphmana sample reassign](graphmana-sample-reassign.md)
- [graphmana sample remove](graphmana-sample-remove.md)
- [graphmana sample restore](graphmana-sample-restore.md)
