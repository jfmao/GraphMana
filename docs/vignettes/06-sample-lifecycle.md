# Vignette 06: Sample Management

This vignette walks through the full sample lifecycle in GraphMana: adding samples incrementally, soft-deleting and restoring them, reassigning populations, and permanently removing samples when needed.

## Prerequisites

- A running GraphMana database with an initial dataset already imported
- The `graphmana` CLI installed and configured

We assume a database loaded with 1000 Genomes chr22 data (3,202 samples across 26 populations). If you are starting from scratch, see the main [tutorial](../tutorial.md) for import instructions.

## Listing Samples

Start by inspecting what is already in the database:

```bash
graphmana sample list
```

Expected output:

```
Samples: 3202 active, 0 excluded, 3202 total

  HG00096               GBR
  HG00097               GBR
  HG00099               GBR
  HG00100               GBR
  ...
  NA21144               GIH
```

Filter by population:

```bash
graphmana sample list --population YRI
```

Expected output:

```
Samples: 3202 active, 0 excluded, 3202 total

  NA18486               YRI
  NA18487               YRI
  NA18488               YRI
  ...
  NA19257               YRI
```

## Adding Samples Incrementally

GraphMana supports adding new samples to an existing database without re-importing everything. This is the `--mode incremental` flag on the `ingest` command.

Suppose you have a new VCF with 50 additional samples sequenced on chr22:

```bash
graphmana ingest \
    --input new_samples_chr22.vcf.gz \
    --population-map new_samples_pops.tsv \
    --mode incremental \
    --reference GRCh38 \
    --verbose
```

Expected output:

```
Mode: incremental (appending to existing database)
Parsing VCF: new_samples_chr22.vcf.gz
  50 samples, 1,072,533 variant sites
Extending packed arrays for existing variants...
  1,068,211 variants extended (appended 50 samples)
  4,322 new variants created (homref-padded for existing 3,202 samples)
Updating population statistics...
Updating SchemaMetadata...
Done. Database now contains 3,252 samples, 1,076,855 variants.
```

What happens during incremental addition:

1. **Existing variants**: The gt_packed and phase_packed byte arrays are extended by appending the new samples' genotypes. Existing packed_index values never change.
2. **New variants**: Variants present in the new VCF but not in the database are created with HomRef padding for all previous samples.
3. **Population arrays**: The ac[], an[], af[] arrays are recomputed to include the new samples.
4. **Provenance**: An IngestionLog node is recorded for the incremental import.

Verify the result:

```bash
graphmana status
```

Expected output:

```
GraphMana v0.9.0
Connected to: bolt://localhost:7687

Node counts:
  Variant           1,076,855
  Sample                3,252
  Population               27
  Chromosome                1
  Gene                      0
  VCFHeader                 2
```

## Soft-Deleting Samples

Soft-delete is the default removal method. It sets `excluded=true` on the Sample node, which causes the sample to be masked in all exports and queries. It is instant and reversible.

**When to use soft-delete:**

- Removing QC failures before export
- Temporarily excluding samples for a specific analysis
- Marking duplicates or contaminated samples
- Any situation where you might want the sample back later

Remove specific samples by ID:

```bash
graphmana sample remove \
    --sample-ids HG00096 --sample-ids HG00097 \
    --reason "QC failure: low call rate"
```

Expected output:

```
Excluded 2 sample(s).
```

Remove samples from a file (one ID per line):

```bash
graphmana sample remove \
    --sample-list failed_qc_samples.txt \
    --reason "Batch QC failure"
```

Expected output:

```
Excluded 15 sample(s).
```

Verify the exclusions:

```bash
graphmana sample list --show-excluded
```

Expected output:

```
Samples: 3235 active, 17 excluded, 3252 total

  HG00096               GBR             [EXCLUDED] (QC failure: low call rate)
  HG00097               GBR             [EXCLUDED] (QC failure: low call rate)
  HG00099               GBR
  ...
```

**What soft-delete affects:**

- All exports skip excluded samples (VCF, PLINK, EIGENSTRAT, etc.)
- Population statistics (ac[], an[], af[]) are NOT automatically recomputed. Use `--recalculate-af` on export if you need frequencies reflecting only active samples.
- The packed arrays (gt_packed, phase_packed) are unchanged -- the sample's data is still in the byte arrays, just masked during unpacking.
- Cypher queries via `graphmana query` still return excluded samples unless you add a `WHERE s.excluded IS NULL OR s.excluded = false` filter.

## Restoring Soft-Deleted Samples

Restore is the inverse of soft-delete. It clears the `excluded` flag and the `exclusion_reason`:

```bash
graphmana sample restore \
    --sample-ids HG00096 --sample-ids HG00097
```

Expected output:

```
Restored 2 sample(s).
```

Restore from a file:

```bash
graphmana sample restore --sample-list failed_qc_samples.txt
```

Expected output:

```
Restored 15 sample(s).
```

Verify:

```bash
graphmana sample list --show-excluded
```

Expected output:

```
Samples: 3252 active, 0 excluded, 3252 total

  HG00096               GBR
  HG00097               GBR
  ...
```

## Reassigning Populations

When sample metadata changes -- for example, a sample was initially assigned to the wrong population, or you want to reclassify samples into broader superpopulation groups -- use `sample reassign`:

```bash
graphmana sample reassign \
    --sample-ids NA18486 --sample-ids NA18487 \
    --new-population YRI_subset \
    --verbose
```

Expected output:

```
Reassigned 2 sample(s) to YRI_subset. Updated 1,072,533 variants.
```

Reassign from a file:

```bash
graphmana sample reassign \
    --sample-list reclassify_samples.txt \
    --new-population EUR_combined \
    --verbose
```

Expected output:

```
Reassigned 120 sample(s) to EUR_combined. Updated 1,076,855 variants.
```

What happens during reassignment:

1. The sample's `IN_POPULATION` relationship is moved to the new Population node (created if it does not exist).
2. The pop_ids[], ac[], an[], af[] arrays on all Variant nodes are recomputed to reflect the new population membership.
3. Population node counters (n_samples) are updated for both the old and new populations.

This operation is proportional to the number of variants and can take minutes for whole-genome data. Use `--batch-size` to control memory usage.

## Hard-Deleting Samples

Hard-delete permanently removes samples from the database by zeroing their positions in all packed arrays and deleting the Sample nodes. This operation is irreversible and slow -- it must rewrite every Variant node's gt_packed and phase_packed arrays.

**When to use hard-delete:**

- Regulatory requirement to purge all traces of a sample (GDPR, consent withdrawal)
- Reclaiming storage after removing a large number of samples
- Cleaning up after a known data corruption

**When NOT to use hard-delete:**

- Routine QC exclusions (use soft-delete instead)
- Temporary removal for analysis (use soft-delete)
- Any situation where you might want the data back

Hard-delete requires confirmation:

```bash
graphmana sample hard-remove \
    --sample-ids HG00096 --sample-ids HG00097
```

Expected output:

```
This will permanently remove 2 sample(s) and update all variants.
This cannot be undone. Continue? [y/N]: y
Hard-removed 2 sample(s). Updated 1,076,855 variants.
```

Skip the confirmation prompt with `--yes`:

```bash
graphmana sample hard-remove \
    --sample-list purge_list.txt \
    --yes --verbose
```

As a safety measure, you can require that samples be soft-deleted first:

```bash
# First soft-delete
graphmana sample remove --sample-ids HG00099 --reason "Consent withdrawn"

# Then hard-delete only pre-excluded samples
graphmana sample hard-remove \
    --sample-ids HG00099 \
    --require-soft-deleted \
    --yes
```

Expected output:

```
Hard-removed 1 sample(s). Updated 1,076,855 variants.
```

If `--require-soft-deleted` is set and a sample is not already excluded, the command will refuse to proceed.

## Soft-Delete vs Hard-Delete: Summary

| Property | Soft-Delete | Hard-Delete |
|----------|-------------|-------------|
| Speed | Instant | Minutes to hours (rewrites all arrays) |
| Reversible | Yes (`sample restore`) | No |
| Storage reclaimed | None | Yes (arrays shrink) |
| Packed arrays | Unchanged | Zeroed and recomputed |
| Population stats | Unchanged (use `--recalculate-af`) | Recomputed |
| Use case | QC, temporary exclusion | GDPR, consent withdrawal, storage |

## Best Practices

1. **Snapshot before hard-delete.** Always create a snapshot so you can recover if something goes wrong:

   ```bash
   graphmana snapshot create --name pre_purge --neo4j-home /var/lib/neo4j
   graphmana sample hard-remove --sample-list purge.txt --yes
   ```

2. **Soft-delete first, hard-delete later.** Use `--require-soft-deleted` as a two-step safety gate.

3. **Run QC after changes.** After adding, removing, or reassigning samples, generate a fresh QC report:

   ```bash
   graphmana qc --type all --output post_changes_qc.html --format html
   ```

4. **Check provenance.** Every incremental import creates a provenance record:

   ```bash
   graphmana provenance list
   ```

## See Also

- [Tutorial](../tutorial.md) -- Initial import and basic operations
- [Vignette 10: Database Administration](10-database-admin.md) -- Snapshots, QC, provenance
