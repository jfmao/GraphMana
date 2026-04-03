# Vignette 07: Reference Genome Liftover

This vignette demonstrates how to convert variant coordinates between reference genome assemblies using GraphMana's liftover command. The most common use case is converting from GRCh37 (hg19) to GRCh38 (hg38), but any pair of assemblies with a UCSC chain file is supported.

## Prerequisites

- A running GraphMana database with variants imported (any reference genome)
- The `graphmana` CLI installed
- A UCSC chain file for your desired conversion

## Understanding Liftover

Reference genome liftover changes the coordinate system of your variants without altering the actual genotype data. This is essential when:

- Combining datasets generated on different reference builds
- Preparing data for tools that require a specific assembly
- Updating a legacy GRCh37 database to GRCh38

## Step 1: Download a Chain File

UCSC provides chain files for all common assembly conversions:

```bash
# GRCh37 (hg19) to GRCh38 (hg38)
wget https://hgdownload.cse.ucsc.edu/goldenpath/hg19/liftOver/hg19ToHg38.over.chain.gz

# GRCh38 (hg38) to GRCh37 (hg19) -- reverse direction
wget https://hgdownload.cse.ucsc.edu/goldenpath/hg38/liftOver/hg38ToHg19.over.chain.gz
```

Chain files are typically 1-10 MB compressed. No decompression is needed -- GraphMana reads `.chain.gz` directly.

## Step 2: Dry Run

Before modifying the database, run a dry run to see how many variants will be mapped, unmapped, or ambiguous:

```bash
graphmana liftover \
    --chain hg19ToHg38.over.chain.gz \
    --target-reference GRCh38 \
    --dry-run \
    --verbose
```

Expected output:

```
Loading chain file: hg19ToHg38.over.chain.gz
  Loaded 1,264 chain records
Fetching variants from database...
  1,072,533 variants on 1 chromosome(s)
Computing coordinate mappings...

DRY RUN -- no database changes made.
Target reference: GRCh38
Total variants:   1,072,533
Mapped:           1,070,891
Unmapped:             1,412
Ambiguous:              198
Collisions:              32
```

Understanding the categories:

- **Mapped**: Variants with a clean one-to-one coordinate conversion. These will be updated.
- **Unmapped**: Variants in regions that have no alignment between assemblies (e.g., centromeric regions, assembly gaps). These are flagged but not deleted.
- **Ambiguous**: Variants that map to multiple locations in the target assembly. Flagged as ambiguous.
- **Collisions**: Variants that would map to the same position as another variant. Flagged to prevent data loss.

## Step 3: Run the Liftover

Once satisfied with the dry run, execute the actual liftover. It is strongly recommended to create a backup first:

```bash
graphmana liftover \
    --chain hg19ToHg38.over.chain.gz \
    --target-reference GRCh38 \
    --reject-file liftover_rejects.tsv \
    --backup-before \
    --neo4j-home /var/lib/neo4j \
    --verbose
```

Expected output:

```
Creating backup snapshot: pre_liftover_GRCh38 ...
Backup complete.
Loading chain file: hg19ToHg38.over.chain.gz
  Loaded 1,264 chain records
Fetching variants from database...
  1,072,533 variants on 1 chromosome(s)
Computing coordinate mappings...
Applying liftover in batches of 500...
  Batch 1/2142: 500 variants updated
  Batch 2/2142: 500 variants updated
  ...
  Batch 2142/2142: 391 variants updated
Updating SchemaMetadata reference_genome -> GRCh38

Target reference: GRCh38
Total variants:   1,072,533
Mapped:           1,070,891
Unmapped:             1,412
Ambiguous:              198
Collisions:              32
Reject file:      liftover_rejects.tsv
```

## What Changes During Liftover

| Property | Updated? | Details |
|----------|----------|---------|
| `variantId` | Yes | Rebuilt as `chr-pos-ref-alt` with new coordinates |
| `pos` | Yes | New position in target assembly |
| `chr` | Yes | Chromosome name may change (e.g., `chrM` renaming) |
| `ref` | Sometimes | Only for strand-flip variants (complemented) |
| `alt` | Sometimes | Only for strand-flip variants (complemented) |
| `liftover_status` | Set | `"mapped"`, `"unmapped"`, `"ambiguous"`, or `"collision"` |
| `original_variantId` | Set | Preserves the pre-liftover variant ID |
| `gt_packed` | No | Genotype data is assembly-independent |
| `phase_packed` | No | Phase data is assembly-independent |
| `pop_ids[], ac[], an[], af[]` | No | Population statistics are unchanged |
| Sample nodes | No | Sample metadata is assembly-independent |
| Population nodes | No | Population metadata is assembly-independent |
| NEXT chain edges | Yes | Rebuilt to reflect new positions |

## Step 4: Inspect the Reject File

The reject file contains all variants that could not be cleanly mapped:

```bash
head -5 liftover_rejects.tsv
```

Expected output:

```
original_variantId	chr	pos	ref	alt	status	reason
chr22-16050075-A-G	chr22	16050075	A	G	unmapped	No chain alignment
chr22-16050115-C-T	chr22	16050115	C	T	unmapped	No chain alignment
chr22-16870200-G-A	chr22	16870200	G	A	ambiguous	Multiple target mappings
chr22-17450012-T-C	chr22	17450012	T	C	collision	Target position occupied
```

## Step 5: Verify the Result

Check the database status to confirm the reference genome has been updated:

```bash
graphmana status
```

Expected output:

```
GraphMana v0.9.0
Connected to: bolt://localhost:7687

Node counts:
  Variant           1,072,533
  Sample                3,202
  Population               26
  Chromosome                1
  Gene                      0
  VCFHeader                 1

Schema version:   2
Reference genome: GRCh38
```

Query unmapped variants:

```bash
graphmana query "MATCH (v:Variant) WHERE v.liftover_status <> 'mapped' RETURN v.liftover_status AS status, count(v) AS n" --format table
```

Expected output:

```
status    | n
----------+------
unmapped  | 1412
ambiguous |  198
collision |   32
```

## Filtering on Liftover Status

After liftover, you can filter exports to include only mapped variants:

```bash
# Export only successfully mapped variants
graphmana export --format vcf --output mapped_only.vcf \
    --filter-liftover-status mapped

# Export unmapped variants for investigation
graphmana export --format tsv --output unmapped.tsv \
    --filter-liftover-status unmapped \
    --tsv-columns variantId,chr,pos,ref,alt,original_variantId,liftover_status
```

## Reverting a Liftover

If you need to undo a liftover, restore from the backup snapshot:

```bash
# Stop Neo4j first
graphmana neo4j-stop --neo4j-home /var/lib/neo4j

# Restore the pre-liftover snapshot
graphmana snapshot restore \
    --name pre_liftover_GRCh38 \
    --neo4j-home /var/lib/neo4j

# Restart Neo4j
graphmana neo4j-start --neo4j-home /var/lib/neo4j --wait
```

Verify the reference is back to the original:

```bash
graphmana status
```

Expected output:

```
...
Reference genome: GRCh37
```

## Liftover for Non-Human Genomes

GraphMana is species-agnostic. Any organism with a UCSC-format chain file works. For example, converting mouse coordinates from mm9 to mm10:

```bash
graphmana liftover \
    --chain mm9ToMm10.over.chain.gz \
    --target-reference mm10 \
    --reject-file mouse_rejects.tsv \
    --verbose
```

For plant genomes (e.g., rice), you may need to generate a custom chain file using tools like `liftOver` or `CrossMap`. The chain file format is documented at [UCSC](https://genome.ucsc.edu/goldenPath/help/chain.html).

## Best Practices

1. **Always dry-run first.** The `--dry-run` flag lets you evaluate the mapping quality before committing changes.

2. **Always back up.** Use `--backup-before` or manually create a snapshot. Liftover modifies variant IDs in place.

3. **Save the reject file.** The `--reject-file` output is your record of what could not be mapped. Keep it with your analysis notes.

4. **Run QC after liftover.** Liftover can expose edge cases in your data:

   ```bash
   graphmana qc --type variant --output post_liftover_qc.html --format html
   ```

5. **Check provenance.** The liftover is recorded in the provenance trail:

   ```bash
   graphmana provenance list
   ```

## See Also

- [Tutorial](../tutorial.md) -- Initial import and basic operations
- [Vignette 06: Sample Management](06-sample-lifecycle.md) -- Managing samples
- [Vignette 10: Database Administration](10-database-admin.md) -- Snapshots and backups
