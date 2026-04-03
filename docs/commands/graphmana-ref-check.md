# graphmana ref-check

Verify stored REF alleles in the database against a reference genome FASTA
file. Reports any mismatches as a TSV.

Useful for quality control after liftover, verifying data integrity, or
cross-checking imported data against a known reference.

## Usage

```
graphmana ref-check --fasta <path> [OPTIONS]
```

## Options

```
  --fasta PATH              Reference genome FASTA file (with .fai index
                            recommended).  [required]
  --output PATH             Output TSV file for mismatches. Default: stdout.
  --chromosomes TEXT        Limit check to these chromosomes.
  --max-mismatches INTEGER  Stop after N mismatches (0 = report all).
  --neo4j-uri TEXT          Neo4j Bolt URI.
  --neo4j-user TEXT         Neo4j username.
  --neo4j-password TEXT     Neo4j password.
  --database TEXT           Neo4j database name.
```

## Performance

For best performance, provide a FASTA file with a `.fai` index (created by
`samtools faidx`). Without an index, GraphMana loads entire chromosome
sequences into memory one at a time.

## Example

```bash
# Check all variants against GRCh38
graphmana ref-check --fasta GRCh38.fa --output mismatches.tsv

# Quick spot-check on chr22
graphmana ref-check --fasta GRCh38.fa --chromosomes chr22 --max-mismatches 10
```

## Output Format

Tab-separated columns:

```
variantId       chr     pos     stored_ref      genome_ref
chr22:16050408:A:G  chr22   16050408    A       A
chr22:16050612:C:T  chr22   16050612    G       C
```
