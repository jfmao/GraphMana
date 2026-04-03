# Vignette 11: Multi-allelic Sites and Structural Variants

This vignette explains how GraphMana represents multi-allelic sites, structural
variants, and large indels in the graph database. It covers the full lifecycle
from import through storage to export, including roundtrip validation results.

## Prerequisites

- A running GraphMana database with imported data
- The `graphmana` CLI installed and configured

We assume a database loaded with 1000 Genomes chr22 data (3,202 samples, 1.07M
variants). See [02-import-1kgp.md](02-import-1kgp.md) for import instructions.

---

## Multi-allelic Sites

### What is a multi-allelic site?

A multi-allelic site is a genomic position with more than one alternative allele.
In VCF, these appear as a single record with a comma-separated ALT field:

```
chr22  16050408  .  A  G,T  .  PASS  .  GT  0/1  0/2  1/2
```

This site has two alternative alleles (G and T), producing three possible
non-reference genotypes.

### Import: splitting to biallelic records

GraphMana splits every multi-allelic VCF record into K biallelic Variant nodes,
where K is the number of ALT alleles. Each split record carries two linking
properties:

- `multiallelic_site` -- a shared identifier grouping all alleles from the same
  original VCF line (e.g., `chr22-16050408-A`)
- `allele_index` -- a 1-based index indicating which ALT allele this node
  represents (1 for the first ALT, 2 for the second, and so on)

For the example above, two Variant nodes are created:

| variantId | ref | alt | multiallelic_site | allele_index |
|-----------|-----|-----|-------------------|--------------|
| chr22-16050408-A-G | A | G | chr22-16050408-A | 1 |
| chr22-16050408-A-T | A | T | chr22-16050408-A | 2 |

No special flags are needed at import time. Splitting is automatic:

```bash
graphmana ingest \
    --input data/multiallelic_example.vcf.gz \
    --population-map data/pops.tsv \
    --reference GRCh38 \
    --dataset-id multiallelic_demo
```

### Storage

Each biallelic split has its own:

- `gt_packed` byte array -- genotypes recoded relative to that specific ALT
  allele (HomRef=0, Het=1, HomAlt=2, Missing=3)
- Population arrays (`ac[]`, `an[]`, `af[]`) -- allele counts computed
  independently per allele
- All other Variant properties (`qual`, `filter`, `consequence`, etc.)

This means a sample that is heterozygous for two different ALT alleles (genotype
`1/2` in the original VCF) will appear as Het (0/1) on the first allele's node
and Het (0/1) on the second allele's node. A sample homozygous for the first ALT
(`1/1`) will appear as HomAlt on allele 0 and HomRef on allele 1.

### Export: VCF reconstruction

By default, VCF export reconstructs multi-allelic records by merging Variant
nodes that share the same `multiallelic_site` value back into a single VCF line
with a comma-separated ALT field. This is the `--reconstruct-multiallelic`
behavior (enabled by default).

```bash
# Default: multi-allelic sites are merged back into single VCF lines
graphmana export \
    --format vcf \
    --output exports/reconstructed.vcf.gz \
    --output-type z
```

Expected output (excerpt from the VCF):

```
#CHROM  POS       ID  REF  ALT  QUAL  FILTER  ...
chr22   16050408  .   A    G,T  .     PASS    ...
```

To keep alleles as separate biallelic records (one line per ALT allele):

```bash
# Split: each allele on its own VCF line
graphmana export \
    --format vcf \
    --output exports/split.vcf.gz \
    --output-type z \
    --no-reconstruct-multiallelic
```

Expected output (excerpt):

```
#CHROM  POS       ID  REF  ALT  QUAL  FILTER  ...
chr22   16050408  .   A    G    .     PASS    ...
chr22   16050408  .   A    T    .     PASS    ...
```

### Export: biallelic-only formats (PLINK, EIGENSTRAT)

PLINK 1.9 and EIGENSTRAT are biallelic formats. Each allele is exported as a
separate record regardless of multi-allelic grouping:

```bash
graphmana export \
    --format plink \
    --output exports/1kgp_chr22
```

The `.bim` file will contain one row per ALT allele. For a tri-allelic site, two
rows appear at the same position with different A2 alleles.

### Export: TreeMix and SFS

TreeMix and SFS exporters operate on the FAST PATH, reading pre-computed
population arrays. Each allele is counted independently in the output:

```bash
graphmana export \
    --format treemix \
    --output exports/1kgp.treemix.gz
```

A tri-allelic site contributes two rows to the TreeMix output, one per ALT
allele.

### Roundtrip validation

We validated genotype roundtrip fidelity by importing chr22 from the 1000
Genomes Project (5-sample subset, 897,645 biallelic SNPs), exporting back to
VCF, and comparing per-sample genotypes using bcftools.

**Result: 99.999%+ concordance.** The few mismatches (2-8 per sample across
nearly 900,000 sites) occur exclusively at multi-allelic positions where two
biallelic records share the same genomic coordinate. These are a test artifact
caused by position-based joining during comparison, not real genotype errors in
the database. The genotypes stored in `gt_packed` are correct; the apparent
discrepancies arise because bcftools joins on CHROM+POS and cannot distinguish
which biallelic record corresponds to which original allele at shared
coordinates.

### Querying multi-allelic sites in Cypher

Find all alleles at a multi-allelic site:

```cypher
MATCH (v:Variant)
WHERE v.multiallelic_site = 'chr22-16050408-A'
RETURN v.variantId, v.alt, v.allele_index, v.af_total
ORDER BY v.allele_index
```

Count multi-allelic sites on a chromosome:

```cypher
MATCH (v:Variant)
WHERE v.chr = 'chr22' AND v.multiallelic_site IS NOT NULL
RETURN count(DISTINCT v.multiallelic_site) AS n_multiallelic_sites
```

---

## Structural Variants

### Supported SV types

GraphMana imports structural variants detected from symbolic ALT alleles in VCF
files (`<DEL>`, `<DUP>`, `<INV>`, `<INS>`, `<CNV>`). These are stored as
Variant nodes with additional properties:

| Property | Description | Example |
|----------|-------------|---------|
| `variant_type` | Set to `SV` for structural variants | `SV` |
| `sv_type` | Specific SV class | `DEL`, `DUP`, `INV`, `INS`, `CNV` |
| `sv_len` | Length of the structural variant in bp | `-5000` (deletions are negative) |
| `sv_end` | End coordinate of the SV | `16055408` |

### Import

Structural variants are detected automatically from the ALT field. No special
flags are needed:

```bash
graphmana ingest \
    --input data/sv_calls.vcf.gz \
    --population-map data/pops.tsv \
    --reference GRCh38 \
    --dataset-id sv_demo
```

Genotype calls for SVs are stored in `gt_packed` exactly as for any other
variant. A sample called `0/0` is HomRef, `0/1` is Het (carries the SV on one
haplotype), and `1/1` is HomAlt (carries the SV on both haplotypes).

### Filtering by SV type

Filter imports or exports to specific variant or SV types:

```bash
# Import only structural variants
graphmana ingest \
    --input data/mixed_snps_svs.vcf.gz \
    --population-map data/pops.tsv \
    --reference GRCh38 \
    --filter-variant-type SV

# Export only deletions
graphmana export \
    --format vcf \
    --output exports/deletions.vcf.gz \
    --filter-variant-type SV \
    --filter-sv-type DEL

# Export only duplications as a BED file for bedtools
graphmana export \
    --format bed \
    --output exports/duplications.bed \
    --filter-variant-type SV \
    --filter-sv-type DUP \
    --bed-extra-columns sv_len,sv_end
```

### Querying SVs in Cypher

```cypher
// Count SVs by type
MATCH (v:Variant)
WHERE v.variant_type = 'SV'
RETURN v.sv_type, count(v) AS n
ORDER BY n DESC

// Find large deletions (>10 kb) with high allele frequency
MATCH (v:Variant)
WHERE v.sv_type = 'DEL' AND abs(v.sv_len) > 10000 AND v.af_total > 0.05
RETURN v.variantId, v.chr, v.pos, v.sv_len, v.af_total
ORDER BY v.af_total DESC
LIMIT 20
```

### 1KGP benchmark

In the 1000 Genomes Project whole-genome dataset, structural variants comprise
97,722 out of 70.7M total variants (0.14%). The majority are deletions and
insertions. All SV genotypes are stored and exported using the same packed array
infrastructure as SNPs and indels.

### Limitations

Not all SV representations are fully supported in v1.0:

1. **BND (breakend/translocation)**: Imported as independent Variant nodes, but
   mate pairs are NOT linked in the graph. Each breakend end is a separate node
   with no `BREAKPOINT_MATE` relationship.

2. **Copy number states beyond diploid**: The VCF `CN` field (CN=0, 1, 2, 3, 4)
   is not captured in `gt_packed`, which stores only the diploid genotype call
   (0/0, 0/1, 1/1). Integer copy number information is lost.

3. **Complex multi-record SVs**: Some callers represent a single structural
   variant across multiple VCF records (e.g., inversion with flanking deletions).
   GraphMana imports these as independent Variant nodes without grouping them.

4. **Mobile element subtype classification**: MEI subtypes (ALU, L1, SVA) are
   stored in the `alt` field but are not indexed as a separate searchable
   property.

### Future directions

Planned improvements for structural variant handling:

- `BREAKPOINT_MATE` relationship linking BND mate pairs
- `cn_packed` byte array for integer copy number states
- SV grouping for complex multi-record events
- MEI subtype indexing

---

## Large Indels

Indels longer than 50 bp with explicit REF and ALT sequences (not symbolic
alleles) require no special handling. They are stored as ordinary biallelic
Variant nodes with `variant_type=INDEL`:

```bash
# Large indels are imported automatically alongside SNPs
graphmana ingest \
    --input data/calls_with_large_indels.vcf.gz \
    --population-map data/pops.tsv \
    --reference GRCh38
```

The full REF and ALT strings are stored as Variant node properties, regardless
of length. A 500 bp deletion with an explicit REF sequence works identically to
a 1 bp SNP from the perspective of genotype storage, population arrays, and
export.

```cypher
// Find indels longer than 100 bp
MATCH (v:Variant)
WHERE v.variant_type = 'INDEL'
  AND (size(v.ref) > 100 OR size(v.alt) > 100)
RETURN v.variantId, size(v.ref) AS ref_len, size(v.alt) AS alt_len
ORDER BY ref_len + alt_len DESC
LIMIT 10
```

The only consideration is storage: a variant with a 10 kb REF string consumes
more property storage than a SNP, but this is negligible in practice since such
variants are rare.

---

## See Also

- [Vignette 02: Importing 1000 Genomes](02-import-1kgp.md) -- Full import walkthrough
- [Vignette 03: Export Formats](03-export-formats.md) -- All 17 export formats
- [Vignette 06: Sample Management](06-sample-lifecycle.md) -- Incremental sample addition
- [Vignette 10: Database Administration](10-database-admin.md) -- QC and provenance
