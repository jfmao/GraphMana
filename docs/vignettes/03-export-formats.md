# Export Formats Reference

GraphMana supports 17 export formats organized into two access paths:

- **FAST PATH**: Reads pre-computed population arrays (`ac[]`, `an[]`, `af[]`).
  No genotype unpacking. Completes in seconds to minutes regardless of sample
  count.
- **FULL PATH**: Unpacks `gt_packed` and `phase_packed` byte arrays to
  reconstruct per-sample genotypes. Runtime is linear in the number of
  samples.

To see all available formats at a glance, run:

```bash
graphmana list-formats
```

All examples below assume a 1KGP import (3,202 samples, 26 populations). See
[02-import-1kgp.md](02-import-1kgp.md) for import instructions.

## Benchmark timing (1KGP, 3,202 samples)

| Format | Scope | Time | Output size | Path |
|--------|-------|------|-------------|------|
| TreeMix | all chromosomes | 102 min | 780 MB | FAST |
| SFS dadi (2-pop) | all chromosomes | 98 min | 5.7 KB | FAST |
| SFS fsc (2-pop) | all chromosomes | 101 min | 1.3 KB | FAST |
| BED | all chromosomes | 103 min | 2.9 GB | FAST |
| TSV | all chromosomes | 101 min | 3.8 GB | FAST |
| VCF | chr22 only | 649 s | 208 MB | FULL |
| PLINK 1.9 | chr22 only | 22 s | 336 MB | FULL |
| EIGENSTRAT | chr22 only | 192 s | 2.8 GB | FULL |
| PLINK 1.9 | all chromosomes, 8 threads | 156 s | 7.2 GB | FULL |

FAST PATH timings include the overhead of scanning all 70.7M Variant nodes to
read their population arrays. The actual SFS/TreeMix computation is
instantaneous because the arrays are constant size (K populations, not N
samples).

---

## FAST PATH formats

### TreeMix

Population allele count matrix for the TreeMix admixture graph tool. One row
per variant, one column per population. Each cell contains
`allele_count,total_minus_allele_count`.

```bash
graphmana export \
    --format treemix \
    --output exports/1kgp.treemix.gz
```

**Produces**: gzipped text file. **Consumed by**: TreeMix, OrientAGraph.

```bash
zcat exports/1kgp.treemix.gz | head -2
```

**Expected output:**

```
AFR AMR EAS EUR SAS
3420,1894 532,384 340,736 602,606 574,424
```

### SFS dadi (Site Frequency Spectrum)

Joint or marginal site frequency spectrum for dadi (Diffusion Approximation
for Demographic Inference).

```bash
graphmana export \
    --format sfs-dadi \
    --output exports/eur_eas.dadi.sfs \
    --sfs-populations EUR EAS \
    --sfs-projection 100 100 \
    --sfs-folded
```

**Produces**: dadi-format frequency spectrum file. **Consumed by**: dadi,
moments.

> **Note**: Use `--sfs-folded` unless your data has polarized ancestral allele
> information (imported via `--ancestral-fasta` during `prepare-csv`). Using
> `--sfs-polarized` (the default) without ancestral alleles will produce
> incorrect spectra.

### SFS fastsimcoal2

Site frequency spectrum in fastsimcoal2 `.obs` format.

```bash
graphmana export \
    --format sfs-fsc \
    --output exports/eur_eas.fsc \
    --sfs-populations EUR EAS \
    --sfs-projection 100 100 \
    --sfs-folded
```

**Produces**: `.obs` file with fastsimcoal2 naming convention. **Consumed
by**: fastsimcoal2, fastsimcoal26.

### BED

Variant positions in BED format for genomic interval tools.

```bash
graphmana export \
    --format bed \
    --output exports/variants.bed
```

**Produces**: tab-separated BED3+ file. **Consumed by**: bedtools, IGV,
UCSC Genome Browser.

Add extra columns with `--bed-extra-columns`:

```bash
graphmana export \
    --format bed \
    --output exports/variants_annotated.bed \
    --bed-extra-columns variant_type af_total
```

### TSV

Flexible tab-separated output with configurable columns.

```bash
graphmana export \
    --format tsv \
    --output exports/variants.tsv \
    --tsv-columns chr pos ref alt af_total variant_type
```

**Produces**: tab-separated file with header. **Consumed by**: R, Python
(pandas), Excel, any text processor.

Default columns (when `--tsv-columns` is omitted): `chr`, `pos`, `ref`,
`alt`, `af_total`, `variant_type`.

---

## FULL PATH formats

### VCF

Standard Variant Call Format with full per-sample genotypes reconstructed from
packed arrays.

```bash
# Uncompressed VCF
graphmana export \
    --format vcf \
    --output exports/chr22.vcf \
    --chromosomes chr22

# Gzipped VCF
graphmana export \
    --format vcf \
    --output exports/chr22.vcf.gz \
    --chromosomes chr22 \
    --output-type z

# BCF (binary)
graphmana export \
    --format vcf \
    --output exports/chr22.bcf \
    --chromosomes chr22 \
    --output-type b
```

**Produces**: VCF 4.3 / gzipped VCF / BCF. **Consumed by**: bcftools,
GATK, PLINK, any VCF-compliant tool.

Additional options:

- `--phased` -- output phased genotypes (`0|1` instead of `0/1`)
- `--vcf-version 4.1` -- VCF 4.1 header (for older tools)
- `--no-reconstruct-multiallelic` -- keep split alleles as separate records

### PLINK 1.9

Binary genotype files for PLINK 1.9 and compatible tools.

```bash
graphmana export \
    --format plink \
    --output exports/chr22 \
    --chromosomes chr22
```

**Produces**: `.bed`, `.bim`, `.fam` files. **Consumed by**: PLINK 1.9,
GCTA, LDAK, KING, flashpca.

Multi-chromosome export with parallelism:

```bash
graphmana export \
    --format plink \
    --output exports/1kgp_all \
    --threads 8
```

### PLINK 2.0

PLINK 2 binary format with full multiallelic and phasing support.

```bash
graphmana export \
    --format plink2 \
    --output exports/chr22 \
    --chromosomes chr22
```

**Produces**: `.pgen`, `.pvar`, `.psam` files. **Consumed by**: PLINK 2.0,
regenie.

### EIGENSTRAT

EIGENSTRAT format for population structure analysis.

```bash
graphmana export \
    --format eigenstrat \
    --output exports/chr22 \
    --chromosomes chr22
```

**Produces**: `.geno`, `.snp`, `.ind` files. **Consumed by**: smartPCA
(EIGENSOFT), AdmixTools, AdmixTools 2.

### Beagle

Genotype likelihood format for phasing and imputation.

```bash
graphmana export \
    --format beagle \
    --output exports/chr22.beagle.gz \
    --chromosomes chr22
```

**Produces**: gzipped Beagle format file. **Consumed by**: Beagle 5.x,
IMPUTE5.

### STRUCTURE

Input format for the STRUCTURE population assignment program.

```bash
# One-row format (default)
graphmana export \
    --format structure \
    --output exports/data.structure \
    --chromosomes chr22

# Two-row format (one row per haplotype)
graphmana export \
    --format structure \
    --output exports/data.structure \
    --chromosomes chr22 \
    --structure-format tworow
```

**Produces**: STRUCTURE input file. **Consumed by**: STRUCTURE, fastSTRUCTURE,
ADMIXTURE (with conversion).

### Genepop

Format for conservation genetics and population differentiation analysis.

```bash
graphmana export \
    --format genepop \
    --output exports/data.genepop \
    --chromosomes chr22
```

**Produces**: Genepop format file. **Consumed by**: Genepop, GenAlEx, Arlequin.

### Haplotype (.hap/.map)

Haplotype format for selection scan tools. Requires phased data.

```bash
graphmana export \
    --format hap \
    --output exports/chr22 \
    --chromosomes chr22
```

**Produces**: `.hap` and `.map` files. **Consumed by**: selscan (iHS, XP-EHH,
nSL), hapbin.

### JSON

JSON Lines format for programmatic access and web APIs.

```bash
# Variant-level summary (FAST PATH when no genotypes requested)
graphmana export \
    --format json \
    --output exports/variants.jsonl \
    --chromosomes chr22

# With per-sample genotypes (FULL PATH)
graphmana export \
    --format json \
    --output exports/variants_with_gt.jsonl \
    --chromosomes chr22 \
    --json-include-genotypes \
    --json-pretty
```

**Produces**: JSON Lines (one JSON object per line). **Consumed by**: jq,
Python, any JSON parser.

### Zarr

Chunked array format for Python genomics libraries.

```bash
graphmana export \
    --format zarr \
    --output exports/chr22.zarr \
    --chromosomes chr22 \
    --zarr-chunk-size 10000
```

**Produces**: Zarr directory store. **Consumed by**: sgkit, scikit-allel,
xarray.

### GDS (SeqArray)

HDF5-based format for R/Bioconductor genomics.

```bash
graphmana export \
    --format gds \
    --output exports/chr22.gds \
    --chromosomes chr22
```

**Produces**: SeqArray GDS file (requires h5py). **Consumed by**: SeqArray
(R/Bioconductor), SNPRelate.

### BGEN

Binary format for large-scale genetic association studies.

```bash
graphmana export \
    --format bgen \
    --output exports/chr22.bgen \
    --chromosomes chr22
```

**Produces**: BGEN 1.2 file (Layout 2, zlib compression). **Consumed by**:
BGENIX, QCTOOL, regenie, BOLT-LMM.

---

## Common export filters

All formats support the same filtering options. Filters are applied before
export and pushed into Cypher WHERE clauses where possible.

```bash
# Export only SNPs with MAF > 5% on chr22
graphmana export \
    --format plink \
    --output exports/common_snps \
    --chromosomes chr22 \
    --filter-variant-type SNP \
    --filter-maf-min 0.05

# Export specific populations
graphmana export \
    --format treemix \
    --output exports/eur_eas.treemix.gz \
    --populations EUR EAS \
    --recalculate-af

# Export a genomic region
graphmana export \
    --format vcf \
    --output exports/region.vcf \
    --region chr22:20000000-25000000

# Export using a named cohort (see 04-cohort-management.md)
graphmana export \
    --format plink \
    --output exports/european_cohort \
    --filter-cohort european_samples

# Export with annotation filters (see 05-annotation.md)
graphmana export \
    --format vcf \
    --output exports/high_impact.vcf \
    --filter-impact HIGH \
    --filter-cadd-min 20
```

## Parallelism

FULL PATH exports support `--threads` for multi-chromosome parallelism. Each
chromosome is exported independently and results are merged.

```bash
graphmana export \
    --format plink \
    --output exports/1kgp_all \
    --threads 8
```

Parallel and single-threaded exports produce identical output. Default is
`--threads 1`.

## Export Manifests

Every export automatically writes a `.manifest.json` sidecar file alongside
the output. The manifest records:

- GraphMana version and timestamp
- Output format and file path
- Number of variants and samples exported
- Chromosomes included
- All active filters (populations, MAF, cohort, annotations, etc.)
- Thread count and recalculate_af setting

This ensures reproducibility: when a collaborator asks "which samples were
in this TreeMix file?", the answer is in the manifest.

```bash
# Manifest written automatically
graphmana export --format treemix --output pop_tree.treemix.gz
# Creates: pop_tree.treemix.gz and pop_tree.treemix.gz.manifest.json

# Skip the manifest if not needed
graphmana export --format treemix --output pop_tree.treemix.gz --no-manifest
```

## See also

- [01-quickstart.md](01-quickstart.md) -- First import and export
- [02-import-1kgp.md](02-import-1kgp.md) -- Importing the benchmark dataset
- [04-cohort-management.md](04-cohort-management.md) -- Using cohorts as export filters
- [05-annotation.md](05-annotation.md) -- Filtering exports by annotation
