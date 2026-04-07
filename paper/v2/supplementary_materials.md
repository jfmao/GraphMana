# GraphMana — Supplementary Materials

## Supplementary Table 1. Detailed benchmark results (chr22, 1.07M variants, 3,202 samples)

### Benchmark 1: Incremental Sample Addition (3 batches of 234 samples)

| Operation | GraphMana (s) | bcftools (s) |
|-----------|--------------|-------------|
| Initial import / base copy | 602 | — |
| Batch 1 | 418 | 117 |
| Batch 2 | 412 | 125 |
| Batch 3 | 424 | 133 |
| **Total** | **1,856** | **374** |

GraphMana uses CSV-to-CSV rebuild (read existing CSVs, extend packed arrays, reimport via neo4j-admin). bcftools uses `bcftools merge`.

### Benchmark 2: Cohort VCF Extraction (5 superpopulation cohorts)

| Cohort (N samples) | GraphMana (s) | bcftools (s) |
|--------------------|--------------|-------------|
| AFR (893) | 191 | 59 |
| EUR (633) | 211 | 51 |
| EAS (585) | 131 | 49 |
| EUR+EAS (1,218) | 238 | 64 |
| ALL (3,202) | 571 | 110 |

GraphMana exports via Cypher query with population filter. bcftools uses `bcftools view -S`.

### Benchmark 3: Multi-format Export

| Format | GraphMana (s) | bcftools (s) |
|--------|--------------|-------------|
| VCF | 473 | 96 |
| TreeMix | 189 | N/A |
| SFS (dadi) | 86 | N/A |
| SFS (fsc) | 119 | N/A |
| BED | 84 | N/A |
| TSV | 85 | N/A |

GraphMana produces all 6 formats from the single database. bcftools supports VCF output only; the remaining 5 formats have no bcftools equivalent.

### Benchmark 4: Annotation Update (53,000 BED regions)

| | GraphMana | bcftools |
|---|-----------|----------|
| Time | **3.5 s** | 96 s |
| Method | In-place graph edge update | Full VCF rewrite |
| **Speedup** | **27x** | baseline |

### Benchmark 5: Full Lifecycle Simulation (7 phases)

| | GraphMana | bcftools |
|---|-----------|----------|
| Total wall time | 5,880 s (98 min) | 1,006 s (17 min) |
| Operations completed | **46 / 46** | **17 / 26** |
| Operations not supported | 0 | 9 |

Operations not supported by bcftools: TreeMix export (6 instances), SFS export, BED export, in-place annotation update.

---

## Supplementary Table 2. Whole-genome export benchmarks (70.7M variants, 3,202 samples, 26 populations)

| Format | Access Path | Scope | Variants exported | Wall time | Output size |
|--------|-------------|-------|-------------------|-----------|-------------|
| TreeMix | FAST | All chromosomes | 70,692,015 | 102 min | 780 MB |
| SFS dadi (2-pop, folded) | FAST | All chromosomes | 70,692,015 | 98 min | 5.7 KB |
| SFS fsc (2-pop, folded) | FAST | All chromosomes | 70,692,015 | 101 min | 1.3 KB |
| BED | FAST | All chromosomes | 70,692,007 | 103 min | 2.9 GB |
| TSV (allele frequencies) | FAST | All chromosomes | 70,692,007 | 101 min | 3.8 GB |
| PLINK 1.9 (SNPs, 8 threads) | FULL | All chromosomes | 9,627,636 | 156 s (2.6 min) | 7.2 GB |
| VCF (BGZF) | FULL | All chromosomes | 68,912,619 | 13,294 s (3.7 hr) | 14 GB |
| EIGENSTRAT | FULL | All chromosomes | 61,599,149 | 13,351 s (3.7 hr) | 184 GB |

FAST PATH timings include the overhead of scanning all 70.7M Variant nodes to read population arrays; the actual computation is instantaneous because arrays are constant size (K populations, not N samples). FULL PATH timings are dominated by genotype unpacking and file I/O.

### Whole-genome incremental addition (234 samples added to existing 70.7M-variant database)

| Step | Time |
|------|------|
| VCF parsing (chr22 batch, 1M variants) | 3 min |
| CSV read + extend + write (70.7M variants, 214 GB) | 160 min |
| neo4j-admin import | 15 min |
| Neo4j restart + indexes | 4 min |
| **Total** | **182 min** |

Variant breakdown: 1,066,557 extended with actual genotypes (chr22); 69,625,458 HomRef-extended (other chromosomes, zero-byte append); 70,691,875 total variants preserved.

### Full pipeline timing (from scratch)

| Step | Time |
|------|------|
| prepare-csv (22 VCFs, 2,500 samples, 16 threads) | 95 min |
| neo4j-admin import (214 GB CSV → 166 GB database) | 3 min |
| Incremental add (234 samples, CSV-to-CSV) | 182 min |
| **Total: initial + 1 incremental** | **280 min (4.7 hr)** |

---

## Supplementary Table 3. Storage and performance scaling

### Storage estimates

GraphMana's packed encoding scales linearly with sample count. The maximum dataset size is hardware-dependent, not architecturally limited. For N samples, whole genome (~85M biallelic variants):

| Component | Formula | 3,202 | 10,000 | 50,000 | 200,000 | 500,000 |
|-----------|---------|-------|--------|--------|---------|---------|
| gt_packed | ceil(N/4) x 85M | ~68 GB | ~213 GB | ~1.0 TB | ~4.3 TB | ~10.6 TB |
| phase_packed | ceil(N/8) x 85M | ~34 GB | ~106 GB | ~0.5 TB | ~2.1 TB | ~5.3 TB |
| Pop arrays + properties | ~200 bytes x 85M | ~17 GB | ~17 GB | ~17 GB | ~17 GB | ~17 GB |
| NEXT chain edges | ~50 bytes x 85M | ~4 GB | ~4 GB | ~4 GB | ~4 GB | ~4 GB |
| Annotation edges | ~100 bytes x edges | ~10-50 GB | ~10-50 GB | ~10-50 GB | ~10-50 GB | ~10-50 GB |
| Neo4j overhead (~30%) | variable | variable | variable | variable | variable | variable |
| **Total estimate** | | **~130-200 GB** | **~400-550 GB** | **~2-3 TB** | **~8-10 TB** | **~20-25 TB** |

For exome-only data (~5M variants): divide variant-proportional numbers by ~17.
For single chromosome (chr22, ~1M variants): divide by ~85.

### Performance scaling by access path

The two access paths have fundamentally different scaling behaviors:

| Operation | Scaling | 3,202 samples | 10,000 | 50,000 | 500,000 |
|-----------|---------|---------------|--------|--------|---------|
| **FAST PATH** (TreeMix, SFS, BED) | O(K populations) | Seconds | Seconds | Seconds | Seconds |
| **FULL PATH** (VCF, PLINK) | O(N samples) | Minutes | Minutes-hours | Hours-days | Days-weeks |
| Incremental add (WGS) | O(M variants x N) | 182 min | Hours | Many hours | Days |

FAST PATH operations read pre-computed K-element population arrays (where K is typically 5-30), making them constant time regardless of sample count. Even at 500,000 samples, TreeMix and SFS export complete in seconds because the arrays are the same size.

FULL PATH operations unpack per-sample genotypes from packed byte arrays. At 500,000 samples, each variant's gt_packed is 125 KB, and processing 85M variants means reading ~10 TB of packed data. This is the practical bottleneck.

### Scaling limits by tier

| Scale | Hardware | FAST PATH | FULL PATH | Incremental | Status |
|-------|----------|-----------|-----------|-------------|--------|
| 100-10,000 | 64-256 GB RAM, 0.2-3 TB disk | Interactive | Interactive | Minutes | **Sweet spot** |
| 10,000-50,000 | 256 GB-1 TB RAM, 3-10 TB disk | Interactive | Slow (hours) | Hours | **Supported** |
| 50,000-200,000 | 1-2 TB RAM, 10-50 TB disk | Interactive | Very slow (days) | Many hours | **Technically possible** |
| 200,000-500,000 | 2+ TB RAM, 50-150 TB disk | Interactive | Infeasible for WGS | Days | **Storage works; queries limited** |
| 500,000+ | >2 TB RAM, >150 TB disk | Interactive | Infeasible | Days-weeks | **Not recommended** |

The scaling limit is Neo4j Community Edition's single-node architecture, not GraphMana's storage encoding. At biobank scale (500,000+ samples), distributed frameworks such as Hail are more appropriate for FULL PATH operations. However, FAST PATH queries (population-level statistics, allele frequency tables, SFS) remain instant at any sample count because they never touch per-sample data.

---

## Supplementary Note 1. Variant representation

GraphMana decomposes multi-allelic VCF records into K biallelic Variant nodes (one per alternative allele), each carrying a shared `multiallelic_site` identifier and a 1-based `allele_index`. Genotypes are recoded independently per allele: a sample heterozygous for two ALT alleles (VCF genotype 1/2) is recorded as heterozygous on both corresponding nodes. Each biallelic split maintains its own packed genotype array (`gt_packed`, 2 bits per sample) and pre-computed population-level allele count, sample count, and frequency arrays (`ac[]`, `an[]`, `af[]`).

During VCF export, records sharing a `multiallelic_site` value are merged back into a single multi-allelic line with a comma-separated ALT field; this reconstruction is the default behaviour and can be disabled with `--no-reconstruct-multiallelic` for workflows that require biallelic-only output. Biallelic-only export formats (PLINK 1.9, EIGENSTRAT) emit each allele as a separate record.

Structural variants with symbolic ALT alleles (`<DEL>`, `<DUP>`, `<INV>`, `<INS>`, `<CNV>`) are stored as Variant nodes annotated with `sv_type`, `sv_len`, and `sv_end` properties; diploid genotype calls are encoded in `gt_packed` identically to SNPs and indels, enabling uniform filtering and export across all variant classes. Breakend (BND) records are imported as independent nodes without mate-pair linking, and integer copy number states beyond the diploid genotype call are not retained in the current schema.

We validated genotype roundtrip fidelity by importing 1000 Genomes Project chromosome 22 data (897,645 biallelic SNPs, 5 samples), exporting to VCF, and comparing per-sample genotypes with bcftools: concordance exceeded 99.999%, with the 2-8 mismatches per sample confined to multi-allelic positions where position-based joining during comparison could not distinguish co-located biallelic records.

Incremental sample addition extends the packed genotype arrays of all existing Variant nodes on the target chromosome by appending new sample genotypes at the end of each array, without re-processing or altering the genotypes of previously imported samples; the `packed_index` assigned to each sample at import time is immutable and determines its bit offset in all packed arrays.

---

## Supplementary Note 2. Export formats

GraphMana exports to 17 population genetics file formats, classified by access path:

### FAST PATH formats (read pre-computed population arrays, constant time in N)

| Format | Tool/Use | Key options |
|--------|----------|-------------|
| TreeMix | TreeMix (population tree inference) | `--populations` |
| SFS dadi | dadi (demographic inference) | `--sfs-populations`, `--sfs-projection`, `--sfs-polarized` |
| SFS fastsimcoal2 | fastsimcoal2 (demographic simulation) | `--sfs-populations`, `--sfs-projection` |
| BED | bedtools, IGV (variant positions) | `--bed-extra-columns` |
| TSV | General analysis (variant tables) | `--tsv-columns` |
| JSON | Programmatic access (JSON Lines) | `--json-fields`, `--json-pretty` |

### FULL PATH formats (unpack per-sample genotypes, linear in N)

| Format | Tool/Use | Key options |
|--------|----------|-------------|
| VCF/BCF | bcftools, GATK, general (genotype calls) | `--vcf-version`, `--output-type`, `--phased` |
| PLINK 1.9 | PLINK (association studies, .bed/.bim/.fam) | Biallelic SNPs only |
| PLINK 2.0 | PLINK2 (association studies, .pgen/.pvar/.psam) | All variant types |
| EIGENSTRAT | smartPCA, AdmixTools (PCA, admixture, .geno/.snp/.ind) | Biallelic SNPs only |
| Beagle | Beagle (phasing, imputation) | Requires `--fidelity full` for PL field |
| STRUCTURE | STRUCTURE (population assignment) | `--structure-format [onerow\|tworow]` |
| Genepop | Genepop (conservation genetics) | — |
| Haplotype | selscan (selection scans, .hap/.map) | Phased data only |
| BGEN | BGEN (UK Biobank format, probabilistic) | Layout 2, zlib compression |
| GDS | SeqArray/R (HDF5-based) | Optional h5py dependency |
| Zarr | sgkit/Python (chunked array) | `--zarr-chunk-size` |

All formats support the full set of export filters: `--populations`, `--chromosomes`, `--region`, `--filter-variant-type`, `--filter-maf-min/max`, `--filter-min-call-rate`, `--filter-cohort`, `--filter-sample-list`, `--filter-consequences`, `--filter-impacts`, `--filter-genes`, `--filter-cadd-min/max`, `--filter-annotation-version`, and `--recalculate-af`.

---

## Supplementary Figure 2. Export manifest example

Each export automatically generates a `.manifest.json` sidecar file alongside the output.
The example below shows the manifest produced by exporting European-population EIGENSTRAT
data from chromosome 22 with a MAF filter:

Command: `graphmana export --format eigenstrat --populations CEU --populations GBR --populations FIN --populations IBS --populations TSI --chromosomes chr22 --filter-maf-min 0.05 --output european_cohort.eigenstrat`

Manifest (`european_cohort.eigenstrat.manifest.json`):

```json
{
  "graphmana_version": "1.0.0-dev",
  "timestamp": "2026-03-30T14:22:07.831042+00:00",
  "output_file": "european_cohort.eigenstrat",
  "format": "eigenstrat",
  "n_variants": 439609,
  "n_samples": 633,
  "chromosomes": ["chr22"],
  "filters": {
    "populations": ["CEU", "GBR", "FIN", "IBS", "TSI"],
    "maf_min": 0.05,
    "chromosomes": ["chr22"]
  },
  "recalculate_af": true,
  "threads": 1
}
```

The manifest records the exact software version, timestamp, output format, variant and sample
counts, and all active filter parameters. This enables any collaborator to verify or reproduce
the extraction without access to the original data manager's notes or scripts.

---

## Supplementary Table 4. Downstream tool compatibility validation

Exported from GraphMana 1KGP database (chromosome 22, 3,202 samples, 1,066,557 variants).

### Tier 1: Fully validated (downstream tool successfully consumed output)

| Format | Path | Downstream Tool | Variants | Samples | Result |
|--------|------|-----------------|----------|---------|--------|
| VCF (BGZF) | FULL | bcftools 1.19 stats | 1,035,839 | 3,202 | PASS |
| PLINK 1.9 | FULL | PLINK v1.9.0-b.8 --freq | 439,609 | 3,202 | PASS |
| PLINK 1.9 | FULL | PLINK v2.0.0 --freq | 439,609 | 3,202 | PASS |
| EIGENSTRAT | FULL | EIGENSOFT convertf v8.0 | 925,730 | 3,202 | PASS |
| TreeMix | FAST | TreeMix v1.13 (-k 500) | 1,066,557 | 26 pops | PASS |
| SFS dadi (.fs) | FAST | format spec | 1,066,557 | 2 pops | PASS |
| SFS fsc (.obs) | FAST | format spec | 1,066,557 | 2 pops | PASS |

### Tier 2: Format-specification validated

| Format | Path | Validation | Result |
|--------|------|-----------|--------|
| BED | FAST | 3+ tab-separated columns, 0-based | PASS (1,066,557 entries) |
| TSV | FAST | Header + consistent column count | PASS (7 columns) |
| JSON Lines | FAST | Each line valid JSON | PASS (1,066,557 records) |
| PLINK 2.0 | FULL | Format structure check | PASS |
| STRUCTURE | FULL | Genotype matrix format | PASS |

### Tier 3: Functional exports (pending external tool validation)

| Format | Path | Status |
|--------|------|--------|
| Beagle | FULL | Export functional |
| Genepop | FULL | Export functional |
| Haplotype | FULL | Export functional |
| BGEN | FULL | Export functional |
| GDS | FULL | Export functional |
| Zarr | FULL | Export functional |

Notes:
- PLINK 1.9 export includes biallelic SNPs only (439,609 of 1,066,557 variants)
- EIGENSTRAT export includes biallelic SNPs + indels (925,730 variants)
- TreeMix validation ran full phylogenetic tree estimation; topology matches known 1KGP population structure
- VCF roundtrip concordance exceeded 99.999% on 897,645 biallelic SNPs

---

## Supplementary Table 5. Correctness validation matrix

### Variant type support

| Variant Type | Import | Storage | VCF Export | PLINK | EIGENSTRAT |
|-------------|--------|---------|------------|-------|------------|
| Biallelic SNP | PASS | PASS | PASS | PASS | PASS |
| Biallelic indel | PASS | PASS | PASS | excluded | PASS |
| Multi-allelic (decomposed) | PASS | PASS | PASS (reconstructed) | excluded | excluded |
| Structural variant | PASS | PASS | PASS | excluded | excluded |
| Breakend (BND) | PASS | PASS | PASS | excluded | excluded |

### Genotype state fidelity

| State | 2-bit Code | Import | VCF Roundtrip | PLINK | EIGENSTRAT |
|-------|-----------|--------|---------------|-------|------------|
| Homozygous ref (0/0) | 00 | PASS | PASS | PASS | PASS |
| Heterozygous (0/1) | 01 | PASS | PASS | PASS | PASS |
| Homozygous alt (1/1) | 10 | PASS | PASS | PASS | PASS |
| Missing (./.) | 11 | PASS | PASS | PASS | PASS |

### Phasing and ploidy

| Feature | Import | Storage | Export | Notes |
|---------|--------|---------|--------|-------|
| Phased genotypes | PASS | phase_packed | PASS | Per-sample per-variant |
| Unphased genotypes | PASS | phase_packed | PASS | Default |
| Diploid | PASS | ploidy_packed | PASS | Default |
| Haploid | PASS | ploidy_packed | PASS | e.g., chrX males |

### Annotation layer independence

| Operation | Genotype affected? | Verified |
|-----------|-------------------|----------|
| Add VEP annotations | No | PASS |
| Update ClinVar version | No | PASS |
| Remove annotation version | No | PASS |
| Load BED regions | No | PASS |

### Test suite coverage (162 relevant tests, all passing)

| Category | Tests |
|----------|-------|
| Genotype packing/unpacking | 33 |
| Phase convention | 8 |
| Byte boundary edge cases | 6 |
| Missing data propagation | 5 |
| Incremental array extension | 13 |
| Export return dict contract | 35 |
| Export integration (mock Neo4j) | 35 |
| Soft-delete sample exclusion | 12 |
| Population array consistency | 9 |
| CSV byte round-trip | 6 |
