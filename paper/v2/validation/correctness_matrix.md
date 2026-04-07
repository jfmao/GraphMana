# Supplementary Table 5: Correctness Validation Matrix

Systematic verification of data fidelity across variant types, genotype states, and export formats.
Based on 1,439 unit/integration tests + benchmark roundtrip validation.

## Variant Type Support

| Variant Type | Import | Storage | VCF Export | PLINK Export | EIGENSTRAT Export | Notes |
|-------------|--------|---------|------------|-------------|-------------------|-------|
| Biallelic SNP | PASS | PASS | PASS | PASS | PASS | Primary supported type |
| Biallelic indel | PASS | PASS | PASS | excluded | PASS | PLINK 1.9 excludes indels by design |
| Multi-allelic (decomposed) | PASS | PASS | PASS (reconstructed) | excluded | excluded | Decomposed to K biallelic nodes; VCF reconstructs multi-allelic lines |
| Structural variant (DEL/DUP/INV/INS/CNV) | PASS | PASS | PASS | excluded | excluded | Stored with sv_type, sv_len, sv_end; genotype encoding identical to SNPs |
| Breakend (BND) | PASS | PASS | PASS | excluded | excluded | Imported as independent nodes; no mate-pair linking |

## Genotype State Fidelity

| Genotype State | 2-bit Code | Import | Roundtrip (VCF) | PLINK | EIGENSTRAT | Notes |
|----------------|-----------|--------|-----------------|-------|------------|-------|
| Homozygous reference (0/0) | 00 | PASS | PASS | PASS | PASS (code 2) | |
| Heterozygous (0/1, 1/0) | 01 | PASS | PASS | PASS | PASS (code 1) | Phase preserved separately |
| Homozygous alternate (1/1) | 10 | PASS | PASS | PASS | PASS (code 0) | |
| Missing (./.) | 11 | PASS | PASS | PASS | PASS (code 9) | |

## Phasing Fidelity

| State | Import | Storage | VCF Export (--phased) | Notes |
|-------|--------|---------|----------------------|-------|
| Phased (|) | PASS | phase_packed bit=1 | PASS | Phase convention: bit=1 means ALT on second haplotype |
| Unphased (/) | PASS | phase_packed bit=0 | PASS | Default output when --phased not specified |
| Mixed phasing | PASS | Per-variant per-sample | PASS | Each sample can be independently phased/unphased |

## Ploidy Support

| State | Import | Storage | Export | Notes |
|-------|--------|---------|--------|-------|
| Diploid | PASS | ploidy_packed bit=0 | PASS | Default; all standard VCF genotypes |
| Haploid (e.g., chrX males) | PASS | ploidy_packed bit=1 | PASS | Stored as haploid; exported correctly per format |
| Mixed ploidy | PASS | Per-sample per-variant | PASS | ploidy_packed tracks per-sample ploidy state |

## Population Array Consistency

| Property | Validation | Notes |
|----------|-----------|-------|
| pop_ids[] length = K | PASS (db validate) | Verified by structural safeguard tests |
| ac[] length = K | PASS | Consistent with pop_ids on all variants |
| an[] length = K | PASS | Consistent with pop_ids |
| af[] length = K | PASS | af = ac/an, verified |
| het_count[] length = K | PASS | |
| hom_alt_count[] length = K | PASS | |
| ac_total = sum(ac[]) | PASS | Cross-checked during export |
| an_total = sum(an[]) | PASS | Cross-checked during export |

## Annotation Layer Independence

| Operation | Genotype Layer Affected? | Verified |
|-----------|-------------------------|----------|
| Add VEP annotations | No | PASS — gt_packed unchanged before/after |
| Update ClinVar version | No | PASS — only HAS_CONSEQUENCE edges modified |
| Remove annotation version | No | PASS — genotype arrays unmodified |
| Load BED regions | No | PASS — creates IN_REGION edges only |

## Sample Order Consistency

| Test | Result | Notes |
|------|--------|-------|
| packed_index immutability | PASS | packed_index assigned at import, never changes |
| Sample order in VCF export | PASS | Matches packed_index order |
| Sample order in PLINK .fam | PASS | Matches packed_index order |
| Sample order in EIGENSTRAT .ind | PASS | Matches packed_index order |
| Sample order after incremental add | PASS | New samples appended; existing unchanged |

## VCF Roundtrip Concordance

| Dataset | Variants | Samples | Concordance | Mismatches | Source |
|---------|----------|---------|-------------|------------|--------|
| 1KGP chr22 (5 samples) | 897,645 | 5 | 99.999%+ | 2-8 per sample | Multi-allelic position ambiguity |
| 1KGP chr22 (3,202 samples) | 1,035,839 | 3,202 | bcftools reads OK | N/A (read validation only) | bcftools stats confirms structure |

## Test Suite Coverage

| Category | Tests | Status |
|----------|-------|--------|
| Genotype packing/unpacking | 33 | PASS |
| Phase convention | 8 | PASS |
| Byte boundary edge cases (4, 8 samples) | 6 | PASS |
| Missing data propagation | 5 | PASS |
| Incremental array extension | 13 | PASS |
| Export return dict contract | 35 | PASS |
| Export integration (mock Neo4j) | 35 | PASS |
| Soft-delete sample exclusion | 12 | PASS |
| Population array consistency | 9 | PASS |
| CSV byte round-trip (signed Java bytes) | 6 | PASS |
| **Total relevant tests** | **162** | **All passing** |
