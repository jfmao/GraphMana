# Supplementary Table 4: Downstream Tool Compatibility Validation

Exported from GraphMana 1KGP database (chromosome 22, 3,202 samples, 1,066,557 variants).
Each format was exported and loaded into its target downstream tool.

## Tier 1: Fully validated (downstream tool successfully consumed output)

| Format | Access Path | Downstream Tool | Variants | Samples | Validation Result |
|--------|------------|-----------------|----------|---------|-------------------|
| VCF (BGZF) | FULL | bcftools 1.19 stats | 1,035,839 | 3,202 | PASS — bcftools reads, reports 914,150 SNPs + 121,689 indels |
| PLINK 1.9 (.bed/.bim/.fam) | FULL | PLINK v1.9.0-b.8 --freq | 439,609 | 3,202 | PASS — frequency file generated for all biallelic SNPs |
| PLINK 1.9 (.bed/.bim/.fam) | FULL | PLINK v2.0.0-a.6.5LM --freq | 439,609 | 3,202 | PASS — PLINK 2 reads PLINK 1.9 files and computes frequencies |
| EIGENSTRAT (.geno/.snp/.ind) | FULL | EIGENSOFT convertf (v8.0) | 925,730 | 3,202 | PASS — convertf reads and converts to PACKEDANCESTRYMAP |
| TreeMix (gzipped) | FAST | TreeMix v1.13 (-k 500 -root YRI) | 1,066,557 | 26 pops | PASS — phylogenetic tree estimated successfully |
| SFS dadi (.fs) | FAST | format spec validation | 1,066,557 | 2 pops (YRI, CEU) | PASS — valid .fs file with comment, dimensions, values, mask lines |
| SFS fastsimcoal2 (.obs) | FAST | format spec validation | 1,066,557 | 2 pops (YRI, CEU) | PASS — valid .obs file with "1 observations" header |

## Tier 2: Format-specification validated (output verified against published format specs)

| Format | Access Path | Validation Method | Result |
|--------|------------|-------------------|--------|
| BED | FAST | Tab-separated, 0-based half-open coordinates, 3+ columns per line | PASS — 1,066,557 entries, 0 malformed |
| TSV | FAST | Header + tab-separated data, consistent column count | PASS — 7 columns, 1,066,557 data rows |
| JSON Lines | FAST | Each line valid JSON (json.loads) | PASS — 1,066,557 records, 0 invalid |
| PLINK 2.0 (.pgen/.pvar/.psam) | FULL | Format structure check | Exported successfully |
| STRUCTURE | FULL | Genotype matrix format check | Exported successfully |

## Tier 3: Functional exports (tested internally, not yet validated against external tool pipelines)

| Format | Access Path | Status |
|--------|------------|--------|
| Beagle | FULL | Export functional; downstream tool validation pending |
| Genepop | FULL | Export functional; downstream tool validation pending |
| Haplotype (.hap/.map) | FULL | Export functional; downstream tool validation pending |
| BGEN | FULL | Export functional; downstream tool validation pending |
| GDS (SeqArray) | FULL | Export functional; downstream tool validation pending |
| Zarr (sgkit) | FULL | Export functional; downstream tool validation pending |

## Notes

- VCF roundtrip concordance exceeded 99.999% (897,645 biallelic SNPs, 5 samples; see Correctness Validation).
- PLINK export includes biallelic SNPs only (439,609 of 1,066,557 variants); indels and multi-allelic sites are excluded per PLINK 1.9 format requirements.
- EIGENSTRAT export includes 925,730 variants (biallelic SNPs + biallelic indels).
- TreeMix validation ran a full phylogenetic tree estimation with 500-SNP blocks; the resulting tree topology matches known 1KGP population structure (YRI as outgroup, continental clustering).
- SFS exports used sub-population names (YRI, CEU) with projection sizes of 20; folded spectrum.
