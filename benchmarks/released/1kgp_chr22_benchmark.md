# 1KGP chr22 Benchmark Results

**Date**: 2026-03-16
**Dataset**: 1000 Genomes Project High Coverage (GRCh38), chromosome 22
**Variants**: 1,066,557 (SNPs + INDELs + SVs)
**Samples**: 3,202 (26 populations, 5 superpopulations)
**VCF size**: 426 MB (bgzipped)

## System

| Resource | Value |
|----------|-------|
| CPU | Intel i9-13900K (32 threads) |
| RAM | 56 GB DDR5 |
| Storage | 3.7 TB NVMe SSD (via WSL2 on Windows) |
| OS | Ubuntu 22.04 on WSL2 (Linux 6.6.87) |
| Neo4j | 2026.01.4 Community (16 GB heap, 16 GB pagecache) |
| Java | OpenJDK 21.0.10 |
| Python | 3.11 (cyvcf2 0.32.1, numpy) |

## Import Pipeline

| Step | Time | Notes |
|------|------|-------|
| **prepare-csv** (VCF → CSV) | **818.5s** (13.6 min) | 16 threads, parallel-by-chromosome |
| **neo4j-admin import** (CSV → Neo4j) | **58.0s** (1.0 min) | 32 threads, 1.07M nodes + 2.14M relationships |
| **Total import** | **876.5s** (14.6 min) | End-to-end: raw VCF to queryable graph |

### CSV Output
- variant_nodes.csv: 2.8 GB
- next_edges.csv: 50 MB
- on_chromosome_edges.csv: 42 MB
- sample_nodes.csv: 522 KB
- Other CSVs: < 1 MB
- **Total CSV**: 2.9 GB

### Neo4j Database
- Variant nodes: 1,066,555
- Sample nodes: 3,202
- Population nodes: 5
- Chromosome nodes: 1
- VCFHeader nodes: 1
- NEXT edges: 1,066,556
- ON_CHROMOSOME edges: 1,066,557
- IN_POPULATION edges: 3,202

## Export Benchmarks

### FAST PATH (pre-computed population arrays, no genotype unpacking)

| Format | Time (s) | Output Size | Notes |
|--------|----------|-------------|-------|
| SFS (dadi) | **0.4** | < 1 KB | 2-pop projection, instantaneous |
| SFS (fastsimcoal2) | **0.4** | < 1 KB | 2-pop .obs files, instantaneous |
| TreeMix | **112.1** | 3.6 MB | Allele count matrix, gzipped |
| BED | **104.9** | 45 MB | Variant positions |
| TSV | **106.6** | 56 MB | Default columns |
| JSON | **116.5** | 181 MB | JSON Lines, all variant fields |

### FULL PATH (genotype unpacking from packed arrays)

| Format | Time (s) | Output Size | Notes |
|--------|----------|-------------|-------|
| VCF | **1016.2** (16.9 min) | 13 GB | Full genotype roundtrip |
| PLINK 1.9 | **177.3** (3.0 min) | 708 MB | .bed/.bim/.fam, SNPs only (925,730) |
| EIGENSTRAT | **382.1** (6.4 min) | 2.8 GB | .geno/.snp/.ind, SNPs only |

### Throughput

| Operation | Variants/sec |
|-----------|-------------|
| prepare-csv (VCF parsing + packing) | ~1,303 |
| neo4j-admin import | ~18,389 |
| FAST PATH export (streaming) | ~9,500 |
| VCF export (full unpack) | ~1,049 |
| PLINK export (full unpack, SNP filter) | ~6,015 |
| EIGENSTRAT export (full unpack, SNP filter) | ~2,791 |

## Key Observations

1. **SFS exports are truly instantaneous** (0.4s) — they aggregate pre-computed population
   arrays (ac[], an[]) and return a small matrix. This demonstrates the FAST PATH advantage.

2. **FAST PATH variant-streaming exports** (TreeMix, BED, TSV, JSON) take ~105-117s.
   The bottleneck is Neo4j streaming 1.07M nodes with their properties, not the export
   logic itself. These read only population-level arrays, not per-sample genotypes.

3. **FULL PATH exports** scale with output size. VCF is slowest (13 GB output) because it
   writes per-sample genotype strings for 3,202 samples. PLINK is fastest because its
   binary .bed format is compact.

4. **Import is dominated by VCF parsing**, not Neo4j loading. prepare-csv (818.5s) dwarfs
   neo4j-admin import (58.0s). The parsing includes genotype packing (2-bit encoding for
   3,202 samples per variant) and allele count computation.

5. **Storage efficiency**: The 426 MB compressed VCF becomes 2.9 GB of CSV, which loads into
   a Neo4j database of approximately 2.1 GB. The packed arrays (801 bytes gt_packed per
   variant for 3,202 samples) are comparable to PLINK BED density.
