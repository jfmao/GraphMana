# GraphMana — Brief Communication Outline

**Target:** Nature Methods, Brief Communication
**Limits:** Abstract ≤70 words, Main text ≤1,600 words (no sections/subheadings), ≤2 figures, ≤20 references
**Updated:** 2026-03-31

---

## Title

GraphMana: graph-native data management for population genomics projects

## Abstract (~70 words)

Population genomics projects rely on fragmented file-based workflows that lose
provenance and require full reprocessing when samples are added. GraphMana
stores variant data in a Neo4j graph as packed genotype arrays with
pre-computed population statistics, enabling incremental sample addition,
provenance tracking, cohort management, and export to 17 formats. Applied to
the 1000 Genomes Project (3,202 samples, 70.7 million variants), GraphMana
completes a full project lifecycle otherwise requiring ad hoc scripting.

## Current Codebase Statistics

- 21,267 lines Python + 2,864 lines Java
- 58 CLI commands, 17 export formats
- 1,439 unit tests passing
- 11 vignettes, 65 command reference pages
- Demonstration: 1KGP whole-genome (70.7M variants, 3,202 samples, 26 populations, 166 GB database)

## Benchmark Numbers (publication-ready)

### Initial import
| Step | Time |
|------|------|
| prepare-csv (22 VCFs, 2,500 samples, 16 threads) | 95 min |
| neo4j-admin import (214 GB CSV → 166 GB DB) | 3 min |

### Incremental addition (234 samples to 70.7M-variant DB)
| Approach | Time |
|----------|------|
| CSV-to-CSV rebuild | 182 min |

### Export benchmarks (chr22, 1.07M variants, 3,202 samples)

| Format | Path | Variants | Time | Size |
|--------|------|----------|------|------|
| TreeMix | FAST | 1.07M | 131s | 12 MB |
| SFS dadi | FAST | 1.07M | 86s | 5.7 KB |
| BED | FAST | 1.07M | 84s | — |
| TSV | FAST | 1.07M | 85s | — |
| VCF | FULL | 1.04M | 473s | 208 MB |
| PLINK 1.9 | FULL | 440K | 22s | 336 MB |
| EIGENSTRAT | FULL | 926K | 192s | 2.8 GB |

### Whole-genome exports (70.7M variants)

| Format | Path | Time |
|--------|------|------|
| TreeMix | FAST | 102 min |
| SFS dadi | FAST | 98 min |
| BED | FAST | 103 min |
| TSV | FAST | 101 min |
| PLINK 1.9 (8 threads) | FULL | 156s |
| VCF | FULL | 3.7 hr |
| EIGENSTRAT | FULL | 3.7 hr |

### GraphMana vs bcftools (chr22)

| Benchmark | GraphMana | bcftools | Note |
|-----------|-----------|----------|------|
| Incremental add (3 × 234) | 1,254s | 374s | bcftools faster per-op |
| Cohort VCF (5 pops) | 191–571s | 49–110s | bcftools faster per-op |
| Multi-format export | 6 formats | 1 format | GraphMana: 6× breadth |
| Annotation update (53K BED) | **3.5s** | 96s | **GraphMana 27× faster** |
| Full lifecycle (46 ops) | 98 min / 46 ops | 17 min / 17 ops | GraphMana: 2.7× more ops |

## Main Text Structure (~1,500 words, no section headings)

### Opening: The problem (≤300 words)
- Medium-size population genomics projects (100–10,000 samples) face infrastructure, not compute, challenges
- Five daily pain points:
  1. Adding samples invalidates all downstream results → full reprocessing
  2. Results scattered across tools, directories, and file formats
  3. Provenance lost (parameters, versions, timestamps, sample lists)
  4. Sharing subsets requires manual export + coordinate matching
  5. No single queryable record of the project's analytical state
- These are project lifecycle problems, not statistical problems
- GraphPop (companion paper) solves graph-native computation; GraphMana solves data management

### Middle: What GraphMana does (≤800 words)
- **Storage architecture:** Neo4j graph database; Variant nodes carry packed genotype arrays (2 bits/sample) and pre-computed population-level allele count arrays; 125× smaller than per-sample edges
- **Two access paths:** FAST PATH (reads pop arrays, constant time in N) for TreeMix/SFS/BED; FULL PATH (unpacks genotypes, linear in N) for VCF/PLINK/EIGENSTRAT
- **Incremental import:** New samples appended to packed arrays without re-processing existing data; CSV-to-CSV rebuild for whole-genome scale
- **Provenance:** Every ingestion logged with parameters, timestamps, software version, source files
- **Multi-format export:** 17 formats from a single database (VCF, PLINK 1.9/2.0, EIGENSTRAT, TreeMix, SFS dadi/fsc, Beagle, STRUCTURE, Genepop, BED, haplotype, BGEN, GDS, Zarr, JSON, TSV)
- **Cohort management:** Named cohorts defined as Cypher queries over the graph — no file extraction needed
- **Variant representation:** Multi-allelic sites decomposed into biallelic nodes with 1-based allele_index; structural variants stored uniformly; VCF roundtrip fidelity >99.999%
- **Annotation versioning:** Updatable annotation layers (VEP, ClinVar, CADD, GO, pathways) with version tracking; in-place updates 27× faster than VCF rewrite
- **Cluster deployment:** Two-step split (prepare-csv on any node, load-csv on Neo4j host); SLURM/PBS scripts provided

### Closing: Demonstration + impact (≤400 words)
- 1KGP demonstration: 3,202 samples, 70.7M variants, 26 populations
- Full lifecycle benchmark: import → incremental add → annotation → cohort extraction → 6-format export → QC
- GraphMana completes 46 operations; equivalent bcftools workflow completes 17 (lacks multi-format export, annotation update, cohort management)
- Annotation update 27× faster (in-place graph vs VCF rewrite)
- Honest limitations: per-operation VCF export 3–5× slower than bcftools; sweet spot 100–10,000 samples; whole-genome incremental takes hours
- Pre-built databases deposited for community use
- Open source (MIT license), cross-platform, community-edition Neo4j (free)

## Figure 1: Architecture and Access Paths
- Panel a: Graph schema overview (Variant, Sample, Population, Chromosome, Gene nodes + relationships)
- Panel b: Packed genotype encoding (2 bits/sample) + two access paths (FAST vs FULL)
- Panel c: Incremental sample addition — new genotypes appended, existing unchanged

## Figure 2: 1KGP Benchmark (incorporates former Table 1)
- Panel a: Lifecycle simulation — stacked bars: GraphMana 46 ops vs bcftools 17/26 ops (9 grey = no equivalent)
- Panel b: Head-to-head task comparison — grouped bars for incremental add, cohort extraction, annotation update (27x highlight), multi-format export (6 vs 1)
- Panel c: Export format breadth — 17 formats grouped by access path (FAST/FULL)

## Display items: 2 figures, 0 tables (within ≤3 display item limit)

## References (~15)
1. GraphPop (companion paper)
2. Neo4j (graph database)
3. 1000 Genomes Project (Byrska-Bishop et al. 2022)
4. Hail (Goldstein et al.)
5. PLINK 1.9 (Purcell et al. 2007; Chang et al. 2015)
6. bcftools (Danecek et al. 2021)
7. scikit-allel (Miles et al.)
8. cyvcf2 (Pedersen & Quinlan 2017)
9. VCF specification (Danecek et al. 2011)
10. dadi (Gutenkunst et al. 2009)
11. fastsimcoal2 (Excoffier et al. 2021)
12. TreeMix (Pickrell & Pritchard 2012)
13. EIGENSTRAT/AdmixTools (Patterson et al. 2006)
14. Beagle (Browning & Browning 2007)
15. STRUCTURE (Pritchard et al. 2000)

---

## Differentiation from GraphPop Article

| Aspect | GraphPop (Article) | GraphMana (Brief Communication) |
|--------|-------------------|-------------------------------|
| Core question | Can we compute differently? | Can we manage projects differently? |
| Thesis | Graph-native computation reveals new biology | Persistent analytical record as single source of truth |
| Content | 12 procedures, benchmarks, biological findings | Import, QC, provenance, export, cohort management |
| Figures | Architecture, benchmarks, human + rice biology | Architecture, 1KGP lifecycle benchmark |
| Dataset | 1KGP + Rice 3K | 1KGP (3,202 samples, 70.7M variants) |
| Pre-built DBs | Mentioned in Data Availability | Core deliverable |
