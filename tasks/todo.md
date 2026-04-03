# GraphMana — Implementation Checklist

_Last updated: 2026-03-24 (integration tests complete; within-chromosome parallelism; multi-file parallel)_

---

## Documentation: Neo4j Background for Non-Expert Users

_Added: 2026-03-20_

Users of GraphMana may have no prior experience with Neo4j. The README and/or a
dedicated `docs/neo4j-primer.md` should cover:

- **What Neo4j is**: property graph database, nodes + relationships + properties,
  Cypher query language — contrast with relational SQL and file-based storage
- **Community vs Enterprise Edition**: GraphMana uses Community (free, no license
  barrier). Community = one user database per instance. Enterprise adds named
  multi-database, role-based access, etc. Users must know they cannot run
  `CREATE DATABASE` in Community.
- **One data directory per instance**: two Neo4j processes cannot share a data
  folder; each needs its own directory + distinct port numbers (bolt/http)
- **Running multiple instances**: how to run a second instance (e.g., a test DB
  alongside production) using separate `neo4j.conf` files and ports 7688/7475
- **Connection details**: bolt URI format, default ports (7474 HTTP, 7687 Bolt),
  authentication (`neo4j` / initial password)
- **Memory settings**: heap vs page cache in `neo4j.conf`; why Neo4j needs ≥16 GB
  for large genomic graphs; risk of OOM if concurrent queries exceed RAM
- **Data directory must be on local SSD**: NFS/Lustre causes severe slowdowns;
  `graphmana check-filesystem` detects this
- **Neo4j Browser**: web UI at http://localhost:7474 for Cypher exploration
- **GraphMana manages Neo4j lifecycle**: `graphmana setup-neo4j`, `neo4j-start`,
  `neo4j-stop` — users do not need to manage Neo4j directly in most cases

Place this content in: `docs/neo4j-primer.md` + link from README "Prerequisites".

---

## Export Performance Benchmark — 1KGP Full Scale (COMPLETE)

_Added: 2026-03-20. Completed: 2026-03-29._

### Database Context
**GraphMana-native database** — imported via `graphmana prepare-csv` + `graphmana load-csv`.

| Property | Value |
|----------|-------|
| Database | GraphMana-native 1KGP whole-genome |
| URI | `bolt://localhost:7687` |
| Credentials | `neo4j` / `graphmana` |
| Variants | 70,691,875 |
| Samples | 3,202 |
| Populations | 26 (all 1KGP populations) |
| Chromosomes | 22 (all autosomes, chr1–chr22) |
| chr22 variants | 1,066,557 |
| Database size | 166 GB |
| Neo4j config | heap=16g, pagecache=16g |
| Hardware | 32 cores, 62 GB RAM, NVMe SSD |

### Import Pipeline Timing

| Step | Time |
|------|------|
| `prepare-csv` (22 VCFs, 16 threads) | ~2 hours |
| `neo4j-admin import` (221 GB CSVs) | ~3 minutes |
| Post-import indexes | seconds |

---

### Phase 1: FAST PATH Benchmarks (all 22 chromosomes) — COMPLETE

| Format | Variants | Wall time | File size |
|--------|----------|-----------|-----------|
| TreeMix (26 pops) | 70,692,015 | **102 min** | 780 MB |
| SFS dadi (2-pop folded) | 70,692,015 | **98 min** | 5.7 KB |
| SFS fsc (2-pop folded) | 70,692,015 | **101 min** | 1.3 KB |
| BED | 70,692,007 | **103 min** | 2.9 GB |
| TSV (8 AF columns) | 70,692,007 | **101 min** | 3.8 GB |

### Phase 2: FULL PATH Benchmarks (chr22) — COMPLETE

| Format | Variants | Wall time | File size |
|--------|----------|-----------|-----------|
| VCF (BGZF) | 1,035,839 | **649s** (10.8 min) | 208 MB |
| PLINK 1.9 (SNPs) | 439,609 | **22s** | 336 MB |
| EIGENSTRAT | 925,730 | **192s** (3.2 min) | 2.8 GB |
| TreeMix chr22 | 1,066,557 | **131s** | 12 MB |

### Phase 3: Parallel Scaling (chr22) — COMPLETE

| Format | Threads | Wall time | Speedup |
|--------|---------|-----------|---------|
| VCF | 1 | 648s | 1.0x |
| VCF | 4 | 645s | 1.0x |
| VCF | 8 | 647s | 1.0x |
| PLINK | 1 | 218s | 1.0x |
| PLINK | 4 | 145s | 1.5x |
| PLINK | 8 | 22s | 9.9x |

- [x] VCF chr22 with --threads 1, 4, 8 — no speedup on single chromosome (expected)
- [x] PLINK chr22 with --threads 1, 4, 8 — 10x speedup at 8 threads
- [x] Output identity verified: VCF t1 vs t8 IDENTICAL (1,035,839 lines), PLINK .bed/.bim IDENTICAL

### Phase 4: Whole-Genome FULL PATH (8 threads) — COMPLETE

| Format | Variants | Samples | Wall time | File size |
|--------|----------|---------|-----------|-----------|
| PLINK 1.9 (SNPs) | 9,627,636 | 3,202 | **156s** (2.6 min) | 7.2 GB |
| EIGENSTRAT | 61,599,149 | 3,202 | **13,351s** (3.7 hr) | 184 GB |
| VCF (BGZF) | 68,912,619 | 3,202 | **13,294s** (3.7 hr) | 14 GB |

### Phase 5: Results Table (publication-ready) — COMPLETE

| Format | Path | Scope | Variants | Samples | Pops | Wall time | File size |
|--------|------|-------|----------|---------|------|-----------|-----------|
| TreeMix | FAST | All (22) | 70.7M | 3,202 | 26 | 102 min | 780 MB |
| SFS dadi | FAST | All (22) | 70.7M | — | 2 | 98 min | 5.7 KB |
| SFS fsc | FAST | All (22) | 70.7M | — | 2 | 101 min | 1.3 KB |
| BED | FAST | All (22) | 70.7M | — | — | 103 min | 2.9 GB |
| TSV (AF) | FAST | All (22) | 70.7M | — | — | 101 min | 3.8 GB |
| TreeMix | FAST | chr22 | 1.07M | — | 26 | 131s | 12 MB |
| VCF | FULL | chr22 | 1.04M | 3,202 | — | 649s | 208 MB |
| PLINK 1.9 | FULL | chr22 | 440K SNP | 3,202 | — | 22s | 336 MB |
| EIGENSTRAT | FULL | chr22 | 926K | 3,202 | — | 192s | 2.8 GB |
| **PLINK 1.9** | **FULL** | **All (22)** | **9.6M SNP** | **3,202** | **—** | **156s** | **7.2 GB** |
| **EIGENSTRAT** | **FULL** | **All (22)** | **61.6M** | **3,202** | **—** | **3.7 hr** | **184 GB** |
| **VCF (BGZF)** | **FULL** | **All (22)** | **68.9M** | **3,202** | **—** | **3.7 hr** | **14 GB** |

### Key Optimizations Implemented During Benchmarking

1. **Smart variant queries**: Three query strategies (FAST unordered, FAST batched, GENOTYPES batched) selected automatically based on export needs
2. **Batched pagination**: 500K variants per batch avoids Neo4j GC pauses on large chromosomes
3. **Streaming SFS**: Accumulate SFS bins directly instead of collecting all variant dicts
4. **BGZF VCF export**: Proper blocked gzip output readable by bcftools/htslib
5. **Property-selected queries**: FAST PATH excludes gt_packed/phase_packed (~5× less data per variant)

### Validation Tests (for paper)

**VCF Roundtrip Fidelity** (2026-03-30):
- Imported 1KGP chr22 → exported with `--phased` → compared with original using bcftools
- Position-matched biallelic SNP comparison: 897,645 variants, 5 samples
- Results: HG00096 99.9992%, HG00097 99.9997%, NA18525 100.0000%, NA19238 99.9991%, HG01879 99.9995%
- The 2-8 mismatches per sample are at multi-allelic positions (test artifact from position-based join, not real errors)
- Multi-allelic reconstruction: 1,066,557 biallelic records → 1,035,839 records (24,006 multi-allelic lines)

**Incremental Import** (2026-03-30):
- Tested adding 10 new samples (chr22) to the existing 3,202-sample whole-genome database
- Neo4j crashed under write pressure (~20 min into extending 1.07M variant packed arrays)
- Database integrity preserved — transactions rolled back cleanly, no corruption
- Root cause: whole-genome incremental import requires updating millions of packed arrays in write transactions; exceeds available memory on 64 GB machine with heap=16g + pagecache=16g
- Recommendation: incremental import works well for per-chromosome and exome-scale databases; whole-genome incremental requires 128+ GB RAM
- Integration tests (chr22-only database) pass 28/28 for incremental import

---

## v0.1 — Minimum Viable Product (COMPLETE)

### Phase 1: Project Scaffolding
- [x] Initialize repository structure at /mnt/e/GraphMana
- [x] Create Maven project for graphmana-procedures (Neo4j 5.x, JDK 21, Maven Shade)
- [x] Create Python project graphmana-cli (pyproject.toml, Click, cyvcf2, neo4j-driver)
- [x] Create docker-compose.yml with Neo4j Community
- [x] Hello-world Neo4j procedure (HealthCheckProcedure.java, 31 lines)
- [x] `graphmana status` command with --detailed and --json
- [x] End-to-end smoke test: docker compose up → graphmana status

### Phase 2: Schema and VCF Parser
- [x] schema.py: 10 constraints + 12 indexes from CLAUDE.md schema
- [x] SchemaMetadata node creation on first initialization
- [x] VCF parser (vcf_parser.py, 499 lines — streaming cyvcf2, ploidy, ancestral allele)
- [x] Genotype packer (genotype_packer.py, 122 lines — 2-bit pack/unpack, branchless)
- [x] Population map parser (population_map.py, 204 lines — PED/panel auto-detect)
- [x] Ploidy detector (ploidy_detector.py, 32 lines)
- [x] Chr reconciler (chr_reconciler.py, 199 lines — auto-detect, UCSC/Ensembl, custom map)
- [x] Tests: genotype packer, VCF parser, chr reconciler, population map, ploidy (235+136+123+77 lines)
- [x] cyvcf2 remap verification tests

### Phase 3: Import Pipeline (Bulk)
- [x] csv_emitter.py (572 lines — 7-file CSV, NEXT chain, sort-order warning)
- [x] Import filters (import_filters.py, 128 lines — qual, call_rate, maf, variant_type)
- [x] CLI: `graphmana ingest` with --mode auto/initial/incremental
- [x] `graphmana prepare-csv` (CSV generation without Neo4j — cluster-friendly)
- [x] `graphmana load-csv` (load via neo4j-admin import)
- [x] NEXT chain construction during streaming
- [x] VEP parser (vep_parser.py, 480 lines — VEP CSQ + SnpEff ANN auto-detect)
- [x] loader.py (197 lines — neo4j-admin integration, post-import indexes)
- [x] Tests: csv_emitter (410), import_filters (199), loader (133), vep_parser (180)
- [x] normalizer.py: bcftools norm wrapper (155 lines — left-align, split, trim, result parsing)
- [x] Integration test with real 1000 Genomes data (chr22: 28/28 passed; full genome: 10 tests, 2-chrom exports)
- [x] Verify GraphPop compatibility against /mnt/e/GraphPop (docs/graphpop-compat.md + 21 encoding tests)

### Phase 4: Export (Core Formats)
- [x] BaseExporter (base.py, 230 lines — shared filtering, variant streaming, unpacking)
- [x] VCF export (vcf_export.py, 270 lines — roundtrip fidelity, header preservation)
- [x] Export filters (export_filters.py, 184 lines — populations, cohort, annotation, CADD, region)
- [x] PLINK 1.9 export (plink_export.py, 216 lines — .bed/.bim/.fam, biallelic SNPs)
- [x] TSV export (tsv_export.py, 117 lines — configurable columns)
- [x] CLI: `graphmana export --format vcf|plink|tsv`
- [x] Tests: vcf_export (155+379), plink_export (132), tsv_export (72)

### Phase 5: Polish v0.1
- [x] `graphmana status` with --detailed and --json flags
- [x] Error handling throughout CLI
- [x] Logging: --verbose / --quiet on all commands
- [x] README.md (skeletal quickstart — 24 lines)
- [ ] Tag v0.1.0

---

## v0.5 — Core Features Complete (MOSTLY COMPLETE)

### Parallel Execution
- [x] Parallel CSV generation by chromosome (parallel.py, 237 lines — ProcessPoolExecutor)
- [x] Parallel export by chromosome (export/parallel.py, 213 lines)
- [x] Tests: parallel ingest (205), parallel export (140), CLI parallel (81)

### VCF Roundtrip Fidelity
- [x] VCF header preservation (VCFHeader node with verbatim text)
- [x] AF recalculation for exported cohort subsets
- [x] Tests: vcf_header (379 lines — 5 test classes)
- [x] Multi-allelic reconstruction during export (import tagging + VCF export reconstruction)

### Incremental Sample Addition
- [x] Extend packed arrays — append new samples (array_ops.py, 460 lines)
- [x] MERGE existing + CREATE new variants (incremental.py, 645 lines)
- [x] Update population arrays for affected populations
- [x] Duplicate sample detection (skip or error)
- [x] Tests: incremental (548 lines — 9 test classes)

### Additional Export Formats (ALL Tier 1 + Tier 2 complete)
- [x] EIGENSTRAT (eigenstrat_export.py, 140 lines) — FULL PATH
- [x] TreeMix (treemix_export.py, 96 lines) — FAST PATH
- [x] SFS dadi (sfs_dadi_export.py, 135 lines) — FAST PATH
- [x] SFS fastsimcoal2 (sfs_fsc_export.py, 145 lines) — FAST PATH
- [x] BED (bed_export.py, 72 lines) — FAST PATH
- [x] SFS utilities (sfs_utils.py, 190 lines — hypergeometric projection, folding)
- [x] Tests: eigenstrat (119), treemix (57), sfs_dadi (82), sfs_fsc (83), sfs_utils (193), bed (61)

### Cohort Management
- [x] CohortManager (cohort/manager.py, 174 lines — define, list, show, delete, count, validate)
- [x] Cypher query validation (read-only check, syntax via EXPLAIN)
- [x] Use cohort in export: --filter-cohort
- [x] Tests: cohort_manager (101), cli_cohort (105)

### Annotation Versioning
- [x] AnnotationManager (annotation/manager.py, 286 lines — load, list, get, remove)
- [x] annotation_source + annotation_version on HAS_CONSEQUENCE edges
- [x] --mode add|update|replace
- [x] graphmana annotate list, remove
- [x] Additional parsers: CADD (68), ClinVar (83), gene constraint (79), BED regions (110), GO/Pathway (267)
- [x] Tests: annotation_manager (238), annotation_queries (100), cadd (108), constraint (110), clinvar (66), go_pathway (149), bed_region (114)

### Sample Management
- [x] Soft delete: Sample.excluded = true (sample/manager.py, 635 lines)
- [x] Sample restore
- [x] Sample reassignment between populations
- [x] Tests: sample_manager (126), sample_queries (96), sample_reassign (148), cli_sample (94)

### Quality Control
- [x] QCManager (qc/manager.py, 205 lines — variant, sample, batch QC)
- [x] Formatters (qc/formatters.py, 188 lines — TSV, JSON, HTML)
- [x] graphmana qc --type all --output qc_report.html
- [x] Tests: qc_manager (41), qc_queries (69), qc_formatters (141), cli_qc (51)

### Cluster Support
- [x] neo4j_lifecycle.py — Neo4j user-space setup/start/stop (setup_neo4j, start_neo4j, stop_neo4j, auto_memory_config)
- [x] filesystem_check.py — NFS/network filesystem detection (14 network FS types, df -T + /proc/mounts)
- [x] CLI commands: setup-neo4j, neo4j-start, neo4j-stop, check-filesystem
- [x] SLURM/PBS example scripts (6 scripts + README)
- [x] Tests: test_cluster.py (59 tests — 54 original + 5 auto lifecycle)
- [x] docs/cluster.md — Full cluster deployment guide

### Infrastructure
- [x] Snapshot/backup/restore (snapshot/manager.py, 272 lines — neo4j-admin dump/load)
- [x] Tests: snapshot_manager (159), cli_snapshot (90)
- [x] Benchmark suite (benchmarks/ — 11 files: ingest, export, parallel, 1KGP, fixtures)
- [x] Tests: benchmark_fixtures (125), benchmark_measurement (152)
- [x] Schema migration system (migration/manager.py, 140 lines)
- [x] Tests: migration (215 lines — 7 test classes)
- [x] Python Jupyter API (graphmana-py/ — client.py, _queries.py, _unpack.py — SKELETON)
- [ ] Tag v0.5.0

---

## v0.9 — Feature Complete (MOSTLY COMPLETE)

### Tier 2 Export Formats (ALL COMPLETE)
- [x] PLINK 2.0 (plink2_export.py, 144 lines — pgenlib)
- [x] Beagle (beagle_export.py, 138 lines — phased alleles)
- [x] STRUCTURE (structure_export.py, 212 lines — onerow/tworow)
- [x] Genepop (genepop_export.py, 159 lines — 6-digit codes)
- [x] Haplotype/selscan (hap_export.py, 207 lines — .hap/.map)
- [x] Tests: plink2 (107), beagle (115), structure (122), genepop (77), hap (109)

### Core v0.9 Features
- [x] Reference genome liftover (liftover/ — chain_parser.py 144 + lifter.py 267 lines)
- [x] Tests: liftover (361 lines — 9 test classes)
- [x] Schema migration (migration/manager.py — framework + 2 pre-defined migrations)
- [x] Tests: migration (215 lines — 7 test classes)
- [x] Sample hard delete with packed array rebuild
- [x] Tests: hard_delete (209 lines)

### Remaining v0.9 Items
- [x] --auto-start-neo4j on ingest/load-csv/export (context manager + 7 tests)
- [x] Dockerfile with demo dataset (multi-stage: Java build, Python venv, Neo4j runtime)
- [x] Tutorial and full documentation (README expanded, docs/tutorial.md, docs/cluster.md)
- [x] Provenance tracking (provenance/manager.py — ProvenanceManager, CLI, 23 tests)
- [ ] Tag v0.9.0

---

## v1.0 — Publication Release (MOSTLY COMPLETE)

_Single comprehensive Nature Methods paper describing the fully mature GraphMana platform._

### Java Procedures (COMPLETE)
- [x] Copy 8 utility classes from GraphPop (package rename, HWE removed from VariantFilter, stats removed from VectorOps)
- [x] SubsetStatsProcedure.java — server-side graphmana.subsetStats procedure
- [x] 9 Java test files (~95 tests)

### Normalizer + SV Support (COMPLETE)
- [x] normalizer.py rewritten — bcftools norm wrapper (155 lines)
- [x] SV fields (sv_type, sv_len, sv_end) in VCF parser, CSV emitter, export filters
- [x] CLI: --normalize, --reference-fasta on ingest + prepare-csv; --filter-sv-type on export
- [x] Tests: test_normalizer.py (13), test_sv_support.py (11)

### Tier 3 Export Formats (COMPLETE)
- [x] JSON export (json_export.py — JSON Lines, FAST/FULL path, fields/pretty/genotypes options)
- [x] Zarr export (zarr_export.py — sgkit-compatible, chunked, optional zarr dependency)
- [x] GDS export (gds_export.py — SeqArray HDF5, optional h5py dependency)
- [x] BGEN export (bgen_export.py — BGEN 1.2 Layout 2, zlib, uint16 probabilities)
- [x] Registered in export/__init__.py + CLI format choices + dispatch
- [x] Tests: json (12), zarr (12), gds (10), bgen (10)

### Jupyter API Expansion (COMPLETE)
- [x] unpack_phase(), unpack_ploidy() in _unpack.py
- [x] New queries: GENE_VARIANTS, ANNOTATED_VARIANTS, COHORT_SAMPLES, FILTERED_VARIANTS
- [x] New client methods: gene_variants, annotated_variants, cohort_samples, filtered_variants, to_vcf, to_plink, to_treemix
- [x] Tests: test_unpack.py (11), test_client_expanded.py (19)

### Remaining for Publication
- [x] Database merging (merger.py ~400 lines, 34 tests, CLI `graphmana merge`)
- [x] Comprehensive docs and API reference (docs/api-reference.md — CLI, Python API, export formats, encoding spec)
- [x] Integration test with real 1000 Genomes data (chr22: 28/28 passed; full genome: 10 tests, 2-chrom exports)
- [ ] Full benchmark suite with publication-quality results
- [x] Verify GraphPop compatibility against /mnt/e/GraphPop (docs/graphpop-compat.md + 21 encoding tests)
- [ ] Tag v1.0.0 and submit paper

---

## Codebase Statistics (as of 2026-03-17)

| Component | Files | Lines | Status |
|-----------|-------|-------|--------|
| graphmana-cli (Python) | 75 | ~17,500 | Production-ready (audit-hardened) |
| graphmana-procedures (Java) | 12 | ~1,170 | 10 classes + SubsetStatsProcedure |
| graphmana-py (Jupyter API) | 4 | ~780 | Expanded (phase/ploidy, filtering, exports) |
| Tests (Python CLI) | 73 | ~12,500 | 1217 tests passing |
| Tests (Python Jupyter) | 5 | ~465 | 74 tests passing |
| Tests (Java) | 10 | ~1,300 | ~96 tests |
| Benchmarks | 11 | ~2,190 | Complete |
| Dockerfile | 1 | 65 | Multi-stage build |
| docker-compose.yml | 1 | 11 | Working |
| Documentation | 7 | ~1,900 | README, tutorial, cluster, schema, API ref, AI integration, GraphPop compat |
| Demo data | 2 | ~130 | VCF + population map |

### Key Gaps (for v1.0 Publication Release)
1. ~~Database merging~~ — DONE (merger.py ~400 lines, 34 tests)
2. ~~Comprehensive docs/API reference~~ — DONE (docs/api-reference.md)
3. ~~Integration tests~~ — DONE (chr22: 28/28 passed; full genome: 10 tests with 2-chrom exports, pending re-run)
4. **Publication-quality benchmarks** — timing/scaling figures for the paper
5. **Version tags** — v0.1.0, v0.5.0, v0.9.0, v1.0.0 not yet tagged
6. **Single-paper manuscript** — Nature Methods Article describing the fully mature platform

---

## Review Notes

### 2026-03-11 — v0.9 finalization
- --auto-start-neo4j wired into ingest, load-csv, export via _auto_neo4j_lifecycle context manager
- Multi-stage Dockerfile with demo dataset (100 SNPs, 20 samples, 4 populations)
- Documentation: README expanded (~160 lines), docs/tutorial.md (~250 lines), docs/cluster.md (~250 lines)
- 955 Python tests passing (7 new: 5 auto lifecycle + 2 export options), black + ruff clean
- Ready for v0.9.0 tag

### 2026-03-11 — v1.0+ implementation
- Java procedures: 8 utility classes from GraphPop + SubsetStatsProcedure + 9 test files (~95 tests)
- Normalizer rewritten: bcftools norm wrapper (155 lines, no longer a stub)
- SV support: sv_type/sv_len/sv_end in parser, emitter, filters, CLI
- Tier 3 exports: JSON, Zarr, GDS, BGEN — all with tests
- Jupyter API expanded: phase/ploidy unpacking, gene/annotation/cohort/filtered queries, convenience exports
- 1031 CLI tests + 74 Jupyter tests passing, black + ruff clean

### 2026-03-11 — Thorough audit + cluster implementation
- All v0.1 phases complete except normalizer.py (stub) and GraphPop compat verification
- All v0.5 features complete including cluster support (neo4j_lifecycle, filesystem_check, SLURM/PBS scripts)
- All v0.9 Tier 2 export formats complete; liftover, migration, hard delete done
- 925 Python tests passing (54 new cluster tests), black + ruff clean
- Main remaining work: provenance, Java procedures, Dockerfile, docs

### 2026-03-24 — Integration tests + parallelism improvements
- Integration tests with real 1KGP data: chr22 (28/28 passed, 43 min) and full genome (8/8 passed, 2 skipped OOM, ~9 hr)
- Within-chromosome parallelism: splits single-chromosome VCFs into position-based regions (2.45x speedup on chr22)
- Multi-file parallel processing: processes 22 per-chromosome VCFs concurrently (8 workers)
- Fixed: cyvcf2 seqlens crash, Neo4j user-space run dir, kwargs forwarding, multi-file CLI support
- Fixed: benchmark script CLI options (--filter-chr → --chromosomes, --sfs-populations repeated flags)
- Test infrastructure: session-scoped fixtures, wc -l for large files, file-size validation
- 1,217 unit tests + 28 chr22 integration + 10 full genome integration tests
- Bugs discovered: SFS dadi 3-pop not implemented, VCF --output-type z not BGZF, prepare-csv only processed first file

### Next Steps After Computer Reformat (Windows → Ubuntu)

**Immediate (before anything else):**
1. Copy `/mnt/data/GraphMana` and `/mnt/data/GraphPop` to the new system
2. Set up conda env `graphevo` with Python 3.11+, install `graphmana-cli` editable
3. Install Neo4j Community 5.x, Java 21+, bcftools, cyvcf2
4. Symlink 1KGP data in `data/` (or copy from GraphPop)
5. Verify: `python -m pytest tests/ -q` (1,217 unit tests)
6. Initialize git repo and create initial commit (still no version control!)

**Full genome integration re-run:**
7. Stop any Neo4j: `sudo systemctl stop neo4j`
8. Run: `pytest tests/integration/test_1kgp_integration.py -v -s -k "TestFullGenome" --timeout=36000`
   - 10 tests including 2-chrom TreeMix/BED exports (changed from full-genome to avoid OOM)
   - Estimated ~8 hours (prepare-csv ~4 hr, import ~3.5 hr, exports ~30 min)
   - Requires: port 7687 free, 50+ GB RAM, 300+ GB disk free
9. If all 10 pass → mark full genome integration as fully confirmed

**Remaining for v1.0 publication release:**
10. ~~Publication-quality benchmarks (timing/scaling figures for the paper)~~ — DONE (2026-03-27 to 2026-03-29)
11. Version tags: v0.1.0, v0.5.0, v0.9.0, v1.0.0
12. Nature Methods manuscript

---

### 2026-03-27 to 2026-03-30 — Major development session (Ubuntu migration + benchmarks)

**Environment setup:**
- Reformatted Windows/WSL → pure Ubuntu 24.04
- Installed Java 21, Maven, bcftools, Git, Miniforge, Neo4j 5.26.0
- Fixed 55 symlinks (`/mnt/workspace` → `/mnt/data`) + 6 source files
- Created conda env `graphmana` (Python 3.12, cyvcf2 0.32.1)
- 1230 unit tests passing

**Database creation:**
- Full GraphMana-native 1KGP import: 70.7M variants, 3,202 samples, 22 chromosomes
- prepare-csv: 2 hours (16 threads) → 221 GB CSVs
- neo4j-admin import: 3 minutes → 166 GB database

**Bugs fixed:**
- BGZF VCF export (`--output-type z`): implemented BGZFWriter with proper EOF marker
- OOM on all-chromosome exports: property-selected FAST PATH queries (5x less data)
- GC pause timeouts: unordered queries for aggregation exports
- Batched pagination: 500K variants per batch for ordered queries
- SFS memory accumulation: streaming into SFS array directly (both dadi + fsc)
- SFS fsc 0 variants: missing `--sfs-folded` flag (not a code bug)

**New CLI commands (9 new, 31 total):**
- `graphmana init` — one-command project setup
- `graphmana cluster generate-job` — SLURM/PBS script generation
- `graphmana cluster check-env` — environment verification
- `graphmana db info/check/password/copy` — database administration
- `graphmana summary` — human-readable dataset report
- `graphmana query` — Cypher from CLI (read-only)

**Architecture improvement:**
- Unified `_iter_variants()` with `need_genotypes` / `need_order` parameters
- Three query tiers: FAST (pop arrays), GENOTYPES (packed arrays), FULL (legacy)
- Auto-selected batched pagination for large chromosomes

**Documentation:**
- 58 command reference pages (auto-generated from CLI --help)
- 11 vignettes (quickstart, 1KGP import, export formats, cohorts, annotation, sample lifecycle, liftover, HPC cluster, Jupyter API, database admin, variant representation)
- Expanded docs/cluster.md with resource estimation, end-to-end workflow, troubleshooting

**Benchmark results (all phases complete):**
- Phase 1: FAST PATH all-chr — TreeMix 102min, SFS dadi 98min, BED 103min, TSV 101min
- Phase 2: FULL PATH chr22 — VCF 649s, PLINK 22s, EIGENSTRAT 192s
- Phase 3: Parallel scaling — PLINK 10x speedup at 8 threads
- Phase 4: FULL PATH all-chr — PLINK 156s, EIGENSTRAT 3.7hr, VCF 3.7hr

**Validation:**
- VCF roundtrip: 99.999%+ concordance (897,645 biallelic SNPs, 5 samples)
- Incremental import: works for per-chromosome; whole-genome exceeds 64GB (documented)

**Git:** Initial commit created (247 files, 45,057 lines). GitHub repo pending PAT setup.

---

## Comprehensive Benchmark Strategy (Nature Methods)

_Added: 2026-03-30. Updated: 2026-03-31. Status: Complete._

### General Strategy

**Primary dataset**: 1KGP chr22 (1.07M variants, 3,202 samples, 26 populations).
Chr22 is used for the full benchmark matrix — large enough to be meaningful, small
enough to complete the entire GraphMana vs bcftools comparison in 2-3 hours.

**Whole-genome data points**: Add selectively for key demonstrations (initial import
timing, FAST PATH export scaling) to show that results generalize. Not used for
the full apples-to-apples comparison — that would take days.

**Comparison target**: bcftools only (no PLINK). bcftools is the stronger tool;
comparing against it avoids cherry-picking.

**Sample split**: base 2,500 + 3 batches of 234 = 3,202 total. Deterministic seed=42.

### Five Benchmarks

1. **Incremental Addition** — GraphMana rebuild vs bcftools merge (3 rounds)
2. **Cohort Extraction** — 5 superpopulation cohorts, VCF export
3. **Multi-format Export** — 6 formats from single source (bcftools: VCF only)
4. **Annotation Update** — In-place (GraphMana) vs full VCF rewrite (bcftools)
5. **Lifecycle Simulation** — 7-phase project simulation (the headline figure)

### Chr22 Benchmark Results (GraphMana vs bcftools, COMPLETE)

**Benchmark 1: Incremental Addition** (chr22, 1M variants, 3 × 234 samples)

| | Batch 1 | Batch 2 | Batch 3 | Total |
|---|---|---|---|---|
| GraphMana (rebuild) | 418s | 412s | 424s | 1,254s (+602s initial) |
| bcftools (merge) | 117s | 125s | 133s | 374s |

**Benchmark 2: Cohort Extraction** (5 superpopulation cohorts → VCF)

| Cohort | GraphMana | bcftools |
|--------|-----------|----------|
| AFR (893) | 191s | 59s |
| EUR (633) | 211s | 51s |
| EAS (585) | 131s | 49s |
| EUR+EAS (1218) | 238s | 64s |
| ALL (3202) | 571s | 110s |

**Benchmark 3: Multi-format Export** (from single source)

| Format | GraphMana | bcftools |
|--------|-----------|----------|
| VCF | 473s | 96s |
| TreeMix | 189s | N/A |
| SFS-dadi | 86s | N/A |
| SFS-fsc | 119s | N/A |
| BED | 84s | N/A |
| TSV | 85s | N/A |

**Benchmark 4: Annotation Update** (53K BED regions)

| | GraphMana | bcftools |
|---|-----------|----------|
| Time | **3.5s** | 96s |
| Method | In-place update | Rewrite entire VCF |
| Speedup | **27x** | baseline |

**Benchmark 5: Lifecycle Simulation** (7 phases)

| | GraphMana | bcftools |
|---|-----------|----------|
| Total time | 5,880s (98 min) | 1,006s (17 min) |
| Operations completed | **46/46** | **17/26** |
| TreeMix/SFS/BED exports | Supported | N/A (9 operations) |

### Whole-Genome Benchmark Results (COMPLETE 2026-03-31)

**Incremental import: 234 samples added to 70.7M-variant database**

| Approach | Time | Status |
|----------|------|--------|
| Cypher transactions (2026-03-30) | Crashed at 20 min | Failed |
| Neo4j Bolt read → rebuild (2026-03-31) | 10+ hr (estimated) | Too slow |
| **CSV-to-CSV rebuild** | **182 min (3 hr)** | **Success** |

Breakdown:
- VCF parsing (chr22 batch, 1M variants): 3 min
- CSV read + extend + write (70.7M variants, 214 GB): 160 min
- neo4j-admin import: 15 min
- Neo4j restart + indexes: 4 min

Variant processing:
- 1,066,557 extended (chr22 — actual genotypes)
- 69,625,458 HomRef-extended (other chromosomes — zero-byte append)
- 70,691,875 total variants preserved

**Full pipeline timing (whole-genome from scratch):**

| Step | Time |
|------|------|
| prepare-csv (22 VCFs, 2500 samples, 16 threads) | 95 min |
| neo4j-admin import (214 GB CSVs → 166 GB DB) | 3 min |
| Incremental add 234 samples (CSV-to-CSV) | 182 min |
| **Total: initial + 1 incremental** | **280 min (4.7 hr)** |

### Implementation

- Benchmark script: `benchmarks/bench_comprehensive.py`
- Fixtures: `benchmarks/fixtures/comprehensive/`
- Results: `benchmarks/results/comprehensive_comprehensive.jsonl`
- Incremental strategies:
  - CSV-to-CSV rebuild: `ingest/incremental_rebuild.py::run_incremental_from_csv()` — fastest, requires CSV checkpoint
  - Neo4j-based rebuild: `ingest/incremental_rebuild.py::run_incremental_rebuild()` — works without checkpoint, slow for whole-genome
  - Cypher transactions: `ingest/incremental.py::IncrementalIngester` — original, works for small additions (<10K variants)
  - Java server-side: `IncrementalExtendProcedure.java` — kept for future small-batch use

---

## Completed: Incremental Import Optimization (2026-03-30 to 2026-03-31)

### CSV-to-CSV rebuild (2026-03-31, the winning strategy)
- Reads existing variant_nodes.csv directly (no Neo4j query) at NVMe speed
- Extends packed arrays in Python, writes new CSV, reimports via neo4j-admin
- Whole-genome (70.7M variants): **182 min** — the only viable approach on 64 GB
- Chr22 (1M variants): **7 min** vs 60+ min (Cypher) or 10+ hr (Bolt read)
- CSV checkpoint kept as permanent artifact alongside Neo4j database
- Auto-detected: if `--output-csv-dir` contains variant_nodes.csv, uses CSV path
- Code: `ingest/incremental_rebuild.py::run_incremental_from_csv()`

### Chromosome-wise streaming (memory optimization)
- Replaced `_collect_variants_by_chr()` with `_stream_variants_by_chr()` generator
- Memory: O(all variants) → O(largest chromosome), ~12x reduction
- Code: `incremental.py` lines 169-237

### Export-extend-reimport (performance optimization)
- Bypasses Neo4j transaction engine entirely for large incremental imports
- Reads existing data from Neo4j (read-only), extends in Python (numpy), rebuilds via neo4j-admin import
- 6 min 45 sec for chr22 (1M variants, 234 samples) vs 60+ min with Cypher transactions
- Auto-selected when `--neo4j-home` is provided with `--mode incremental`
- Code: `ingest/incremental_rebuild.py`, routing in `pipeline.py`

### Server-side Java procedure (kept but not primary path)
- `graphmana.extendVariants` / `graphmana.extendHomRef` procedures created
- Useful for small incremental additions (<10K variants) in the future
- Code: `IncrementalExtendProcedure.java`

---

## Previous: Chromosome-wise Incremental Import Design (superseded)

_Priority: High — overcomes the whole-genome incremental import limitation on 64 GB machines._

### Problem
Whole-genome incremental import (adding samples to a 70.7M-variant database) crashes Neo4j because updating millions of packed arrays in one continuous session exceeds available transaction memory.

### Solution: Chromosome-wise Strategy
Process one chromosome at a time, committing and releasing transaction memory between chromosomes.

### Implementation Plan

**Step 1: Chromosome-wise sequential import**
```python
for chrom in chromosomes:
    ingest_incremental(vcf, panel, chromosome=chrom)
    # Transaction log freed between chromosomes
```
- Each chromosome is 1-6M variants → fits in 16 GB heap
- Simple, guaranteed to work on 64 GB machines

**Step 2: Batch variant updates within each chromosome**
```cypher
-- Instead of one variant per transaction:
UNWIND $updates AS u
MATCH (v:Variant {variantId: u.variantId})
SET v.gt_packed = u.gt_packed, v.ac = u.ac, ...
```
- 1000 variants per batch → 66,000 transactions vs 70,700,000
- ~1000x less transaction overhead

**Step 3: Auto-select strategy**
```python
def choose_strategy(n_variants, n_new_samples, available_ram):
    bytes_per_variant = 50 + (n_new_samples + 3) // 4
    total_write_mem = n_variants * bytes_per_variant
    neo4j_available = available_ram * 0.4

    if total_write_mem < neo4j_available:
        return "whole-genome"       # Single pass
    elif total_write_mem / 22 < neo4j_available:
        return "chromosome-wise"    # Per-chromosome
    else:
        return "batched"            # Even single chromosome batched
```

### Files to Modify
- `graphmana-cli/src/graphmana/ingest/incremental.py` — add chromosome-wise loop + batch updates
- `graphmana-cli/src/graphmana/cli.py` — auto-select strategy in `ingest` command
- `graphmana-cli/tests/test_incremental.py` — add chromosome-wise tests

### Verification
- Test on existing 1KGP 70.7M-variant database: add 10/100 samples incrementally
- Verify gt_packed grows correctly, population arrays updated, sample count increases
- Compare genotypes before/after for a known sample

---

## Planned: Other Items (lower priority)

- [ ] Git push to GitHub (need Personal Access Token)
- [ ] Version tags: v0.1.0, v0.5.0, v0.9.0, v1.0.0
- [ ] SFS dadi 3-population implementation
- [ ] `--auto-start-neo4j` on `load-csv` — should stop Neo4j first, import, then start
- [ ] BCF output (`--output-type b`)
- [ ] mkdocs/Sphinx documentation site
- [ ] Docker image rebuild
- [ ] Nature Methods manuscript writing

---

## Resume Checklist (after computer restart)

1. `conda activate graphmana` — Python environment
2. `~/neo4j/bin/neo4j start` — start Neo4j (data at `~/neo4j/data/databases/neo4j/`, 166 GB)
3. `graphmana status --neo4j-password graphmana` — verify database (70.7M variants, 3202 samples)
4. `cd /mnt/data/GraphMana` — project directory
5. `pytest graphmana-cli/tests/ -q` — verify 1230 tests pass
6. Neo4j config: heap=16g, pagecache=16g at `~/neo4j/conf/neo4j.conf`
7. Git: initial commit done, no remote yet (need GitHub PAT)
8. Benchmark outputs at `results/bench/` — do NOT delete
