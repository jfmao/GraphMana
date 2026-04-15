# GraphMana — Claude Code Project Instructions

## Project Identity

**GraphMana** is a graph-native data management platform for variant genomics. It stores VCF/GVCF data as a persistent, queryable Neo4j graph with packed genotype arrays on Variant nodes, pre-computed population statistics, incremental sample addition, integrated functional annotations, cohort management, reference genome liftover, annotation versioning, and multi-format export to VCF, PLINK, EIGENSTRAT, TreeMix, SFS, and 10+ additional formats. Target: 100–50,000 samples on a single machine or HPC cluster node.

**Publication target:** Nature Methods (Brief Communication). A focused paper on the data management platform — incremental import, provenance tracking, multi-format export, QC, cohort management. GraphPop (analytical engine) is a separate companion Article in Nature Methods; GraphMana addresses the complementary challenge of project-level data lifecycle management.

**Demonstration dataset:** 1000 Genomes Project chr22 (3,202 samples, 1.07M variants) — full import, export, and lifecycle benchmark complete. Rice 3K chr1 CSV generation also tested.

---

## Development Paths

```
GraphMana project:   /mnt/data/GraphMana      ← ALL development happens here
GraphPop reference:  /mnt/data/GraphPop        ← READ-ONLY reference for code reuse
```

- **GraphMana** (`/mnt/data/GraphMana`): Active development target.
- **GraphPop** (`/mnt/data/GraphPop`): Reference codebase. Do NOT modify.

When adapting GraphPop code: read first at `/mnt/data/GraphPop`, copy to `/mnt/data/GraphMana`, rename `graphpop` → `graphmana`, remove statistical module references, preserve tests.

---

## Workflow Orchestration

### 1. Plan Mode Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately
- Always plan before touching schema, import pipeline, packed array logic, export format logic, liftover, cluster deployment, or parallelism design

### 2. Subagent Strategy
- Use subagents liberally for benchmarking scripts, test fixture generation, bit-packing verification, export format validation, Cypher query optimization, reading GraphPop source at `/mnt/data/GraphPop`

### 3. Self-Improvement Loop
- After ANY correction: update "tasks/lessons.md"
- Review lessons at session start

### 4. Verification Before Done
- Run tests, check logs, demonstrate correctness
- For every export format: verify output can be read by the target tool
- For packed arrays: verify bit-level correctness
- For parallelism: verify `--threads 1` vs `--threads N` produce identical output
- For cluster mode: verify `prepare-csv` + `load-csv` produces same result as single `ingest`

### 5. Demand Elegance (Balanced)
- For non-trivial changes: "is there a more elegant way?" Skip for simple fixes.

### 6. Autonomous Bug Fixing
- Just fix it. Zero context switching from the user.

---

## Task Management

1. **Plan First**: Write plan to "tasks/todo.md"
2. **Verify Plan**: Check in before starting
3. **Track Progress**: Mark items complete
4. **Explain Changes**: High-level summary
5. **Document Results**: Review section in "tasks/todo.md"
6. **Capture Lessons**: Update "tasks/lessons.md"

---

## Core Principles

- **Simplicity First**: Make every change as simple as possible.
- **No Laziness**: Find root causes. Senior developer standards.
- **Minimal Impact**: Touch only what's necessary.
- **Lossless Fidelity**: Document what is and is not preserved per pathway.
- **Forward Compatibility**: Schema must work for GraphPop directly.
- **No CARRIES Edges**: Packed byte arrays on Variant nodes. Settled.
- **Parallelism by Default**: Every procedure supports `--threads`. Default 1.
- **Rich Options**: Every CLI command exposes comprehensive settings.
- **Cluster-Friendly**: CSV generation runs without Neo4j. Batch jobs supported.

---

## What GraphMana IS

- A data management platform storing genomic variant data as a persistent, queryable Neo4j graph
- A system allowing incremental addition of new samples without re-processing
- A cohort management tool defining subsets as graph queries, not file extractions
- A multi-format export engine (VCF, PLINK1.9, PLINK2, EIGENSTRAT, TreeMix, SFS, Beagle, STRUCTURE, Genepop, BED, TSV, and more)
- A reference genome liftover tool for coordinate transformation across assemblies
- An annotation management system with versioning and updatable layers
- Species-agnostic; handles diploid, haploid, and mixed-ploidy chromosomes
- Deployable on personal workstations AND HPC cluster compute nodes
- A data foundation that GraphPop connects to directly

## What GraphMana is NOT

- NOT a variant caller
- NOT a population genetic statistics engine (that is GraphPop at `/mnt/data/GraphPop`)
- NOT a visualization tool
- NOT targeting biobank scale (500K+ samples) in v1
- NOT a distributed computing framework (no Spark/Dask/multi-node Neo4j)
- NOT replacing Hail or PLINK2 for compute-intensive operations

**CRITICAL BOUNDARY**: Never implement statistical procedures in this codebase. Those belong to GraphPop only.

---

## Scalability Profile

### Target Scale

**Sweet spot: 100–10,000 samples.** Everything is interactive — imports in minutes, exports in seconds to minutes, incremental addition genuinely fast, FULL PATH exports complete quickly.

**Supported with longer operation times: 10,000–50,000 samples.** Initial import takes hours, incremental addition takes tens of minutes to hours for whole-genome, FULL PATH exports are slow. FAST PATH operations (TreeMix, SFS, allele frequency queries) remain instant.

**Hard ceiling: ~50,000 samples for whole-genome data** on a single machine with 8 TB NVMe. Beyond this, packed arrays per variant exceed comfortable Neo4j property sizes (~12.5 KB at 50K samples), and total database size pushes past available SSD. For exome-only data (5–10M variants vs 85M), the ceiling is proportionally higher.

### Storage Arithmetic

For N samples, whole genome (~85M biallelic variants):

| Component | Formula | 3,202 samples | 10K samples | 50K samples |
|-----------|---------|---------------|-------------|-------------|
| gt_packed | ceil(N/4) × 85M | ~68 GB | ~213 GB | ~1.0 TB |
| phase_packed | ceil(N/8) × 85M | ~34 GB | ~106 GB | ~0.5 TB |
| Pop arrays + properties | ~200 bytes × 85M | ~17 GB | ~17 GB | ~17 GB |
| NEXT chain edges | ~50 bytes × 85M | ~4 GB | ~4 GB | ~4 GB |
| Annotation edges | ~100 bytes × edges | ~10–50 GB | ~10–50 GB | ~10–50 GB |
| Neo4j overhead (indexes, store) | ~30% of above | variable | variable | variable |
| **Total estimate** | | **~130–200 GB** | **~400–550 GB** | **~2–3 TB** |

For exome-only (~5M variants): divide all variant-proportional numbers by ~17.
For single chromosome (chr22, ~1M variants): divide by ~85.

### Operation Timing Matrix

| Operation | 100–3K samples, chr22 | 3K–10K samples, WGS | 10K–50K samples, WGS |
|-----------|----------------------|---------------------|----------------------|
| Initial import | Minutes | 30–120 min | Hours |
| Incremental add (100 samples) | Seconds | Minutes | 30–120 min |
| FAST PATH export (TreeMix, SFS) | Seconds | Seconds | Seconds |
| FULL PATH export (VCF, PLINK) | Seconds | Minutes | Hours |
| Liftover | Seconds | Minutes | 30–60 min |
| Soft delete | Seconds | Seconds | Seconds |
| Hard delete (array rebuild) | Minutes | Hours | Many hours |
| Cypher relational query | Seconds | Seconds | Seconds |

### Why FAST PATH Scales Perfectly

FAST PATH operations read pre-computed population arrays (ac[], an[], af[]) which are K-element arrays where K = number of populations (typically 5–30). These arrays are **constant size regardless of N**. At 50K samples with 10 populations, the ac[] array is still just 10 integers. This is why TreeMix/SFS export takes seconds at any sample count.

---

## Cluster and HPC Deployment

Four deployment models: (1) Dedicated Node, (2) Interactive SLURM/PBS Job, (3) Batch Job with `--auto-start-neo4j`, (4) **Two-Step Split** (recommended — `prepare-csv` on any node, `load-csv` on Neo4j host).

**Key constraint**: Neo4j data dir MUST be on local SSD/scratch (not NFS/Lustre/GPFS). Use `graphmana check-filesystem` to verify. CSV/VCF/exports can use shared storage.

Cluster CLI: `setup-neo4j`, `neo4j-start`, `neo4j-stop`, `check-filesystem`, `prepare-csv`, `load-csv`. SLURM/PBS scripts in `scripts/cluster/`. Full details in `docs/cluster.md`.

---

## Relationship to GraphPop

GraphMana is the data management foundation. GraphPop is the analytical engine built on top. They share the same database, schema, and Java plugin infrastructure, and are described together in a single publication.

**Database must be directly usable by GraphPop without conversion.** Verify after schema changes by checking `/mnt/data/GraphPop`.

### Code Reuse from GraphPop (at `/mnt/data/GraphPop`)
CAN adapt: VCF parsing, CSV generation, schema creation, VEP parser, allele count arrays, genotype packing, PackedGenotypeReader.java, SampleSubsetComputer.java, NEXT chain builder, ploidy detection.

Must NOT copy: statistical procedures, HaplotypeMatrix, GraphPop benchmarks/CLI.

---

## CRITICAL ARCHITECTURAL DECISION: Why Not CARRIES Edges

| Approach | Per-variant (3,202 samples) | chr22 (1.07M) | Whole genome (~85M) |
|----------|---------------------------|----------------|---------------------|
| CARRIES edges | ~150 KB | ~160 GB | ~12 TB |
| Packed arrays | 1,202 bytes | ~1.3 GB | ~100 GB |

**125× reduction.** DO NOT revert.

---

## Packed Genotype Array Specification

**gt_packed**: 2 bits/sample, 4/byte, LSB-first. 00=HomRef, 01=Het, 10=HomAlt, 11=Missing. CRITICAL REMAP from cyvcf2. v1.1: optional tagged-blob wrapper (0x00 dense / 0x01 sparse) chosen per variant.
**phase_packed**: 1 bit/sample, 8/byte, LSB-first.
**ploidy_packed**: 1 bit/sample. Null = all diploid.
**called_packed** (v1.1): 1 bit/sample, 8/byte, LSB-first. 1=sample was interrogated at this site, 0=not looked at. Preserves HomRef-vs-Missing across incremental batches. Null on schema v1.0 DBs = "all samples called" (legacy fallback). See docs/gvcf-workflow.md.
**CSV**: Semicolon-delimited signed Java bytes (-128 to 127).
**PackedGenotypeReader**: Branchless bit shifts. O(1) per sample.

---

## Two Access Paths for Genotype Data

**FAST PATH**: Pre-computed pop arrays (ac[], an[], af[]). Zero unpacking. TreeMix, SFS, fastsimcoal2, BED, allele frequency TSV. **Constant time in N.**

**FULL PATH**: Unpack gt_packed/phase_packed via PackedGenotypeReader. VCF, PLINK, EIGENSTRAT, Beagle, STRUCTURE, Genepop, haplotype matrix, QC. **Linear in N.**

---

## Export Formats — Comprehensive Reference

### Tier 1: Core Formats (v0.5)

**VCF/BCF** `--format vcf` — FULL PATH. Roundtrip fidelity. Options: `--vcf-version`, `--output-type [v|z|b]`
**PLINK 1.9** `--format plink` — FULL PATH. .bed/.bim/.fam. Biallelic SNPs only.
**EIGENSTRAT** `--format eigenstrat` — FULL PATH. .geno/.snp/.ind. For smartPCA, AdmixTools.
**SFS dadi** `--format sfs-dadi` — FAST PATH. Options: `--sfs-populations`, `--sfs-projection`, `--sfs-polarized/--sfs-folded`
**SFS fastsimcoal2** `--format sfs-fsc` — FAST PATH. .obs files with fsc2 naming convention.
**TreeMix** `--format treemix` — FAST PATH. Gzipped allele count matrix. Showcase: seconds at any N.
**TSV** `--format tsv` — Either path. Options: `--tsv-columns`
**BED** `--format bed` — FAST PATH. Variant positions for bedtools/IGV. Options: `--bed-extra-columns`

### Tier 2: Specialist Formats (v0.9)

**PLINK 2.0** `--format plink2` — FULL PATH. .pgen/.pvar/.psam.
**Beagle** `--format beagle` — FULL PATH. Requires `--fidelity full` for PL field.
**STRUCTURE** `--format structure` — FULL PATH. Options: `--structure-format [onerow|tworow]`
**Genepop** `--format genepop` — FULL PATH. Conservation genetics bridge format.
**Haplotype** `--format hap` — FULL PATH. .hap/.map for selscan. Phased data only.

### Tier 3: Binary/Array Formats (v1.0)

**BGEN** `--format bgen` — FULL PATH. Layout 2, zlib, uint16 probabilities.
**GDS** `--format gds` — FULL PATH. SeqArray HDF5 (optional h5py).
**Zarr** `--format zarr` — FULL PATH. sgkit-compatible, chunked.
**JSON** `--format json` — JSON Lines, FAST/FULL path.

---

## Reference Genome Liftover

Changes: variantId, pos, chr, ref (strand flips), NEXT chain, Gene coords.
Unchanged: gt_packed, phase_packed, population arrays, Samples, Populations.
Unmappable variants: flagged with liftover_status, never silently dropped.

```
graphmana liftover --chain FILE --target-reference TEXT
  [--reject-file FILE] [--update-annotations] [--threads INT] [--dry-run] [--backup-before]
```

---

## Annotation Update and Versioning

HAS_CONSEQUENCE edges carry `annotation_source` + `annotation_version`. AnnotationVersion nodes track history. Three modes: add (layer), update (merge), replace (clean swap).

```
graphmana annotate --type TYPE --input FILE --version TEXT --mode [add|update|replace]
graphmana annotate list | remove --version TEXT
```

---

## Sample Removal

**Soft delete** (default): `Sample.excluded = true`, masked everywhere. **Hard delete** (maintenance): rebuild packed arrays. **Restore**: clear excluded flag.

---

## Chromosome Naming, Backup/Snapshot, Schema Versioning

**Chr naming**: Auto-detect + `--chr-style [ucsc|ensembl|original]` + `--chr-map FILE`. Chromosome.aliases.
**Snapshots**: `graphmana snapshot create|list|restore|delete --name TEXT`. Wraps neo4j-admin dump/load.
**Schema versioning**: SchemaMetadata node. `graphmana migrate` for version upgrades.

---

## Parallelism Design

Every procedure supports `--threads`. Default 1. CSV generation parallel by chromosome. Export parallel by chromosome. QC parallel by batch. `prepare-csv` is embarrassingly parallel (ideal for cluster). Parallel output must equal sequential output.

---

## Filtering Design

**Import**: pass-only, biallelic-only, regions, contigs, min-qual, min-dp, min-gq, min-call-rate, maf, variant-type, sample lists.
**Export**: all import filters plus populations, cohort, consequence, impact, gene, cadd, annotation-version, liftover-status, exclude-soft-deleted, exclude-monomorphic.
Export filters on graph properties pushed into Cypher WHERE clauses.

---

## Graph Schema

### Node Types

```
(:Variant {
    variantId, chr, pos, ref, alt, variant_type,
    pop_ids[], ac[], an[], af[], het_count[], hom_alt_count[], het_exp[],
    ac_total, an_total, af_total, call_rate,
    gt_packed: byte[], phase_packed: byte[], ploidy_packed: byte[],
    ancestral_allele, is_polarized, qual, filter,
    consequence, impact, gene_symbol, info_raw, csq_raw,
    multiallelic_site, allele_index, population_specificity,
    liftover_status, original_variantId
})
(:Sample { sampleId, population, packed_index, sex, source_dataset, source_file,
    ingestion_date, phenotypes, excluded, exclusion_reason,
    n_het, n_hom_alt, heterozygosity, call_rate, rare_variant_burden })
(:Population { populationId, name, n_samples, a_n, a_n2 })
(:Chromosome { chromosomeId, length, n_variants, aliases[] })
(:Gene { geneId, symbol, chr, start, end, strand, biotype })
(:Pathway { id, name, source })
(:GOTerm { id, name, namespace })
(:RegulatoryElement { id, type, chr, start, end })
(:VCFHeader { dataset_id, source_file, header_text, file_format, reference, caller, import_date, info_fields[], format_fields[], filter_fields[], sample_fields_stored[] })
(:CohortDefinition { name, cypher_query, created_date, description })
(:IngestionLog { log_id, source_file, dataset_id, mode, import_date, n_samples, n_variants, filters_applied, fidelity, reference_genome })
(:AnnotationVersion { version_id, source, version, loaded_date, n_annotations, description })
(:SchemaMetadata { schema_version, graphmana_version, reference_genome, created_date, last_modified, n_samples, n_variants, n_populations, chr_naming_style })
```

### Relationships

```
(:Variant)-[:NEXT {distance_bp}]->(:Variant)
(:Variant)-[:ON_CHROMOSOME]->(:Chromosome)
(:Sample)-[:IN_POPULATION]->(:Population)
(:Variant)-[:HAS_CONSEQUENCE { type, impact, transcript_id, protein_change, codon_change, sift, polyphen, annotation_source, annotation_version }]->(:Gene)
(:Gene)-[:IN_PATHWAY]->(:Pathway)
(:Gene)-[:HAS_GO_TERM]->(:GOTerm)
(:GOTerm)-[:IS_A]->(:GOTerm)
```

**NO (:Sample)-[:CARRIES]->(:Variant).** Check `/mnt/data/GraphPop` after changes.

---

## Complete CLI Reference

### graphmana ingest
```
  --input FILE [FILE...], --input-list FILE, --population-map FILE
  --mode [auto|initial|incremental], --stratify-by COLUMN
  --reference TEXT, --dataset-id TEXT, --fidelity [minimal|default|full]
  --ancestral-fasta FILE, --chr-map FILE, --chr-style [ucsc|ensembl|original]
  --threads INT, --batch-size INT, --output-csv-dir PATH
  --neo4j-uri, --neo4j-user, --neo4j-password
  --auto-start-neo4j, --neo4j-home PATH, --neo4j-data-dir PATH
  (all --filter-* import filters)
  --dry-run, --verbose / --quiet
```

### graphmana prepare-csv (cluster-friendly, no Neo4j)
```
  --input FILE [FILE...], --input-list FILE, --population-map FILE
  --output-dir PATH (required), --stratify-by COLUMN, --reference TEXT
  --ancestral-fasta FILE, --chr-map FILE, --chr-style [ucsc|ensembl|original]
  --threads INT, --batch-size INT
  (all --filter-* import filters)
  --verbose / --quiet
```

### graphmana load-csv
```
  --csv-dir PATH (required)
  --neo4j-home PATH, --neo4j-data-dir PATH
  --auto-start-neo4j
  --neo4j-uri, --neo4j-user, --neo4j-password
```

### graphmana export
```
  --output FILE
  --format [vcf|plink|plink2|eigenstrat|treemix|sfs-dadi|sfs-fsc|beagle|structure|genepop|bed|hap|tsv]
  --threads INT
  Format-specific: --vcf-version, --output-type, --sfs-populations, --sfs-projection,
    --sfs-polarized/--sfs-folded, --sfs-include-monomorphic, --structure-format,
    --bed-extra-columns, --tsv-columns
  (all --filter-* export filters)
  --recalculate-af / --no-recalculate-af
  --auto-start-neo4j, --neo4j-home, --neo4j-data-dir
  --neo4j-uri, --neo4j-user, --neo4j-password, --verbose / --quiet
```

### graphmana liftover, annotate, cohort, sample, qc, status, snapshot, migrate
```
  graphmana liftover --chain FILE --target-reference TEXT [options]
  graphmana annotate --type TYPE --input FILE --version TEXT --mode [add|update|replace]
  graphmana annotate list | remove --version TEXT
  graphmana cohort define|list|show|delete|count|validate --name TEXT
  graphmana sample remove|restore|list [options]
  graphmana qc --type [sample|variant|batch|all] --output FILE --format [html|tsv|json]
  graphmana status [--detailed] [--json]
  graphmana snapshot create|list|restore|delete --name TEXT
  graphmana migrate [--dry-run]
```

### graphmana setup-neo4j, neo4j-start, neo4j-stop, check-filesystem
```
  graphmana setup-neo4j --install-dir PATH [--memory-auto]
  graphmana neo4j-start --neo4j-home PATH --data-dir PATH [--wait]
  graphmana neo4j-stop --neo4j-home PATH
  graphmana check-filesystem --neo4j-data-dir PATH
```

---

## Architecture — Repository Structure

```
/mnt/data/GraphMana/
├── CLAUDE.md
├── .claude/commands/
├── tasks/ (todo.md, lessons.md)
├── README.md, LICENSE, docker-compose.yml, Dockerfile
│
├── graphmana-procedures/          # Java plugin
│   └── src/main/java/org/graphmana/ (procedures, io, genotype, model, util)
│
├── graphmana-cli/                 # Python CLI
│   └── src/graphmana/
│       ├── cli.py
│       ├── ingest/ (vcf_parser, csv_emitter, genotype_packer, population_map,
│       │            ploidy_detector, normalizer, vep_parser, parallel, chr_reconciler)
│       ├── export/
│       │   ├── base.py            # BaseExporter with shared filtering + parallel
│       │   ├── vcf_export.py, plink_export.py, plink2_export.py
│       │   ├── eigenstrat_export.py, treemix_export.py
│       │   ├── sfs_dadi_export.py, sfs_fsc_export.py
│       │   ├── beagle_export.py, structure_export.py, genepop_export.py
│       │   ├── bed_export.py, hap_export.py, tsv_export.py
│       │   └── parallel.py
│       ├── filtering/ (import_filters, export_filters, base)
│       ├── cohort/, qc/, liftover/, annotation/, sample/
│       ├── snapshot/, migration/, provenance/
│       ├── cluster/               # Cluster deployment support
│       │   ├── neo4j_lifecycle.py # Start/stop/setup Neo4j in user space
│       │   ├── filesystem_check.py # Warn about NFS
│       │   └── prepare_load.py    # prepare-csv and load-csv logic
│       ├── db/ (connection, schema, queries)
│       └── config.py
│
├── graphmana-py/                  # Jupyter API (pandas DataFrames)
├── benchmarks/, docs/
│   └── docs/cluster.md            # Cluster deployment guide
├── scripts/cluster/               # SLURM/PBS example scripts
│   ├── slurm_prepare_csv.sh, slurm_load_csv.sh
│   ├── slurm_ingest_single.sh, slurm_export.sh
│   ├── slurm_interactive.sh, pbs_prepare_csv.sh
│   └── README.md
└── paper/
```

### Software Stack
Neo4j Community 5.x, Java 21+, Python 3.11+ (cyvcf2, numpy), Click CLI, bcftools. Testing: pytest, JUnit 5 + neo4j-harness. Linting: Black + ruff, Google Java Style.

### Hardware Target
- **Workstation**: 64–128 GB RAM, 1–8 TB NVMe SSD, 16+ core CPU
- **Cluster node**: 128–512 GB RAM, node-local SSD/scratch, 16–64 cores
- Neo4j data dir MUST be on local SSD/scratch (not NFS/Lustre/GPFS)
- No GPU required

---

## Key Design Decisions (DO NOT REVISIT)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Genotype storage | Packed byte arrays (NOT CARRIES edges) | 125× storage reduction |
| Node model | Variant (not Site/Allele) | Simpler, GraphPop compatible |
| cyvcf2 remap | 0=HomRef, 1=Het, 2=HomAlt, 3=Missing | ALT count = 2-bit value |
| Export architecture | FAST PATH + FULL PATH | TreeMix/SFS in seconds |
| Cluster support | Two-step split (prepare-csv + load-csv) | CSV gen runs anywhere without Neo4j |
| Target scale | 100–50K (sweet spot 100–10K) | >90% of real projects |
| Neo4j edition | Community (free) | No license barrier |
| GraphPop compat | Schema must match exactly | Shared database |

---

## Build Commands

```bash
cd /mnt/data/GraphMana/graphmana-procedures && mvn clean package -DskipTests
cd /mnt/data/GraphMana/graphmana-procedures && mvn test
cd /mnt/data/GraphMana/graphmana-cli && pip install -e ".[dev]"
cd /mnt/data/GraphMana/graphmana-cli && pytest -v
cd /mnt/data/GraphMana/graphmana-cli && black --check src/ tests/ && ruff check src/ tests/
cd /mnt/data/GraphMana && docker compose up --build
```

---

## Coding Standards

### Java
Google Java Style, no Lombok, JDK 21+, neo4j-harness tests, branchless PackedGenotypeReader, Maven Shade.

### Python
Black (100), ruff, type hints, Google docstrings, copy cyvcf2 arrays, vectorized numpy, Click, parameterized Cypher, 100K-variant chunks. BaseExporter for all export modules. Neo4j lifecycle helpers in cluster/ module.

### Commits
Conventional: feat:, fix:, test:, docs:, refactor:, bench:. Branch per sprint, squash merge, tags v0.1.0/v0.5.0/v0.9.0.

---

## Codebase Quality Rules (Condensed)

### Parameter Flow
- Every accepted parameter must have a consumer — trace end-to-end after adding
- Parameters must survive serialization boundaries (pickling, subprocess) — update `_get_filter_config_dict()` for new fields
- Fan-out sites must be exhaustive — one CLI option wired to 14 exporters means ALL 14 updated
- Don't load data you don't need — use `_get_sample_count()` not `_load_samples()` for counts

### Default Settings
- Define each default in exactly one place (named constant in `config.py`)
- All entry points (CLI, API, Jupyter) must use the same defaults
- Environment-aware: check cgroup limits in containers, not `/proc/meminfo`

### Logic Consistency
- One canonical reference per convention (phase bit → `vcf_export.py:format_gt`)
- Verify attribute/method names against actual definitions (`rec.chr` not `rec.chrom`)
- Derived state must update when source changes (filter samples → update all counters)
- Delete operations must cascade (remove edges → clean orphan nodes)
- Cross-cutting concerns (soft-delete, provenance) enforced in ALL code paths
- Verify formulas against references (harmonic sum, not `2*n`)
- Verify external tool flags against `--help` or docs
- Return dicts must be consistent across all implementations (always include `n_samples`)

---

---

## Implementation Checklists

Project-specific checklists derived from the rules above. Follow these whenever the corresponding task type comes up.

### New Exporter Checklist

When implementing a new export format:

1. **Identify path**: Is this FAST PATH (reads pop arrays, no unpacking) or FULL PATH (unpacks gt_packed)? This determines the base pattern.
2. **Phase convention**: `phase_bit=1` means ALT on second haplotype (REF|ALT = `0|1` in VCF). `phase_bit=0` at a het means ALT on first haplotype (ALT|REF = `1|0`). **Verify against `vcf_export.py:format_gt` as the canonical reference.**
3. **FAST PATH exporters**: Use `self._get_sample_count()` for `n_samples` in the return dict. Do NOT call `self._load_samples()` unless per-sample data is actually needed.
4. **Return dict**: Always include `n_samples`, `n_variants`, `format`, and `chromosomes` in the return dict.
5. **Parallel serialization**: If adding a new filter/config field, add it to `parallel.py::_get_filter_config_dict()` — it serializes configs for subprocess pickling.
6. **CLI wiring**: Wire `recalculate_af=bool(recalculate_af)` to the constructor in `cli.py`.
7. **Binary format specs**: Verify magic numbers, offsets, field sizes, and endianness against the official specification document. Never guess.
8. **Tests**: Write tests that verify phase convention, missing data handling, and at least one round-trip or tool-readable check.

### New CLI Option Checklist

When adding a new `@click.option` to any command:

1. **Consumer exists**: Grep for the variable name and verify it reaches the function that uses it. Every parameter must have a consumer — never accept an option that silently does nothing.
2. **All constructors updated**: If the option applies to multiple exporters/ingesters, update ALL constructor calls in `cli.py` (there are 14+ exporter instantiations).
3. **Parallel path**: If the option is a filter/config field, add it to `parallel.py::_get_filter_config_dict()`.
4. **Help text**: No version markers like "(v0.5)" in help strings — these go stale.
5. **Defaults**: Keep defaults consistent across CLI and Jupyter API. Both should read from the same env var with the same fallback.

### New Config/Dataclass Field Checklist

When adding a field to `ExportFilterConfig`, `VariantRecord`, or any shared dataclass:

1. **Serialization**: Update all serialization/deserialization points (parallel pickling, JSON, etc.).
2. **Attribute names**: Verify the actual attribute name on the dataclass (`rec.chr` not `rec.chrom`, `filter_chain.accepts()` not `.accept()`). Use grep to confirm.
3. **Method signatures**: When calling methods, verify the exact method name exists. Use grep.

### Operational Invariants

Rules that apply across all modules. Violations cause subtle, hard-to-detect bugs:

1. **Soft-delete awareness**: Any query that resolves sample IDs for operational use (cohort resolution, export filtering, population counts) must filter `WHERE s.excluded IS NULL OR s.excluded = false`. The `FETCH_SAMPLES` query does this; raw Cypher queries from users (cohort definitions) do not — post-filter them.
2. **Orphan cleanup**: When deleting relationships (HAS_CONSEQUENCE edges, IN_POPULATION edges), always check for and clean up orphaned nodes (Gene nodes with no remaining edges).
3. **Provenance**: New operations that modify the database (merge, liftover, annotation load) must record an `IngestionLog` entry via `ProvenanceManager`.
4. **Derived counts after filtering**: When skipping/filtering items (e.g., duplicate samples in merge), update ALL derived counters (`_source_pop_n_samples`, `_n_source_samples`, etc.). Stale counts propagate to wrong population statistics.
5. **Population genetics formulas**: Always verify against a textbook. `a_n = sum(1/i for i in range(1, 2n))` is a harmonic sum, NOT `2*n`.
6. **External tool flags**: Run `tool --help` or check docs before adding CLI flags to subprocess calls. Do not assume flag names.

---

## Testing Strategy

### Unit Tests
Genotype packing/unpacking, cyvcf2 remap, filter chain, chr reconciliation, allele counts, Cypher queries, Neo4j lifecycle (start/stop).

### Integration Tests
- VCF roundtrip: ingest → export → diff → 100% concordance
- PLINK/EIGENSTRAT/TreeMix/SFS: export → read with target tool → verify
- Two-step: prepare-csv → load-csv → compare with single-step ingest result
- Incremental, parallel consistency, filtering, multi-allelic
- GraphPop compat: GraphMana import → GraphPop procedure → works
- Ploidy, liftover, soft delete, annotation update, snapshot, chr naming
- Cluster filesystem check: detect NFS correctly

### Test Fixtures
small_cohort.vcf.gz, multiallelic.vcf.gz, vep_annotated.vcf.gz, population_map.tsv, missing_data.vcf.gz, mixed_ploidy.vcf.gz, phased.vcf.gz, low_quality.vcf.gz, grch37_variants.vcf.gz + chain file, chr_naming_ensembl.vcf.gz

---

## Troubleshooting

**cyvcf2 segfault**: Copy numpy arrays before scope exit.
**Wrong genotype encoding**: Verify remap = np.array([0, 1, 3, 2]).
**Neo4j byte array error**: Signed Java bytes, np.int8, --array-delimiter=";".
**Parallel output differs**: Sort CSV chunks before concatenation.
**GraphPop can't read database**: Check `/mnt/data/GraphPop` for property names.
**Incremental import corrupts**: Append to END, packed_index never changes.
**NEXT chain broken**: VCF must be position-sorted.
**PLINK export wrong**: PLINK BED uses different 2-bit encoding — verify conversion.
**TreeMix wrong pop count**: Verify pop_ids[] length.
**SFS dimensions wrong**: Check projection and monomorphic inclusion.
**Neo4j extremely slow on cluster**: Data dir is on NFS. Use `graphmana check-filesystem`. Move to local SSD/scratch.
**Neo4j won't start on cluster**: Java 21 not on PATH. Check `module load java/21` or equivalent. Ports 7474/7687 may be blocked — use `--neo4j-home` config to change ports.
**prepare-csv succeeds but load-csv fails**: CSV files on shared storage may have been corrupted or truncated. Verify file sizes. Re-run prepare-csv if needed.
**Out of disk on cluster scratch**: 50K WGS produces 2–3 TB total. Request sufficient scratch allocation before importing.

---

## Key Resources

- GraphPop source: `/mnt/data/GraphPop`
- cyvcf2: https://brentp.github.io/cyvcf2/
- VCF 4.5 spec: https://samtools.github.io/hts-specs/VCFv4.5.pdf
- neo4j-admin import: https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/
- PLINK 1.9/2.0 formats: https://www.cog-genomics.org/plink/1.9/formats
