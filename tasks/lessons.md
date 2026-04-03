# GraphMana — Lessons Learned

## How to Use This File
- After ANY correction from the user, add a new entry below
- Each entry: date, what went wrong, the pattern, the rule to prevent it
- Review this file at the start of every session
- Rules accumulate — never delete, only add

---

## Project-Specific Gotchas (Pre-populated)

### Genotype Encoding
- **cyvcf2 uses 0=HomRef, 1=Het, 2=MISSING, 3=HomAlt**
- **Our packed encoding uses 0=HomRef, 1=Het, 2=HomAlt, 3=MISSING**
- The remap is: `remap = np.array([0, 1, 3, 2], dtype=np.uint8)`
- If you see HomAlt and Missing swapped in output, the remap is wrong or missing
- ALWAYS verify with a known-genotype test fixture

### cyvcf2 Memory
- numpy arrays from cyvcf2 are backed by C memory
- When the variant object goes out of scope, the array becomes garbage
- ALWAYS copy: `gt = np.array(variant.gt_types)` NOT `gt = variant.gt_types`
- Segfaults in production almost always trace back to this

### Neo4j Property Names
- GraphPop uses `variantId`, `sampleId`, `populationId`, `chromosomeId`, `geneId`
- NOT `id` — the property names must match exactly for GraphPop compatibility
- After any schema change, verify against /mnt/e/GraphPop source

### PLINK BED vs Our gt_packed
- PLINK BED uses a DIFFERENT 2-bit encoding than our gt_packed
- Our: 00=HomRef, 01=Het, 10=HomAlt, 11=Missing
- PLINK: 00=HomAlt, 01=Missing, 10=Het, 11=HomRef (reversed bit order too)
- Export must convert, not copy directly

### Neo4j Bulk Import
- Array delimiter must be semicolon: --array-delimiter=";"
- Packed arrays serialize as signed Java bytes (-128 to 127), use np.int8
- CSV headers: :ID, :LABEL, :TYPE, :START_ID, :END_ID are special suffixes
- Property types: :INT, :LONG, :FLOAT, :BOOLEAN, :byte[]

### Parallel Output
- ALWAYS test: --threads 1 and --threads 4 must produce identical output
- Sort CSV chunks before concatenation to ensure deterministic order
- Race conditions in file writing are the most common cause of divergence

### Neo4j on NFS
- Neo4j performs terribly on network filesystems (NFS, Lustre, GPFS)
- ALWAYS use local SSD or node-local scratch for Neo4j data directory
- graphmana check-filesystem should catch this

### VCF Position Sorting
- The NEXT chain builder assumes position-sorted VCF input
- bcftools and GATK output are sorted by default
- If input is unsorted, the NEXT chain will have wrong distance_bp values
- Add a sort-order check before starting import

---

## Session Corrections

### 2026-03-11: Parallel export serialization must include ALL ExportFilterConfig fields
- **Bug**: `sv_types` was missing from `_get_filter_config_dict()` in parallel.py
- **Impact**: `--filter-sv-type` silently dropped when `--threads > 1`
- **Rule**: When adding a new field to ExportFilterConfig, ALWAYS also add it to
  `parallel.py::_get_filter_config_dict()` — it serializes configs for subprocess pickling

### 2026-03-11: Keep password defaults consistent across CLI and Jupyter API
- **Bug**: CLI used `"graphmana"` default, Jupyter API used `"password"`
- **Rule**: Both should read from `GRAPHMANA_NEO4J_PASSWORD` env var with same fallback

### 2026-03-11: Summary dicts must always include n_samples for CLI output consistency
- **Pattern**: FAST PATH exporters (TreeMix, SFS, BED, TSV, JSON) omitted `n_samples`
  from their return dicts, while FULL PATH exporters included it
- **Rule**: Every exporter.export() return dict must include `n_samples`

### 2026-03-12: Phase bit convention must be consistent across ALL exporters
- **Bug**: Beagle, HAP, and STRUCTURE exporters had phase bit convention reversed
  relative to VCF exporter and the packer. `phase_bit=1` means ALT on second
  haplotype (REF|ALT = 0|1 in VCF). Three exporters had it backwards.
- **Rule**: `phase_bit=1` → ALT on second haplotype. `phase_bit=0` (at a het) → ALT
  on first haplotype. When writing a new exporter, verify against `vcf_export.py:format_gt`
  as the canonical reference.

### 2026-03-12: VariantRecord attribute is `.chr`, not `.chrom`
- **Bug**: `import_filters.py` used `rec.chrom` in `_check_contig` and `_check_region`,
  but `VariantRecord` has `.chr`. Crashes whenever `--filter-contigs` or `--filter-region`
  is used during import.
- **Rule**: Always check the actual dataclass/namedtuple definition when using attributes.

### 2026-03-12: Method names must match — `accepts()` not `accept()`
- **Bug**: `incremental.py` called `filter_chain.accept()` but the method is `accepts()`.
- **Rule**: Use IDE or grep to verify method names exist before calling.

### 2026-03-12: Harmonic numbers for Watterson's estimator are NOT simple arithmetic
- **Bug**: `sample/manager.py` computed `a_n = 2*n` and `a_n2 = (2*n)^2` instead of
  harmonic sums: `a_n = sum(1/i for i in 1..2n-1)`, `a_n2 = sum(1/i^2 for i in 1..2n-1)`.
- **Rule**: Always verify population genetics formulas against a textbook or reference.
  The initial import path (`csv_emitter.py`) had the correct `_harmonic()` functions.

### 2026-03-12: Verify magic numbers against format specifications
- **Bug**: BGEN exporter used `b"\x00\x00\x00\x00"` instead of `b"bgen"` for magic bytes.
- **Rule**: When implementing binary format writers, verify magic numbers, offsets, and
  field sizes against the official specification document.

### 2026-03-12: bcftools flags must match the actual CLI help
- **Bug**: normalizer.py used `-D` for bcftools norm but this flag doesn't exist.
- **Rule**: Run `bcftools norm --help` or check docs before adding flags.

### 2026-03-12: CLI parameters must be wired through to their consumers
- **Bug**: `--recalculate-af` was accepted by the CLI but never passed to any exporter.
- **Rule**: After adding a CLI option, grep for the variable name and verify it reaches
  the function that uses it. Every parameter must have a consumer.

### 2026-03-12: FAST PATH exporters must not load sample metadata
- **Bug**: TreeMix, BED, SFS, TSV, JSON exporters called `_load_samples()` just
  for `n_samples` in the return dict, which fetches all sample metadata from Neo4j.
- **Rule**: Use `_get_sample_count()` for FAST PATH exporters that only need the count.
  Only call `_load_samples()` when per-sample data (packed_index, sampleId) is needed.

### 2026-03-12: Merger must update per-pop counts after skipping duplicate samples
- **Bug**: `_source_pop_n_samples` retained original source DB counts even when samples
  were skipped due to `--on-duplicate-sample skip`. HomRef extension used wrong AN values.
- **Rule**: After filtering source samples, update `_source_pop_n_samples` to reflect
  effective (non-skipped) per-population counts.

### 2026-03-12: Annotation replace mode must clean up orphan Gene nodes
- **Bug**: `load()` in "replace" mode deleted HAS_CONSEQUENCE edges but did NOT run
  `DELETE_ORPHAN_GENES`, leaving Gene nodes with no incoming edges.
- **Rule**: Whenever deleting HAS_CONSEQUENCE edges, always clean up orphan Gene nodes
  afterward. Both `remove()` and `load(mode="replace")` must do this.

### 2026-03-12: Cohort queries must auto-filter excluded (soft-deleted) samples
- **Bug**: `resolve_sample_ids()` ran user's Cypher query directly, returning samples
  with `excluded=true` alongside active ones.
- **Rule**: Any method that resolves sample IDs for operational use must post-filter
  to exclude soft-deleted samples (where `excluded IS NULL OR excluded = false`).

### 2026-03-12: Container memory detection must use cgroup limits
- **Bug**: `auto_memory_config()` read `/proc/meminfo` which shows host memory, not the
  container's cgroup limit. Neo4j would be configured with more RAM than allocated.
- **Rule**: Check cgroup v2 (`memory.max`) and v1 (`memory.limit_in_bytes`) before
  falling back to `/proc/meminfo` or `sysctl`.

### 2026-03-16: Five Cypher queries touching :Sample lacked soft-delete filter
- **Bug**: `FETCH_MAX_PACKED_INDEX`, `FETCH_EXISTING_SAMPLE_IDS` (queries.py),
  both inline copies in `pipeline.py`, and `hard_remove` count in `sample/manager.py`
  queried all Sample nodes without filtering `excluded=true`.
- **Impact**: Incremental import could reuse packed_index slots from excluded samples,
  duplicate detection reported false positives for excluded samples, and hard_remove
  used wrong total count for call_rate recalculation.
- **Fix**: Added `ACTIVE_SAMPLE_FILTER` constant to queries.py, refactored all 8 existing
  hand-written filters to use it, fixed 5 missing filters.
- **Safeguard**: `test_structural_safeguards.py::TestSoftDeleteEnforcement` scans all
  source files for MATCH (:Sample) queries and fails if any new query lacks the filter.
- **Rule**: Every new Cypher query that touches :Sample MUST use `ACTIVE_SAMPLE_FILTER`
  from db.queries unless it's an admin query (add to whitelist in tests).

### 2026-03-16: 29 GraphManaConnection() calls lacked database= parameter
- **Bug**: 29 `GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password)` calls in cli.py
  omitted `database=database`, making all those commands hard-coded to the default database.
- **Fix**: Added `--database` option (default=DEFAULT_DATABASE) to all 25 affected functions
  (29 call sites). Special case: `merge` needs both `--database` (target) and `--source-database`.
- **Rule**: Every new CLI command that opens a `GraphManaConnection` MUST accept `--database`
  and forward it with `database=database`.

### 2026-03-17: Systematic audit — 10 bugs across parameter flow, defaults, and logic consistency
- **Bug 1 (P0)**: `recalculate_af` conditional default resolved INSIDE exporter constructors
  via `bool(recalculate_af)`, meaning `None` → `False` instead of `None` → `bool(populations)`.
  Fix: Resolve to bool BEFORE ExportFilterConfig construction in cli.py.
- **Bug 2 (P0)**: Migration version update used bare `execute_write` (no auto-retry) instead
  of `execute_write_tx` (managed transaction). Fix: Wrap in `execute_write_tx`.
- **Bug 3 (P1)**: `hard_remove()` deleted populations but never decremented `n_populations`
  on SchemaMetadata. Fix: Count deleted populations and update metadata.
- **Bug 4 (P1)**: `--include-filtered` accepted by VCF parser but never wired to CLI
  `prepare-csv` or `ingest` commands. Fix: Add option + forward to both commands.
- **Bug 5 (P1)**: 5 commands used bare `--verbose` (is_flag=True) instead of `--verbose/--quiet`
  (liftover, migrate, setup-neo4j, neo4j-start, neo4j-stop). Fix: Standardize all to `--verbose/--quiet`.
- **Bug 6 (P2)**: `liftover_status` property on variants had no export filter. Fix: Add
  `--filter-liftover-status` to export + ExportFilterConfig + ExportFilter + parallel serialization.
- **Bug 7 (P2)**: `_maybe_recalculate_af` only called by VCF, PLINK, TreeMix exporters. 10 of
  14 FULL PATH exporters missed it. Fix: Add `_maybe_recalculate_af` to BaseExporter, call
  in all 10 missing exporters.
- **Bug 8 (P3)**: PLINK2 exporter's `--threads` option silently ignored (PgenWriter needs
  variant_ct upfront). Fix: Add warning when threads > 1, document limitation.
- **Bug 9 (P3)**: Zarr, GDS, BGEN exporters buffered ALL variant data into memory before
  writing. Fix: Refactor to count-first-then-stream pattern (two-pass).
- **Bug 10 (P3)**: Snapshot restore didn't check if Neo4j was running. `neo4j-admin load`
  fails on running instance with cryptic error. Fix: Add `_is_neo4j_running()` PID check.
- **Rule**: After implementing any feature, audit it against: (1) does every accepted
  parameter have a consumer? (2) are defaults consistent across entry points? (3) does the
  new code integrate with ALL existing infrastructure (filters, parallel, provenance)?

### 2026-03-17: Click boolean flags use `secondary_opts`, not `secondary`
- **Bug**: Test used `p.secondary` to check --verbose/--quiet pairing, but Click `Option`
  objects use `p.secondary_opts` (a list like `['--quiet']`).
- **Rule**: When introspecting Click internals, verify attribute names against the Click source.

### 2026-03-17: `inspect.getsource(cli)` fails on Click Group objects
- **Bug**: `cli` is a Click `Group` instance, not a module. `inspect.getsource()` on it
  returns only the decorated function, not the full module source.
- **Fix**: Import the module (`from graphmana import cli as cli_module`) and inspect that.
- **Rule**: When using `inspect.getsource()`, pass the module object to get full file source,
  or pass a specific function/method to get that function's source.

### 2026-03-23: cyvcf2.seqlens raises AttributeError when VCF lacks contig lengths
- **Bug**: `vcf_parser.py` called `vcf_tmp.seqlens` which raises `AttributeError` when
  the VCF header has `##contig=<ID=chr22>` without a `length` field.
- **Fix**: Wrap in try/except, fall back to empty list, fill from CHR_LENGTHS.
- **Rule**: cyvcf2 properties can raise AttributeError, not just return None/empty.

### 2026-03-23: Neo4j user-space start requires server.directories.run override
- **Bug**: Starting Neo4j as non-`neo4j` user fails with `AccessDeniedException` on
  `/usr/share/neo4j/run` which is owned by `neo4j` user.
- **Fix**: Set `server.directories.run` to a user-writable temp directory in neo4j.conf.
- **Rule**: When starting Neo4j in user space, ALL directory settings (data, logs, run,
  transaction logs) must point to user-writable paths.

### 2026-03-23: kwargs forwarding must exclude explicitly-passed arguments
- **Bug**: `_worker_prepare_csv_region()` passed `region=chromosome` explicitly but
  `**kwargs` already contained `region=None` from the caller, causing "got multiple
  values for keyword argument 'region'".
- **Fix**: Strip `region` and `threads` from kwargs before forwarding to worker.
- **Rule**: When forwarding `**kwargs` to a function while also passing explicit args,
  always remove those keys from kwargs first.

### 2026-03-23: VCF panel maps fewer samples than VCF contains
- **Pattern**: 1KGP CCDG VCF has 3,202 samples but the population panel only maps
  2,504. GraphMana stores gt_packed for all 3,202 (needed for genotype fidelity) but
  only creates Sample nodes for the 2,504 with population assignments.
- **Implication**: Export sample counts may differ from DB Sample node count. VCF/PLINK
  exports include all gt_packed samples; population-level exports use only mapped ones.
- **Rule**: Never assume n_samples_in_db == n_samples_in_gt_packed. Test both.

### 2026-03-23: Integration test fixtures should be session-scoped and shared
- **Pattern**: Each test class had its own class-scoped fixture doing prepare-csv + import
  (15 min each). Four classes = 60 min of redundant CSV generation.
- **Fix**: Use session-scoped fixture that runs once, shared across all test classes.
- **Rule**: For expensive fixtures (minutes+), use session scope. For cheap fixtures
  (seconds), class or function scope is fine.

### 2026-03-24: Full-genome validation must avoid reading entire large files
- **Bug**: `wc -l` on 215 GB CSV timed out (600s); Python `sum(1 for _ in f)` on
  70M gzipped lines would take 30+ min; reading 70M BED lines to collect chromosomes
  also extremely slow.
- **Fix**: Increase `wc -l` timeout to 1800s. For gzipped files, validate header +
  first few lines + file size instead of line count. For BED, use head/tail for
  chromosome sampling.
- **Rule**: Never iterate all lines in files >10 GB for validation. Use file size,
  `wc -l` (with adequate timeout), or sample head/tail instead.

### 2026-03-24: Neo4j pagecache must be proportional to database size
- **Bug**: Full genome DB (~150-200 GB on disk) allocated only 8 GB pagecache.
  Neo4j thrashed on disk I/O, and export processes were OOM-killed (SIGKILL -9)
  as the OS ran out of memory from excessive page faults.
- **Fix**: Allocate pagecache proportional to DB size. For 200 GB DB on a 56 GB
  machine: heap=4g + pagecache=20g = 24 GB, leaving 32 GB for OS.
- **Rule**: Neo4j pagecache should be at least 10-15% of the database store size
  for reasonable performance. For cold-cache exports, more is better.

### 2026-03-24: prepare-csv only processed the first VCF file
- **Bug**: CLI accepted multiple `--input` files but only processed `all_inputs[0]`,
  printing a "multi-file support coming soon" warning.
- **Fix**: Implemented `run_prepare_csv_multifile()` in parallel.py — processes N files
  concurrently using ProcessPoolExecutor(max_workers=threads).
- **Impact**: 22 single-chromosome VCFs processed in ~3.75 hours (8 workers) instead
  of sequential 6-8 hours.
- **Rule**: When accepting multiple inputs in CLI, verify ALL inputs are actually processed.

### 2026-03-28: Neo4j RETURN v fetches ALL properties — use property-selected queries

- **Bug**: All export queries used `RETURN v` which returns entire Variant nodes including
  gt_packed (800+ bytes), phase_packed (400+ bytes), ploidy_packed (400+ bytes). For 70.7M
  variants at ~1,600 bytes each = 112 GB in Neo4j driver buffers → OOM kill.
- **Fix**: Added three query tiers: FAST (pop arrays only, ~300 bytes), GENOTYPES (packed
  arrays + metadata, no pop arrays), FULL (legacy). FAST PATH exports (TreeMix, SFS, BED,
  TSV) now fetch only what they need.
- **Rule**: Never use `RETURN v` for large-scale queries. Always select specific properties.

### 2026-03-28: ORDER BY on millions of rows causes Neo4j GC pauses

- **Bug**: `ORDER BY v.pos` on chromosomes with 5-6M variants caused Neo4j JVM to
  allocate a sort buffer exceeding heap, triggering 240s stop-the-world GC pauses that
  killed the Bolt connection.
- **Fix**: (1) Skip ORDER BY for aggregation exports that don't need positional order
  (TreeMix, SFS). (2) Batched pagination for ordered exports: query 500K variants at a
  time with `WHERE v.pos > $last_pos ... LIMIT $batch_size`.
- **Rule**: Never ORDER BY on more than ~1M rows without pagination. Use LIMIT + cursor.

### 2026-03-28: SFS exporters must not accumulate all variant dicts in memory

- **Bug**: SFS dadi and SFS fsc built `all_variants: list[dict]` with 70M+ entries
  (~21 GB), causing OS to kill Neo4j to free memory.
- **Fix**: Accumulate SFS bins (numpy array, ~1 KB) directly while streaming variants.
  Memory usage dropped from 21 GB to <100 MB.
- **Rule**: Never accumulate variant-level data in lists for whole-genome operations.
  Stream and aggregate incrementally.

### 2026-03-29: SFS --sfs-folded flag required when no ancestral allele information

- **Bug**: SFS fsc benchmark reported 0 variants. Default is `polarized=True` (unfolded),
  which requires `is_polarized=True` on each variant. Without ancestral allele FASTA
  during import, `is_polarized` is NULL → all variants skipped.
- **Fix**: Add `--sfs-folded` flag to benchmark commands. Not a code bug.
- **Rule**: When testing SFS exports, always verify the polarized/folded setting matches
  the available data. Most datasets without ancestral allele info need `--sfs-folded`.

(Older entries go here)
