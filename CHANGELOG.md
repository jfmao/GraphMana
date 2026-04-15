# GraphMana Changelog

## v1.1.0 — 2026-04-15

### Schema changes
- **New property `called_packed` on every `Variant` node** (1 bit per sample,
  LSB-first, `ceil(N/8)` bytes). Bit = 1 means the sample was interrogated at
  this site; bit = 0 means the sample was not looked at. Distinguishes
  "called HomRef" from "not interrogated", which plain VCF cannot express.
- **New property `gt_encoding` on every `Variant` node** (`"dense"` or
  `"sparse_v1"`). Selects the decoder path for `gt_packed`. Defaults to
  `"dense"`, matching v1.0.
- **Optional sparse `gt_packed` storage.** Per-variant tagged blob format:
  leading byte `0x00` = dense (identical to v1.0 bytes), `0x01` = sparse
  (header + non-reference sample indices + packed 2-bit codes). Chosen
  automatically when it yields a smaller encoded size. Legacy untagged
  v1.0 blobs are accepted on read without migration.
- `SchemaMetadata.schema_version` bumps from `"1"` to `"1.1"`.

### Correctness: HomRef-vs-Missing across incremental batches
- Plain VCFs are ambiguous when a position is absent: it can mean "everyone
  is HomRef" or "nobody was interrogated." v1.0 treated the absence as
  HomRef, which silently biased per-population allele frequencies, skewed
  the site frequency spectrum, and could create batch-correlated principal
  components.
- v1.1 preserves the distinction end-to-end via `called_packed`. Incremental
  ingestion now pads existing samples as **Missing** (not HomRef) at variants
  introduced by a later batch, and their `called_packed` bits are cleared.
  Per-population `an[]` excludes not-interrogated samples honestly.
- `graphmana ingest --assume-homref-on-missing` opts back into the v1.0
  behavior for workflows with a guaranteed fixed site list (imputed panels,
  SNP arrays, pre-phased reference cohorts). Emits a loud log warning.
- Java plugin (`PackedGenotypeReader`, `SampleSubsetComputer`,
  `IncrementalExtendProcedure`) honors `called_packed` in subset statistics
  and incremental extension procedures. New parameter
  `assumeHomrefOnMissing` on `graphmana.extendHomRef` (defaults to `true`
  for backward compatibility with v1.0 clients).

### Exporters
- FULL PATH exporters (VCF, PLINK, PLINK2, EIGENSTRAT, Beagle, STRUCTURE,
  Genepop, HAP, BGEN, GDS, Zarr, JSON, TSV) transparently emit each format's
  missing token for samples whose `called_packed` bit is 0, without
  per-exporter code changes: the central `BaseExporter._unpack_variant_genotypes`
  coerces uncalled slots to `gt == 3` (Missing) at the unpack boundary.
- FAST PATH exporters (TreeMix, SFS-dadi, SFS-fsc2, BED, TSV) read the
  recomputed `ac[]/an[]/af[]` directly, so they get honest per-site
  denominators for free.

### Packer / array helpers
- New: `build_called_packed`, `build_called_packed_all`,
  `unpack_called_packed`, `encode_gt_blob`, `decode_gt_blob`,
  `pad_called_for_new_variant`, `extend_called_packed`,
  `concatenate_called_packed`.
- `pad_gt_for_new_variant` now pads with Missing (code 3) by default.
  Pass `assume_homref=True` to restore v1.0 HomRef padding.

### CLI
- `graphmana ingest --assume-homref-on-missing` (new, default off).
- `CREATE_VARIANT_BATCH`, `UPDATE_VARIANT_BATCH`, `UPDATE_VARIANT_HARD_DELETE_BATCH`,
  and all variant `FETCH` queries now carry `called_packed` and `gt_encoding`.

### Documentation
- New `docs/gvcf-workflow.md` — end-to-end recipe from per-sample gVCFs
  through joint calling (GenomicsDB / GLnexus) to GraphMana ingest, with
  the pop-gen rationale for `called_packed`.
- New Supplementary Note 5 in the Nature Methods paper
  (`paper/v4/supplementary.tex`) — full derivation of the HomRef-vs-Missing
  problem, the four options considered, and the chosen solution.
- Main text (`paper/v4/graphmana_nature.tex`) gains a new paragraph in the
  Results describing the correctness hazard and the `called_packed` fix,
  and cites Supplementary Note 5.
- Figure 1 panel (b) gains a `called_packed` row in the
  "Per-sample genotypes" box.
- `docs/schema.md` and `docs/vignettes/11-variant-representation.md`
  updated to describe `called_packed` and `gt_encoding`.

### Tests
- New `tests/test_called_packed_v11.py` (13 tests) covering packer
  round-trips, sparse encoding at varying densities, cross-batch padding,
  allele-stat contribution masking, and the exporter unpack path.
- Existing `tests/test_array_ops.py` and `tests/test_incremental.py`
  updated to reflect the new default (pad with Missing) and to add
  explicit legacy-mode coverage.
- All 1433 Python tests pass (+13 new). All 113 Java tests pass.

### Backward compatibility
- Schema v1.0 databases load without migration: missing `called_packed` is
  treated as "all samples called", missing `gt_encoding` is treated as
  `"dense"`, untagged `gt_packed` blobs are accepted as v1.0 dense bytes.
- `graphmana migrate --to-v1.1` back-fills an explicit all-ones
  `called_packed` mask and records the assumption in the ingestion log.
- Follow-up work tracked in `tasks/v1.1_sparse_called_packed.md`
  (remaining exporters, full F polish, expanded test coverage).
