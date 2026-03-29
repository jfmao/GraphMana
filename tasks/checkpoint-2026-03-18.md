# GraphMana Checkpoint — 2026-03-18

## Project State Summary

| Metric | Value |
|--------|-------|
| Python CLI tests | 1217 passing |
| Jupyter API tests | 74 passing |
| Java tests | ~96 passing |
| Linting | black + ruff clean |
| CLAUDE.md | 618 lines (trimmed from 967) |
| Python source files | ~75 files, ~17,500 lines |
| Export formats | 20+ (all implemented) |

## Completed This Session (2026-03-18)

1. **CLAUDE.md aggressive trim**: 967→618 lines (-36%)
   - Removed: Implementation Priority (duplicates todo.md), Project Layout Rules, API/Tool Integration Rules, Paper Showcase, Scalability Messages for Paper
   - Compressed: Codebase Quality Rules, Cluster section, Key Resources
   - Updated: Tier 3 exports (no longer "Future")

2. **Previous session work carried forward**:
   - 10-bug systematic audit (all fixed, safeguard tests added)
   - `--database` added to all 29 CLI commands
   - lessons.md updated with 20+ entries

## Pending Work (Next Session)

### Immediate: Full 1KGP Genome Benchmark
Plan written but NOT YET APPROVED. Plan file: see below.

**6-phase benchmark plan**:
1. `prepare-csv` all 22 autosomes (3-5 hours, ~200-250 GB output)
2. Neo4j config tune (pagecache 28g) + `load-csv` (30-60 min)
3. FAST PATH exports: SFS, TreeMix, BED, TSV, JSON (minutes)
4. FULL PATH exports: VCF, PLINK, EIGENSTRAT, etc. (hours)
5. Population-filtered exports (European VCF, East Asian TreeMix)
6. Rice 3K chr1: load existing CSVs + exports

**Total**: ~12-20 hours, ~500 GB disk

### Later
- Version tags: v0.1.0, v0.5.0, v0.9.0, v1.0.0
- Nature Methods manuscript
- Integration test with real data (formal)

## Data Symlinks (WILL BREAK on path change)

```
data/1000g/vcf/ALL.chr*.vcf.gz
  → /mnt/e/GraphPop/data/raw/1000g/vcf/ALL.chr*.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz

data/rice3k/vcf/NB_final_snp.vcf.gz
  → /mnt/e/GraphPop/data/raw/3kRG_data/NB_final_snp.vcf.gz
```

Re-create these after moving to new path.

## Existing Benchmark Results (preserved)

| Result | Size | Status |
|--------|------|--------|
| `benchmarks/results/1kgp_chr22_csv/` | 2.9 GB | Complete |
| `benchmarks/results/1kgp_chr22_exports/` | — | Complete |
| `benchmarks/results/rice3k_chr1_csv/` | 9.4 GB | CSVs done, NOT loaded |
| `benchmarks/released/1kgp_chr22_benchmark.md` | — | Published |

## After-Move Checklist

1. Update paths in `CLAUDE.md` (search `/mnt/e/`)
2. Re-create data symlinks to GraphPop data
3. `cd NEW_PATH/graphmana-cli && pip install -e ".[dev]"`
4. `pytest -v` — expect 1217 tests
5. `black --check src/ tests/ && ruff check src/ tests/`
6. Tell Claude: "Continue with the 1KGP full genome benchmark plan"

## Key Architectural Context (for quick ramp-up)

- **Packed arrays**: 2-bit genotypes on Variant nodes (NOT CARRIES edges)
- **FAST PATH**: pre-computed pop arrays (ac/an/af) → seconds at any N
- **FULL PATH**: unpack gt_packed → linear in N, parallel by chromosome
- **cyvcf2 remap**: `[0,1,3,2]` — HomAlt/Missing swapped from cyvcf2 convention
- **Soft delete**: `Sample.excluded=true`, ALL queries must use `ACTIVE_SAMPLE_FILTER`
- **Every CLI command**: must have `--database`, `--verbose/--quiet` (not bare `--verbose`)
