Review the GraphMana codebase for correctness, consistency, and adherence to project standards. Focus on files with uncommitted changes (from `git diff --name-only` and `git diff --cached --name-only`), but flag cross-cutting issues that affect the wider codebase.

## Review Checklist

### 1. Parameter Flow & Wiring
- Every CLI option has a consumer — no silently ignored parameters
- New filter/config fields are in `parallel.py::_get_filter_config_dict()`
- Options wired to ALL relevant exporter/ingester constructors in `cli.py`
- Defaults defined once in `config.py`, consistent across CLI and Jupyter API

### 2. Packed Array Correctness
- cyvcf2 remap: 00=HomRef, 01=Het, 10=HomAlt, 11=Missing
- Phase convention matches `vcf_export.py:format_gt` as canonical reference
- Byte arrays use signed Java bytes (np.int8, -128 to 127)
- CSV uses semicolon-delimited signed bytes

### 3. FAST PATH vs FULL PATH
- FAST PATH exporters (TreeMix, SFS, BED, TSV freq) use pop arrays, never unpack gt_packed
- FAST PATH exporters use `_get_sample_count()` not `_load_samples()` for counts
- FULL PATH exporters (VCF, PLINK, EIGENSTRAT, etc.) unpack correctly

### 4. Export Return Dicts
- All exporters return `n_samples`, `n_variants`, `format`, `chromosomes`

### 5. Soft-Delete Awareness
- Queries resolving sample IDs filter `WHERE s.excluded IS NULL OR s.excluded = false`
- Cohort queries from user Cypher are post-filtered

### 6. Operational Invariants
- Relationship deletions clean up orphaned nodes
- Database-modifying operations record IngestionLog via ProvenanceManager
- Derived counts updated after filtering/skipping items
- Population genetics formulas verified against references

### 7. Code Quality
- Python: Black-compatible (line length 100), type hints, no unused imports
- Java: Google Java Style, no Lombok, branchless bit operations
- No statistical procedures in GraphMana (those belong to GraphPop)
- No version markers like "(v0.5)" in CLI help strings

### 8. Cross-Cutting Concerns
- Attribute/method names match actual definitions (grep to confirm)
- No hardcoded paths or credentials
- Error messages are actionable
- Logging is appropriate (not too verbose, not silent on errors)

## Output Format

For each issue found, report:
- **File:line** — brief description of the issue
- **Severity**: bug, consistency, style, or suggestion
- **Fix**: what should change

Group issues by severity (bugs first). End with a summary count: N bugs, N consistency issues, N style issues, N suggestions.
