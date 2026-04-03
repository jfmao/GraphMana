Review the GraphMana test suite for correctness, coverage gaps, and missing edge cases. Focus on tests for files with uncommitted changes (from `git diff --name-only`), but flag systemic test gaps across the codebase.

## Review Checklist

### 1. Test Correctness
- Assertions actually verify the behavior described in the test name/docstring
- Mock setups match real interfaces (method names, return types, argument signatures)
- No tests that always pass regardless of implementation (tautological assertions)
- No tests that rely on implementation details that could change (brittle tests)

### 2. Coverage Gaps
- Every public function/method in changed files has at least one test
- Error/exception paths are tested, not just happy paths
- Boundary conditions tested: empty inputs, single element, maximum sizes
- For exporters: return dict structure validated (n_samples, n_variants, format, chromosomes)

### 3. Edge Cases for GraphMana-Specific Logic
- Packed array operations: test with 0 samples, 1 sample, 4 samples (byte boundary), 5 samples (crosses byte)
- Phase convention: test phase_bit=0 and phase_bit=1 at het sites
- Missing data (genotype 11/Missing): propagated correctly through all paths
- Soft-deleted samples: excluded from queries and exports
- FAST PATH vs FULL PATH: correct path chosen per exporter
- Population arrays: empty populations, single population, mismatched lengths
- Incremental operations: existing samples unchanged, new samples appended correctly
- Filter chains: combined filters, no filters, all-reject filters

### 4. Test Quality
- Tests are independent (no order dependency between test functions)
- Fixtures are minimal and clearly named
- No hardcoded file paths that only work on one machine
- Parameterized tests used where multiple inputs test the same logic
- Test file naming follows `test_<module>.py` convention

### 5. Integration Test Gaps
- Round-trip tests: ingest then export, verify data survives
- Cross-module interactions: filter + export, incremental + export
- CLI integration: Click commands invoked via CliRunner

## Procedure

1. List all test files under `graphmana-cli/tests/`
2. For each changed source file, find its corresponding test file
3. For changed source files WITHOUT a test file, flag as a coverage gap
4. For existing test files, review against the checklist above
5. Check that test fixtures exist and are sufficient

## Output Format

For each issue found, report:
- **File:line** (or **Missing: module_name**) — brief description
- **Severity**: gap (missing test), bug (incorrect test), quality (improvement)
- **Recommendation**: what test to add or fix

Group by severity (gaps first, then bugs, then quality). End with a summary: N coverage gaps, N test bugs, N quality issues.
