Review the GraphMana codebase for internal consistency across modules. Focus on files with uncommitted changes (from `git diff --name-only`), but flag systemic inconsistencies across the wider codebase.

## Review Checklist

### 1. Interface Contracts
- All exporters pass the same set of kwargs to BaseExporter.__init__ (filter_config, threads, recalculate_af)
- All exporters return dicts with the same base keys (n_variants, n_samples, format, chromosomes)
- All query constants in queries.py use ACTIVE_SAMPLE_FILTER where they resolve sample IDs
- All ingest paths record provenance via ProvenanceManager

### 2. Naming Consistency
- Attribute names match across modules (e.g., `rec.chr` vs `rec.chrom`, `filter_chain.accepts()` vs `.accept()`)
- Population array field names consistent: pop_ids, ac, an, af, het_count, hom_alt_count, het_exp everywhere
- CLI option names match their internal parameter names after Click auto-conversion

### 3. Default Value Consistency
- Same defaults used in CLI, Jupyter API, and direct constructor calls
- Environment variables checked consistently (NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
- batch_size, threads, chunk_size defaults match across ingest, export, and cluster paths

### 4. Error Handling Consistency
- All exporters handle empty variant streams gracefully (not crash)
- All Neo4j queries use parameterized Cypher (no f-string injection of user values)
- Connection errors produce actionable messages pointing to --neo4j-uri or check-filesystem

### 5. Data Flow Consistency
- Packed arrays: cyvcf2 remap applied exactly once, never double-remapped
- Population stats: merge_pop_stats called with same argument order everywhere
- CSV serialization: semicolon delimiter used consistently for array fields
- Byte signedness: Java signed bytes (-128..127) in CSV, unsigned (0..255) in Python bytes

### 6. Documentation/Code Drift
- CLI --help text matches actual behavior
- Docstrings on public functions match their signatures
- CLAUDE.md schema matches actual node/relationship definitions in code

## Procedure

1. Check all exporter constructor calls in cli.py for parameter consistency
2. Grep for ACTIVE_SAMPLE_FILTER usage — verify all sample-resolving queries include it
3. Verify merge_pop_stats call signatures are consistent across incremental.py and incremental_rebuild.py
4. Check for f-string Cypher queries that interpolate user-provided values
5. Verify packed array remap is applied exactly once in each code path

## Output Format

For each issue found, report:
- **File:line** — brief description
- **Severity**: bug, inconsistency, drift
- **Fix**: what should change

Group by severity. End with summary count.
