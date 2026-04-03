Review the GraphMana documentation for accuracy, completeness, and consistency with the actual codebase. Check all docs, vignettes, and command references.

## Review Checklist

### 1. CLI Command Documentation vs Actual CLI
- Every documented command exists in cli.py
- Every documented option exists with the correct name, type, and default
- No undocumented commands or options that should be documented
- Help text matches documented descriptions

### 2. Vignette Accuracy
- Code examples in vignettes use correct command syntax
- Option names match actual CLI (no typos, no renamed options)
- Example outputs are plausible given the described inputs
- Workflow sequences are logically correct (e.g., ingest before export)

### 3. Schema Documentation vs Code
- Node types documented in docs match actual schema in queries.py/csv_emitter.py
- Relationship types match actual code
- Property names on nodes match actual Cypher queries
- No deprecated schema elements still documented

### 4. Format Specifications
- Export format descriptions match actual exporter behavior
- FAST PATH vs FULL PATH classification is correct per exporter
- File extensions and output structure match actual output

### 5. Cross-Reference Consistency
- README references match actual file paths
- Internal doc links are not broken
- Version numbers are consistent across docs

## Procedure

1. List all doc files under docs/ and paper/
2. For each vignette, cross-reference command examples against cli.py --help output
3. For command reference docs, verify every option against the actual Click decorators
4. Check schema docs against queries.py node/relationship definitions
5. Flag stale, incorrect, or missing documentation

## Output Format

For each issue found, report:
- **File:line** — brief description
- **Severity**: error (wrong info), stale (outdated), gap (missing docs)
- **Fix**: what should change

Group by severity. End with summary count.
