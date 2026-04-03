Review the GraphMana manuscript and all paper materials for accuracy, completeness, and consistency with the current codebase state.

## Review Checklist

### 1. Manuscript Accuracy
- All numbers in the text match actual benchmark data (benchmarks/results/)
- Codebase statistics (LOC, test count, command count, format count) are current
- Project names and sample counts cited in the opening are factually correct
- No claims that contradict the code or benchmarks

### 2. Figure Accuracy
- Figure 2 benchmark bars match the JSONL data
- Figure 1 schema matches actual node types and relationships in queries.py
- Figure legends accurately describe what's shown
- Panel labels (a, b, c) match between figures and legends

### 3. Methods Section
- Software architecture description matches actual code structure
- Benchmark methodology matches what was actually run (dataset, comparison tool, hardware)
- Scalability claims match Supplementary Table 3 storage estimates
- Import/export timing numbers match benchmark data

### 4. References
- All 15 citations are cited in the text (no orphan references)
- Citation numbers in text match the reference list
- BibTeX entries have correct DOIs, years, journals

### 5. Supplementary Materials
- Supplementary Tables match benchmark data from benchmarks/results/
- Supplementary Notes match current codebase (17 formats, variant representation)
- Cross-references from main text to supplementary items are correct

### 6. Compliance with Nature Methods Brief Communication
- Abstract <= 100 words, unstructured, no citations
- Main text <= ~1,500 words
- Online Methods <= ~1,500 words with subheadings
- Display items <= 3 (figures + tables)
- References <= ~20
- No subheadings in main text body
- Competing Interests, Acknowledgements, Author Contributions present
- Data/Code Availability section present

### 7. Internal Consistency
- Numbers mentioned in abstract match numbers in main text
- Numbers in main text match numbers in figures and supplementary tables
- The 17-format claim matches the actual list-formats output
- CLI command count matches actual count

## Procedure

1. Read the manuscript, verify all quantitative claims against benchmark data
2. Cross-reference figure scripts against the data they read
3. Verify supplementary tables against benchmark JSONL
4. Check all citations are used and numbered correctly
5. Verify word counts against journal limits

## Output Format

For each issue found, report:
- **File:section** — brief description
- **Severity**: error (factual), stale (outdated number), gap (missing), style
- **Fix**: what should change

Group by severity. End with summary count and compliance checklist.
