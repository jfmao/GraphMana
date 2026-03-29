# AI Agent Integration

GraphMana exposes its database to AI agents via two integration paths:
**MCP (Model Context Protocol)** for Claude Desktop / Claude Code, and
**ToolUniverse** for the Harvard Zitnik Lab multi-tool agent framework.

Both paths wrap the `graphmana-py` Python client with zero duplication.

---

## MCP Server

### Installation

```bash
cd graphmana-mcp
pip install -e .
```

### Claude Desktop Configuration

Add to `~/.config/claude/claude_desktop_config.json` (Linux) or
`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS):

```json
{
  "mcpServers": {
    "graphmana": {
      "command": "graphmana-mcp",
      "env": {
        "GRAPHMANA_NEO4J_URI": "bolt://localhost:7687",
        "GRAPHMANA_NEO4J_PASSWORD": "graphmana"
      }
    }
  }
}
```

### Claude Code Configuration

Add to `.claude/settings.json`:

```json
{
  "mcpServers": {
    "graphmana": {
      "command": "graphmana-mcp",
      "env": {
        "GRAPHMANA_NEO4J_URI": "bolt://localhost:7687",
        "GRAPHMANA_NEO4J_PASSWORD": "graphmana"
      }
    }
  }
}
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GRAPHMANA_NEO4J_URI` | `bolt://localhost:7687` | Neo4j Bolt URI |
| `GRAPHMANA_NEO4J_USER` | `neo4j` | Neo4j username |
| `GRAPHMANA_NEO4J_PASSWORD` | `graphmana` | Neo4j password |

### Available Tools (15)

| Tool | Description | Access Path |
|------|-------------|-------------|
| `graphmana_status` | Database summary (node counts, schema) | — |
| `graphmana_samples` | List all samples | — |
| `graphmana_populations` | List populations with counts | — |
| `graphmana_chromosomes` | List chromosomes with variant counts | — |
| `graphmana_variants` | Variants by chromosome/region | FULL |
| `graphmana_filtered_variants` | Variants matching filters | FULL |
| `graphmana_genotype_matrix` | Samples-by-variants matrix | FULL |
| `graphmana_allele_frequencies` | Per-population allele frequencies | FAST |
| `graphmana_gene_variants` | Variants by gene symbol | FULL |
| `graphmana_annotated_variants` | Variants by annotation version | FULL |
| `graphmana_annotation_versions` | List annotation versions | — |
| `graphmana_cohorts` | List cohort definitions | — |
| `graphmana_cohort_samples` | Samples in a cohort | — |
| `graphmana_export` | Export to file (VCF, PLINK, etc.) | varies |
| `graphmana_query` | Run arbitrary Cypher | — |

FAST PATH tools read pre-computed population arrays and run in seconds at
any sample count. FULL PATH tools unpack genotype arrays and scale linearly
with sample count.

### Usage Examples

**Ask Claude:** "What populations are in the GraphMana database?"
→ Claude calls `graphmana_populations` and summarizes the result.

**Ask Claude:** "Show me high-impact variants in BRCA1"
→ Claude calls `graphmana_gene_variants(gene_symbol="BRCA1")` then filters.

**Ask Claude:** "Export chromosome 22 to PLINK format"
→ Claude calls `graphmana_export(format="plink", output_path="chr22", chr="22")`.

**Ask Claude:** "What's the allele frequency of rs12345 across populations?"
→ Claude calls `graphmana_query` with a Cypher query matching the variant ID.

---

## ToolUniverse Integration

[ToolUniverse](https://github.com/zou-group/tooluniverse) is a framework for
LLM agents to discover and use 2,100+ scientific tools.

### Files

- `graphmana-mcp/tooluniverse/graphmana_tools.json` — Tool definitions (5 tools)
- `graphmana-mcp/tooluniverse/graphmana_tool.py` — Python implementation

### Tools

| Tool | Description |
|------|-------------|
| `GraphMana_QueryVariants` | Query variants by region, gene, consequence, MAF |
| `GraphMana_GetSamples` | Retrieve sample/population/cohort metadata |
| `GraphMana_AlleleFrequencies` | Per-population allele frequencies (FAST PATH) |
| `GraphMana_GenotypeMatrix` | Genotype matrix extraction (FULL PATH) |
| `GraphMana_ExportData` | Export to VCF, PLINK, TreeMix, SFS, etc. |

### Setup

1. Install `graphmana-py` (provides `GraphManaClient`)
2. Copy `graphmana_tools.json` and `graphmana_tool.py` to your ToolUniverse tools directory
3. Set environment variables for Neo4j connection
4. The tools are auto-discovered by ToolUniverse agents

### Calling Convention

```python
from tooluniverse.graphmana_tool import run

result = run("GraphMana_QueryVariants", {
    "gene": "BRCA1",
    "impact": "HIGH",
})
# result = {"result": [...], "success": True}
```
