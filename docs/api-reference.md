# GraphMana API Reference

Complete reference for the GraphMana CLI, Python API, and export formats.

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GRAPHMANA_NEO4J_PASSWORD` | `graphmana` | Neo4j password for CLI and Python API |

### Defaults (config.py)

| Constant | Value | Description |
|----------|-------|-------------|
| `DEFAULT_NEO4J_URI` | `bolt://localhost:7687` | Neo4j Bolt URI |
| `DEFAULT_NEO4J_USER` | `neo4j` | Neo4j username |
| `DEFAULT_BATCH_SIZE` | `100,000` | Variants per batch in import/export |
| `DEFAULT_ANNOTATION_BATCH_SIZE` | `10,000` | Annotations per batch |
| `DEFAULT_THREADS` | `1` | Parallel threads (set higher for multi-core) |
| `DEFAULT_DATABASE` | `neo4j` | Neo4j database name |

### Neo4j Connection Options

All commands that access Neo4j accept these options:

```
--neo4j-uri TEXT         Bolt URI (default: bolt://localhost:7687)
--neo4j-user TEXT        Username (default: neo4j)
--neo4j-password TEXT    Password (default: $GRAPHMANA_NEO4J_PASSWORD or "graphmana")
--database TEXT          Database name (default: neo4j)
```

---

## CLI Reference

### graphmana status

Show database status and node counts.

```
graphmana status [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--json` | Output as JSON |
| `--detailed` | Show detailed statistics per chromosome and population |

### graphmana ingest

Import VCF data into the graph database (combined CSV generation + Neo4j load).

```
graphmana ingest --input FILE --population-map FILE [OPTIONS]
```

**Input Options:**

| Option | Description |
|--------|-------------|
| `--input FILE` | VCF/BCF input file(s). Repeat for multiple files |
| `--input-list FILE` | Text file with one VCF path per line |
| `--population-map FILE` | Tab-separated file mapping samples to populations (PED or panel format, auto-detected) |

**Import Mode:**

| Option | Description |
|--------|-------------|
| `--mode [auto\|initial\|incremental]` | `auto`: detect from database state. `initial`: fresh import (fails if data exists). `incremental`: add new samples to existing database |
| `--on-duplicate [error\|skip]` | Behavior when a sample already exists (incremental mode) |

**Processing:**

| Option | Default | Description |
|--------|---------|-------------|
| `--batch-size INT` | 100,000 | Variants per processing batch |
| `--threads INT` | 1 | Parallel threads (parallelizes by chromosome) |
| `--normalize` | off | Run `bcftools norm` before import (left-align, split multi-allelic) |
| `--reference-fasta FILE` | — | Reference FASTA for normalization |

**Genome Configuration:**

| Option | Description |
|--------|-------------|
| `--reference TEXT` | Reference genome label (e.g., "GRCh38") |
| `--ancestral-fasta FILE` | Ancestral allele FASTA for SFS polarization |
| `--chr-style [ucsc\|ensembl\|original]` | Chromosome naming convention |
| `--chr-map FILE` | Custom chromosome name mapping file |
| `--stratify-by COLUMN` | Population map column for stratification |
| `--dataset-id TEXT` | Dataset identifier (default: VCF filename) |

**Annotation:**

| Option | Description |
|--------|-------------|
| `--vep-vcf FILE` | Pre-annotated VEP/SnpEff VCF for consequence loading |
| `--annotation-version TEXT` | Annotation version label |

**Import Filters:**

| Option | Description |
|--------|-------------|
| `--filter-min-qual FLOAT` | Minimum QUAL score |
| `--filter-min-call-rate FLOAT` | Minimum call rate (0.0-1.0) |
| `--filter-maf-min FLOAT` | Minimum minor allele frequency |
| `--filter-maf-max FLOAT` | Maximum minor allele frequency |
| `--filter-variant-type [SNP\|INDEL\|SV]` | Keep only this variant type |
| `--filter-region TEXT` | Genomic region (chr:start-end) |
| `--filter-contigs TEXT` | Comma-separated list of chromosomes to include |

**Neo4j Lifecycle:**

| Option | Description |
|--------|-------------|
| `--auto-start-neo4j` | Automatically start/stop Neo4j for this operation |
| `--neo4j-home PATH` | Neo4j installation directory (for auto-start) |
| `--neo4j-data-dir PATH` | Neo4j data directory (for auto-start) |

**Control:**

| Option | Description |
|--------|-------------|
| `--output-csv-dir PATH` | Save generated CSVs to this directory |
| `--dry-run` | Report what would be imported without executing |
| `--verbose` | Enable debug logging |
| `--quiet` | Suppress non-error output |

### graphmana prepare-csv

Generate import CSVs from VCF files without requiring Neo4j. Ideal for HPC cluster compute nodes.

```
graphmana prepare-csv --input FILE --population-map FILE --output-dir DIR [OPTIONS]
```

Accepts the same input, processing, genome, annotation, and filter options as `ingest`. Requires `--output-dir`.

### graphmana load-csv

Load pre-generated CSVs into Neo4j via `neo4j-admin import`.

```
graphmana load-csv --csv-dir DIR [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--csv-dir PATH` | Directory containing CSVs from `prepare-csv` (required) |
| `--auto-start-neo4j` | Automatically start/stop Neo4j |
| `--neo4j-home PATH` | Neo4j installation directory |
| `--neo4j-data-dir PATH` | Neo4j data directory |

### graphmana export

Export data from the graph database to various formats.

```
graphmana export --format FORMAT --output FILE [OPTIONS]
```

**Formats:** `vcf`, `plink`, `plink2`, `eigenstrat`, `treemix`, `sfs-dadi`, `sfs-fsc`, `beagle`, `structure`, `genepop`, `bed`, `hap`, `tsv`, `bgen`, `gds`, `zarr`, `json`

**General Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--threads INT` | 1 | Parallel threads (by chromosome) |
| `--recalculate-af / --no-recalculate-af` | on | Recalculate AF for exported subset |
| `--reconstruct-multiallelic / --no-reconstruct-multiallelic` | on | Merge split multi-allelic sites in VCF output |

**VCF-Specific:**

| Option | Description |
|--------|-------------|
| `--vcf-version [4.2\|4.3]` | VCF version for output header |
| `--output-type [v\|z\|b]` | Output type: `v`=VCF, `z`=bgzipped VCF, `b`=BCF |
| `--phased` | Write phased genotypes (pipe `\|` separator) |

**SFS-Specific:**

| Option | Description |
|--------|-------------|
| `--sfs-populations TEXT` | Comma-separated population list for SFS |
| `--sfs-projection TEXT` | Comma-separated projection sizes (hypergeometric downsampling) |
| `--sfs-polarized / --sfs-folded` | Polarized (ancestral/derived) or folded (major/minor) SFS |
| `--sfs-include-monomorphic` | Include monomorphic sites in SFS bins |

**Other Format-Specific:**

| Option | Description |
|--------|-------------|
| `--structure-format [onerow\|tworow]` | STRUCTURE output format |
| `--bed-extra-columns TEXT` | Additional BED columns |
| `--tsv-columns TEXT` | Comma-separated TSV column list |
| `--json-pretty` | Pretty-print JSON output |

**Export Filters:**

All import filters plus:

| Option | Description |
|--------|-------------|
| `--populations TEXT` | Comma-separated populations to include |
| `--filter-consequence TEXT` | VEP consequence type |
| `--filter-impact TEXT` | VEP impact level (HIGH, MODERATE, LOW, MODIFIER) |
| `--filter-gene TEXT` | Gene symbol |
| `--filter-cadd-min FLOAT` | Minimum CADD Phred score |
| `--filter-cadd-max FLOAT` | Maximum CADD Phred score |
| `--filter-sample-list FILE` | File with sample IDs to include |
| `--filter-cohort TEXT` | Named cohort to filter by |
| `--filter-exclude-soft-deleted` | Exclude soft-deleted samples |
| `--filter-exclude-monomorphic` | Exclude monomorphic variants in exported subset |
| `--filter-annotation-version TEXT` | Filter by annotation version |
| `--filter-sv-type TEXT` | Filter structural variant type (DEL, DUP, INV, etc.) |

### graphmana cohort

Manage named sample subsets defined by Cypher queries.

```
graphmana cohort define --name NAME --query CYPHER [--description TEXT]
graphmana cohort list
graphmana cohort show --name NAME
graphmana cohort delete --name NAME
graphmana cohort count --name NAME
graphmana cohort validate --query CYPHER
```

### graphmana sample

Manage individual samples.

```
graphmana sample remove --sample-ids ID [ID ...] [--reason TEXT]
graphmana sample remove --sample-list FILE [--reason TEXT]
graphmana sample restore --sample-ids ID [ID ...]
graphmana sample list [--population POP] [--show-excluded]
graphmana sample reassign --sample-ids ID [ID ...] --new-population POP
graphmana sample hard-remove --sample-ids ID [ID ...] [--yes]
```

| Command | Description |
|---------|-------------|
| `remove` | Soft-delete: set `excluded=true` (reversible, instant) |
| `restore` | Undo soft-delete |
| `list` | List samples with population and status |
| `reassign` | Move samples to a different population (updates pop arrays) |
| `hard-remove` | Permanently delete and rebuild all packed arrays (slow, irreversible) |

### graphmana annotate

Manage functional annotations.

```
graphmana annotate load --input FILE --version TEXT --mode [add|update|replace]
graphmana annotate list
graphmana annotate remove --version TEXT
graphmana annotate load-cadd --input FILE --version TEXT
graphmana annotate load-constraint --input FILE --version TEXT
graphmana annotate load-bed --input FILE --bed-type TYPE --version TEXT
graphmana annotate load-go --input FILE --version TEXT
graphmana annotate load-pathway --input FILE --pathway-source SOURCE --version TEXT
graphmana annotate load-clinvar --input FILE --version TEXT
```

| Mode | Description |
|------|-------------|
| `add` | Layer new annotations alongside existing ones |
| `update` | Merge with existing (overwrite conflicts) |
| `replace` | Remove all existing annotations of this source, then load |

### graphmana liftover

Transform variant coordinates between reference genome assemblies.

```
graphmana liftover --chain FILE --target-reference TEXT [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--chain FILE` | UCSC chain file (e.g., hg19ToHg38.over.chain.gz) |
| `--target-reference TEXT` | Target reference label (e.g., "GRCh38") |
| `--reject-file FILE` | Write unmappable variants to this file |
| `--update-annotations` | Update gene coordinates after liftover |
| `--backup-before` | Create snapshot before modifying |
| `--dry-run` | Report mapping statistics without applying |
| `--threads INT` | Parallel threads |

### graphmana snapshot

Database backup and restore.

```
graphmana snapshot create --name NAME [--neo4j-home PATH] [--snapshot-dir PATH]
graphmana snapshot list [--snapshot-dir PATH]
graphmana snapshot restore --name NAME [--neo4j-home PATH] [--snapshot-dir PATH]
graphmana snapshot delete --name NAME [--snapshot-dir PATH]
```

### graphmana provenance

View import and modification history.

```
graphmana provenance list [--json]
graphmana provenance show --log-id ID [--json]
graphmana provenance headers [--json]
graphmana provenance summary [--json]
```

### graphmana qc

Quality control reports.

```
graphmana qc --qc-type [sample|variant|batch|all] --output FILE [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--qc-type` | Report type: `sample` (het rate, call rate), `variant` (HWE, missingness), `batch` (per-import QC), `all` |
| `--output FILE` | Output file path |
| `--output-format [html\|tsv\|json]` | Report format |

### graphmana merge

Merge data from another GraphMana database.

```
graphmana merge --databases URI [URI ...] [OPTIONS]
```

### graphmana migrate

Apply schema migrations to upgrade database format.

```
graphmana migrate [--dry-run]
```

### Neo4j Lifecycle Commands

```
graphmana setup-neo4j --install-dir PATH [--neo4j-version TEXT] [--memory-auto]
graphmana neo4j-start --neo4j-home PATH [--data-dir PATH] [--wait] [--timeout INT]
graphmana neo4j-stop --neo4j-home PATH
graphmana check-filesystem --neo4j-data-dir PATH
```

---

## Python API Reference (graphmana-py)

### GraphManaClient

```python
from graphmana_py import GraphManaClient
```

#### Constructor

```python
client = GraphManaClient(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="graphmana",  # or $GRAPHMANA_NEO4J_PASSWORD
)
```

Supports context manager:

```python
with GraphManaClient() as client:
    df = client.samples()
```

#### Database Metadata

| Method | Returns | Path | Description |
|--------|---------|------|-------------|
| `status()` | `dict` | FAST | Node counts and schema metadata |
| `samples(include_excluded=False)` | `DataFrame` | FAST | Sample list with population, QC metrics |
| `populations()` | `DataFrame` | FAST | Population metadata with sample counts |
| `chromosomes()` | `DataFrame` | FAST | Chromosome metadata with variant counts |
| `annotation_versions()` | `DataFrame` | FAST | Loaded annotation versions |
| `cohorts()` | `DataFrame` | FAST | Cohort definitions |

#### Variant Queries

| Method | Returns | Path | Description |
|--------|---------|------|-------------|
| `variants(chr, start=None, end=None)` | `DataFrame` | FAST | Variant info by region |
| `allele_frequencies(chr, start=None, end=None)` | `DataFrame` | FAST | Per-population AF/AC/AN |
| `genotype_matrix(chr, start=None, end=None)` | `DataFrame` | FULL | Samples x variants genotype matrix |
| `gene_variants(gene_symbol)` | `DataFrame` | FAST | Variants associated with a gene |
| `annotated_variants(annotation_version)` | `DataFrame` | FAST | Variants with specific annotation |
| `filtered_variants(chr, start, end, variant_type, maf_min, maf_max, populations, consequence, impact, gene)` | `DataFrame` | FAST | Multi-criteria filtering |
| `cohort_samples(cohort_name)` | `DataFrame` | FAST | Samples matching a cohort |

#### Export Convenience Methods

| Method | Description |
|--------|-------------|
| `to_vcf(output_path, **filters)` | Export to VCF via CLI subprocess |
| `to_plink(output_path, **filters)` | Export to PLINK 1.9 via CLI subprocess |
| `to_treemix(output_path)` | Export to TreeMix via CLI subprocess |

#### Arbitrary Cypher

```python
df = client.query("MATCH (v:Variant) WHERE v.chr = $chr RETURN v.pos LIMIT 10", {"chr": "22"})
```

#### Genotype Matrix Format

The `genotype_matrix()` method returns a DataFrame where:
- **Rows** = sample IDs (index)
- **Columns** = variant IDs
- **Values** = integer genotype codes: 0=HomRef, 1=Het, 2=HomAlt, 3=Missing

This is a FULL PATH operation. Memory usage is `n_samples * n_variants * 1 byte`.

```python
gt = client.genotype_matrix("22", start=16000000, end=17000000)
gt.shape  # (n_samples, n_variants)
gt.mean(axis=0)  # Per-variant mean genotype (proxy for AF)
```

---

## Export Format Reference

### FAST PATH Formats

These read pre-computed population arrays. Execution time is independent of sample count.

| Format | Command | Output Files | Target Tool(s) |
|--------|---------|-------------|-----------------|
| TreeMix | `--format treemix` | `.treemix.gz` | TreeMix |
| SFS (dadi) | `--format sfs-dadi` | `.sfs` | dadi, moments |
| SFS (fsc2) | `--format sfs-fsc` | `_DAFpop0.obs`, etc. | fastsimcoal2 |
| BED | `--format bed` | `.bed` | bedtools, IGV |
| TSV | `--format tsv` | `.tsv` | Custom analysis |

### FULL PATH Formats

These unpack genotype arrays. Time scales linearly with sample count.

| Format | Command | Output Files | Target Tool(s) |
|--------|---------|-------------|-----------------|
| VCF | `--format vcf` | `.vcf` / `.vcf.gz` / `.bcf` | bcftools, GATK, any VCF tool |
| PLINK 1.9 | `--format plink` | `.bed`, `.bim`, `.fam` | PLINK 1.9, GCTA |
| PLINK 2.0 | `--format plink2` | `.pgen`, `.pvar`, `.psam` | PLINK 2.0 |
| EIGENSTRAT | `--format eigenstrat` | `.geno`, `.snp`, `.ind` | smartPCA, AdmixTools |
| Beagle | `--format beagle` | `.beagle` | Beagle |
| STRUCTURE | `--format structure` | `.structure` | STRUCTURE |
| Genepop | `--format genepop` | `.genepop` | Genepop |
| Haplotype | `--format hap` | `.hap`, `.map` | selscan |
| BGEN | `--format bgen` | `.bgen`, `.sample` | BGEN tools, BOLT-LMM |
| GDS | `--format gds` | `.gds` | SeqArray (R) |
| Zarr | `--format zarr` | `.zarr/` | sgkit, xarray |
| JSON | `--format json` | `.jsonl` | Custom pipelines |

### Format Notes

**VCF**: Supports multi-allelic reconstruction (`--reconstruct-multiallelic`), phased output (`--phased`), and output type selection (`--output-type z` for bgzipped). Preserves original VCF headers from import.

**PLINK 1.9**: Biallelic SNPs only. Writes binary `.bed` (not to be confused with BED interval format), `.bim` variant info, and `.fam` sample info.

**PLINK 2.0**: Requires `pgenlib` Python package. Supports multi-allelic and phased data.

**SFS**: Supports hypergeometric projection (`--sfs-projection`) for unequal sample sizes. Polarized SFS requires ancestral allele annotation.

**TreeMix**: Gzipped allele count matrix. One of the fastest exports — reads only population-level arrays.

**EIGENSTRAT**: Three-file format for smartPCA and f-statistics (AdmixTools). Biallelic SNPs.

**BGEN**: BGEN 1.2 Layout 2 with zlib compression. Probabilistic genotypes encoded as uint16.

**Zarr**: sgkit-compatible chunked array format. Optional dependency (`pip install zarr`).

**GDS**: SeqArray HDF5 format for R/Bioconductor. Optional dependency (`pip install h5py`).

---

## Two-Step Cluster Workflow

For HPC clusters, split import into CPU-bound CSV generation (any compute node, no Neo4j) and database loading (Neo4j host):

```bash
# Step 1: Any compute node (no Neo4j needed)
graphmana prepare-csv \
    --input data.vcf.gz \
    --population-map pops.tsv \
    --output-dir /scratch/$USER/csv_out \
    --threads 16

# Step 2: Neo4j host (interactive or batch)
graphmana load-csv \
    --csv-dir /scratch/$USER/csv_out \
    --neo4j-home $HOME/neo4j \
    --neo4j-data-dir /scratch/$USER/graphmana_db
```

See `docs/cluster.md` for SLURM/PBS job script examples.

---

## Packed Array Encoding

GraphMana stores genotypes as bit-packed byte arrays on Variant nodes. This is the binary contract between the Python import pipeline and Java procedures (GraphMana and GraphPop).

### gt_packed (2 bits/sample)

4 samples per byte, LSB-first. Sample `i` occupies bits `(i%4)*2` and `(i%4)*2+1` of byte `gt_packed[i/4]`.

| Code | Meaning | cyvcf2 source |
|------|---------|---------------|
| 00 | HomRef | gt_types=0 |
| 01 | Het | gt_types=1 |
| 10 | HomAlt | gt_types=3 (remapped) |
| 11 | Missing | gt_types=2 (remapped) |

Array size: `ceil(N/4)` bytes for N samples.

### phase_packed (1 bit/sample)

8 samples per byte, LSB-first. Bit=1 means ALT allele is on the second haplotype (VCF `0|1`). Bit=0 at a het site means ALT on first haplotype (VCF `1|0`).

Array size: `ceil(N/8)` bytes.

### ploidy_packed (1 bit/sample)

Same layout as phase_packed. Bit=1 = haploid, Bit=0 = diploid. `null` means all diploid (e.g., autosomes).

See `docs/graphpop-compat.md` for encoding verification tests.
