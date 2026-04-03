# Defining and Using Cohorts

Cohorts in GraphMana are named subsets of samples defined by Cypher queries
stored in the graph database. Unlike file-based subsetting (extracting samples
into a new VCF), cohorts are lightweight graph queries -- defining one is
instant, and the same data can be sliced many ways without duplication.

This vignette covers defining cohorts, inspecting them, and using them as
export filters.

## Prerequisites

A GraphMana database with imported data and Neo4j running. See
[01-quickstart.md](01-quickstart.md) or [02-import-1kgp.md](02-import-1kgp.md).

## How cohorts work

A cohort is a `:CohortDefinition` node in the graph with three key properties:

- `name` -- unique identifier used in CLI commands and export filters
- `cypher_query` -- a Cypher query that returns `sampleId` values
- `description` -- human-readable explanation

When you export with `--filter-cohort`, GraphMana executes the stored Cypher
query, collects the matching sample IDs, and restricts the export to those
samples. Soft-deleted samples (`excluded = true`) are automatically filtered
out.

## Example 1: All European samples

Define a cohort containing all samples from the EUR superpopulation:

```bash
graphmana cohort define \
    --name european_samples \
    --query "MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
             WHERE p.populationId IN ['GBR','FIN','CEU','TSI','IBS']
             RETURN s.sampleId AS sampleId" \
    --description "All European (EUR) samples from 1KGP"
```

**Expected output:**

```
Cohort 'european_samples' defined (2026-03-27T14:30:00)
```

## Example 2: High-heterozygosity samples

Define a cohort of samples with heterozygosity above a threshold. This
requires that sample-level QC has been run (see `graphmana qc`), which
populates the `heterozygosity` property on Sample nodes.

```bash
graphmana cohort define \
    --name high_het \
    --query "MATCH (s:Sample)
             WHERE s.heterozygosity > 0.3
             RETURN s.sampleId AS sampleId" \
    --description "Samples with heterozygosity > 0.3 (potential contamination)"
```

**Expected output:**

```
Cohort 'high_het' defined (2026-03-27T14:32:00)
```

## Example 3: Samples from a specific study

If samples carry a `source_dataset` property (set during import via
`--dataset-id`), you can define cohorts based on the originating study:

```bash
graphmana cohort define \
    --name phase3_only \
    --query "MATCH (s:Sample)
             WHERE s.source_dataset = '1kgp_phase3'
             RETURN s.sampleId AS sampleId" \
    --description "Samples from the 1KGP phase 3 release only"
```

## Example 4: Cross-referencing populations and sample properties

Cohort queries can traverse any relationship in the graph. This example
selects female samples from East Asian populations:

```bash
graphmana cohort define \
    --name eas_female \
    --query "MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
             WHERE p.populationId IN ['CHB','CHS','CDX','JPT','KHV']
             AND s.sex = 'female'
             RETURN s.sampleId AS sampleId" \
    --description "Female samples from East Asian populations"
```

## Listing cohorts

```bash
graphmana cohort list
```

**Expected output:**

```
  european_samples — All European (EUR) samples from 1KGP
  high_het — Samples with heterozygosity > 0.3 (potential contamination)
  phase3_only — Samples from the 1KGP phase 3 release only
  eas_female — Female samples from East Asian populations
```

## Inspecting a cohort

View the full details of a cohort including its Cypher query:

```bash
graphmana cohort show --name european_samples
```

**Expected output:**

```
Name:        european_samples
Description: All European (EUR) samples from 1KGP
Created:     2026-03-27T14:30:00
Query:       MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
             WHERE p.populationId IN ['GBR','FIN','CEU','TSI','IBS']
             RETURN s.sampleId AS sampleId
```

## Counting samples in a cohort

```bash
graphmana cohort count --name european_samples
```

**Expected output:**

```
Cohort 'european_samples': 503 samples
```

## Validating a query before saving

Test a Cypher query without creating a cohort:

```bash
graphmana cohort validate \
    --query "MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
             WHERE p.populationId = 'YRI'
             RETURN s.sampleId AS sampleId"
```

**Expected output:**

```
Valid. Would select 108 samples.
```

If the query has a syntax error or returns no `sampleId` column:

```bash
graphmana cohort validate \
    --query "MATCH (s:Sample) WHERE s.population = 'INVALID' RETURN s.name"
```

**Expected output:**

```
Invalid: Query must return a 'sampleId' column
```

## Using cohorts as export filters

The `--filter-cohort` option on `graphmana export` restricts the export to
samples matching the named cohort. This works with all 17 export formats.

### Export VCF for European samples only

```bash
graphmana export \
    --format vcf \
    --output exports/european_chr22.vcf \
    --chromosomes chr22 \
    --filter-cohort european_samples
```

**Expected output:**

```
Export complete (vcf): 1,073,621 variants
Samples: 503
```

### Export PLINK for a QC-filtered cohort

Combine cohort filtering with variant filters:

```bash
graphmana export \
    --format plink \
    --output exports/eur_common_snps \
    --filter-cohort european_samples \
    --filter-variant-type SNP \
    --filter-maf-min 0.01 \
    --filter-min-call-rate 0.95 \
    --chromosomes chr22
```

### Export TreeMix for specific populations within a cohort

When `--filter-cohort` is combined with `--populations`, only samples in both
the cohort AND the specified populations are included:

```bash
graphmana export \
    --format treemix \
    --output exports/eur_treemix.treemix.gz \
    --filter-cohort european_samples \
    --recalculate-af
```

When `--recalculate-af` is set (automatically enabled when filtering
populations), allele frequencies are recomputed from the subset of samples
rather than using the stored population arrays.

## Updating a cohort

To change a cohort's query, run `cohort define` again with the same name. The
existing definition is replaced:

```bash
graphmana cohort define \
    --name european_samples \
    --query "MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
             WHERE p.populationId IN ['GBR','FIN','CEU','TSI','IBS']
             AND (s.excluded IS NULL OR s.excluded = false)
             AND s.call_rate > 0.98
             RETURN s.sampleId AS sampleId" \
    --description "EUR samples passing QC (call_rate > 98%)"
```

## Deleting a cohort

```bash
graphmana cohort delete --name high_het
```

**Expected output:**

```
Cohort 'high_het' deleted.
```

## Writing conventions for cohort queries

1. **Always return `sampleId`**: The query must return a column named
   `sampleId`. Use `RETURN s.sampleId AS sampleId`.

2. **Soft-delete awareness**: GraphMana automatically filters out soft-deleted
   samples when resolving cohorts for export. However, if you want the cohort
   count to reflect only active samples, add the filter explicitly:
   `WHERE s.excluded IS NULL OR s.excluded = false`.

3. **Keep queries simple**: Cohort queries are executed each time the cohort
   is used. Avoid expensive graph traversals that scan many relationships.
   Filtering on indexed properties (`sampleId`, `populationId`) is fast.

4. **Use population IDs, not names**: The `populationId` property is the
   canonical identifier. Population names may contain spaces or special
   characters.

## See also

- [01-quickstart.md](01-quickstart.md) -- Quick start with demo data
- [03-export-formats.md](03-export-formats.md) -- All export formats and filtering options
- [05-annotation.md](05-annotation.md) -- Filtering by functional annotation
