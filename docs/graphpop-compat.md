# GraphPop Compatibility Report

_Audit date: 2026-03-15_

## Summary

GraphMana-imported databases are **fully compatible** with GraphPop procedures. All required properties, encodings, and relationships match. One known gap exists (soft-delete awareness) with a documented workaround.

---

## Layer 1: Property Name Audit

### Variant Node — Required by GraphPop

| Property | GraphPop Type | GraphMana Type | Match | Notes |
|----------|---------------|----------------|-------|-------|
| `variantId` | String | string | YES | |
| `chr` | String | string | YES | |
| `pos` | long | long | YES | |
| `gt_packed` | byte[] | byte[] | YES | Identical 2-bit LSB-first encoding |
| `phase_packed` | byte[] | byte[] | YES | Identical 1-bit LSB-first encoding |
| `ploidy_packed` | byte[] (nullable) | byte[] (nullable) | YES | null = all diploid in both |
| `pop_ids` | String[] | string[] | YES | ArrayUtil handles both native and semicolon-delimited |
| `ac` | int[] | int[] | YES | ArrayUtil handles long[] and String too |
| `an` | int[] | int[] | YES | |
| `af` | double[] | float[] | YES | ArrayUtil.toDoubleArray() accepts float[] and converts |
| `het_count` | int[] | int[] | YES | |
| `hom_alt_count` | int[] | int[] | YES | |
| `an_total` | int (nullable) | int | YES | Used only for call_rate filter |
| `variant_type` | String (nullable) | string | YES | |
| `ancestral_allele` | String (nullable) | string (nullable) | YES | Used for Fay & Wu's H, polarized SFS |

### Variant Node — GraphMana extras (ignored by GraphPop, harmless)

`ref`, `alt`, `het_exp[]`, `ac_total`, `af_total`, `call_rate`, `is_polarized`, `qual`, `filter`, `multiallelic_site`, `allele_index`, `sv_type`, `sv_len`, `sv_end`, `cadd_phred`, `cadd_raw`, `clinvar_*`, `liftover_status`, `original_variantId`

### Sample Node

| Property | GraphPop Type | GraphMana Type | Match |
|----------|---------------|----------------|-------|
| `sampleId` | String | string | YES |
| `packed_index` | int (via Number) | int | YES |

GraphMana extras (ignored): `population`, `sex`, `source_dataset`, `source_file`, `ingestion_date`, `excluded`, `exclusion_reason`, QC metrics.

### Population Node

| Property | GraphPop Type | GraphMana Type | Match | Notes |
|----------|---------------|----------------|-------|-------|
| `populationId` | String | string | YES | |
| `n_samples` | int (via Number) | int | YES | |
| `a_n` | double (via Number) | float | YES | Number.doubleValue() handles float |
| `a_n2` | double (via Number) | float | YES | Same |

### Relationships

| Relationship | GraphPop Uses | GraphMana Creates | Match |
|-------------|---------------|-------------------|-------|
| `(:Sample)-[:IN_POPULATION]->(:Population)` | YES (GenotypeLoader) | YES | YES |
| `(:Variant)-[:HAS_CONSEQUENCE]->(:Gene)` | YES (VariantQuery, `c.consequence`) | YES | YES |
| `(:Gene)-[:IN_PATHWAY]->(:Pathway)` | YES (VariantQuery, `p.name`) | YES | YES |
| `(:Variant)-[:ON_CHROMOSOME]->(:Chromosome)` | Created by GenomeScan | Created by GraphMana | YES |
| `(:Variant)-[:NEXT]->(:Variant)` | Not used by GraphPop | Created by GraphMana | N/A |

---

## Layer 2: Encoding Verification

### Packed Genotype Arrays

GraphMana (Python) and GraphPop (Java) use **identical** PackedGenotypeReader implementations:

- **gt_packed**: 2 bits/sample, 4 samples/byte, LSB-first. `byte[i/4] >> ((i%4)*2) & 0x03`
  - 00=HomRef, 01=Het, 10=HomAlt, 11=Missing
- **phase_packed**: 1 bit/sample, 8 samples/byte, LSB-first. `byte[i/8] >> (i%8) & 0x01`
- **ploidy_packed**: Same layout as phase_packed. null = all diploid.

The GraphMana Java PackedGenotypeReader (`graphmana-procedures`) is byte-identical to the GraphPop PackedGenotypeReader. The Python genotype_packer uses the same encoding with proper cyvcf2 remap: `[0, 1, 3, 2]` (cyvcf2: 0=HOM_REF, 1=HET, 2=MISSING, 3=HOM_ALT → packed: 0=HomRef, 1=Het, 2=HomAlt, 3=Missing).

### Array Type Coercion

GraphPop's `ArrayUtil` class handles multiple storage formats:
- `toIntArray()`: accepts `int[]`, `long[]`, semicolon-delimited `String`
- `toDoubleArray()`: accepts `double[]`, `float[]`, `long[]`, `int[]`, semicolon-delimited `String`
- `toStringArray()`: accepts `String[]`, semicolon-delimited `String`

GraphMana writes typed arrays via `neo4j-admin import` (CSV headers like `ac:int[]`, `af:float[]`). These become native Neo4j typed arrays. All are handled by ArrayUtil.

### Harmonic Numbers

- GraphMana: `a_n = sum(1/i for i in range(1, 2*n))` = sum(1/i, i=1..2n-1)
- GraphPop: reads `a_n` from Population node, used in Tajima's D and diversity calculations
- Formula matches: both use the standard first harmonic number H_{2n-1}

### CSV Byte Encoding

GraphMana writes packed arrays as semicolon-delimited signed Java bytes (-128 to 127) in CSV. `neo4j-admin import` with `--array-delimiter=";"` converts these to `byte[]` properties. GraphPop reads them as `byte[]`. Compatible.

---

## Layer 3: Known Gaps

### Gap 1: Soft-Delete Awareness (MINOR)

**Issue**: GraphPop's `GenotypeLoader` queries:
```cypher
MATCH (s:Sample)-[:IN_POPULATION]->(:Population {populationId: $pop})
RETURN s.sampleId AS sid ORDER BY sid
```
This does NOT filter `WHERE s.excluded IS NULL OR s.excluded = false`. Soft-deleted samples would be included in GraphPop computations.

**Impact**: Only affects databases where `graphmana sample remove` has been used. Fresh imports have no soft-deleted samples.

**Workaround**: Hard-delete soft-deleted samples (`graphmana sample remove --hard`) before running GraphPop procedures. Or: future GraphPop update to add `excluded` filter.

**Risk**: LOW — soft-delete is a management convenience. For analysis, users should hard-delete or just not soft-delete before running procedures.

### Gap 2: Population Array Ordering Assumption

**Issue**: GraphPop resolves population index via `PopulationContext.resolve()`, which scans `pop_ids[]` on the first variant found. This assumes all variants have the same `pop_ids[]` ordering.

**Status**: GraphMana guarantees this — `pop_ids[]` is set identically on all variants during import (from the population map). Incremental import extends arrays in the same order. **No action needed.**

### Gap 3: `het_exp[]` Not Used by GraphPop

GraphMana writes `het_exp[]` (expected heterozygosity per population). GraphPop does not read this property — it computes HWE statistics from `het_count`, `hom_alt_count`, and sample counts. **No conflict — extra properties are ignored.**

---

## Verdict

**GraphPop can consume GraphMana-imported databases without modification.** All required property names, types, encodings, and relationship patterns match exactly. The one known gap (soft-delete awareness) has a simple workaround and is a non-issue for fresh imports.

When GraphMana eventually replaces GraphPop's import pipeline:
1. GraphPop procedures will read the same property names they currently expect
2. Packed array encoding is identical (verified at bit level)
3. Population arrays are compatible via ArrayUtil type coercion
4. Extra GraphMana properties (annotations, liftover, SV fields, multiallelic tracking) are invisible to GraphPop

---

## Verification Test

See `graphmana-cli/tests/test_graphpop_compat.py` for encoding-level verification tests that confirm Python-packed arrays produce the same bit patterns that GraphPop's Java PackedGenotypeReader expects.
