"""Cypher query strings for the Jupyter API.

These mirror the queries in graphmana-cli but are self-contained so that
graphmana-py can be installed independently.
"""

# ---------------------------------------------------------------------------
# Database status
# ---------------------------------------------------------------------------

STATUS_NODE_COUNT = "MATCH (n:{label}) RETURN count(n) AS c"

GET_SCHEMA_METADATA = "MATCH (m:SchemaMetadata) RETURN m LIMIT 1"

# ---------------------------------------------------------------------------
# Samples
# ---------------------------------------------------------------------------

FETCH_SAMPLES = """
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
WHERE s.excluded IS NULL OR s.excluded = false
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex,
       s.source_file AS source_file,
       s.ingestion_date AS ingestion_date,
       s.n_het AS n_het,
       s.n_hom_alt AS n_hom_alt,
       s.heterozygosity AS heterozygosity,
       s.call_rate AS call_rate
ORDER BY s.packed_index
"""

FETCH_ALL_SAMPLES = """
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex,
       s.excluded AS excluded,
       s.exclusion_reason AS exclusion_reason,
       s.source_file AS source_file,
       s.ingestion_date AS ingestion_date
ORDER BY s.packed_index
"""

# ---------------------------------------------------------------------------
# Populations
# ---------------------------------------------------------------------------

FETCH_POPULATIONS = """
MATCH (p:Population)
OPTIONAL MATCH (s:Sample)-[:IN_POPULATION]->(p)
WHERE s.excluded IS NULL OR s.excluded = false
RETURN p.populationId AS populationId,
       p.name AS name,
       p.n_samples AS n_samples,
       count(s) AS n_active_samples,
       p.a_n AS a_n,
       p.a_n2 AS a_n2
ORDER BY p.populationId
"""

# ---------------------------------------------------------------------------
# Chromosomes
# ---------------------------------------------------------------------------

FETCH_CHROMOSOMES = """
MATCH (c:Chromosome)
OPTIONAL MATCH (v:Variant)-[:ON_CHROMOSOME]->(c)
RETURN c.chromosomeId AS chromosomeId,
       c.length AS length,
       count(v) AS n_variants,
       c.aliases AS aliases
ORDER BY c.chromosomeId
"""

# ---------------------------------------------------------------------------
# Variants
# ---------------------------------------------------------------------------

FETCH_VARIANTS_BY_CHR = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
RETURN v.variantId AS variantId, v.chr AS chr, v.pos AS pos,
       v.ref AS ref, v.alt AS alt, v.variant_type AS variant_type,
       v.af_total AS af_total, v.ac_total AS ac_total,
       v.an_total AS an_total, v.call_rate AS call_rate,
       v.consequence AS consequence, v.impact AS impact,
       v.gene_symbol AS gene_symbol,
       v.sv_type AS sv_type, v.sv_len AS sv_len, v.sv_end AS sv_end
ORDER BY v.pos
"""

FETCH_VARIANTS_REGION = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
WHERE v.pos >= $start AND v.pos <= $end
RETURN v.variantId AS variantId, v.chr AS chr, v.pos AS pos,
       v.ref AS ref, v.alt AS alt, v.variant_type AS variant_type,
       v.af_total AS af_total, v.ac_total AS ac_total,
       v.an_total AS an_total, v.call_rate AS call_rate,
       v.consequence AS consequence, v.impact AS impact,
       v.gene_symbol AS gene_symbol,
       v.sv_type AS sv_type, v.sv_len AS sv_len, v.sv_end AS sv_end
ORDER BY v.pos
"""

FETCH_VARIANT_GENOTYPES_BY_CHR = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
RETURN v.variantId AS variantId, v.pos AS pos,
       v.gt_packed AS gt_packed
ORDER BY v.pos
"""

FETCH_VARIANT_GENOTYPES_REGION = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
WHERE v.pos >= $start AND v.pos <= $end
RETURN v.variantId AS variantId, v.pos AS pos,
       v.gt_packed AS gt_packed
ORDER BY v.pos
"""

# ---------------------------------------------------------------------------
# Annotations
# ---------------------------------------------------------------------------

FETCH_ANNOTATION_VERSIONS = """
MATCH (a:AnnotationVersion)
RETURN a.version_id AS version_id,
       a.source AS source,
       a.version AS version,
       a.loaded_date AS loaded_date,
       a.n_annotations AS n_annotations,
       a.description AS description
ORDER BY a.loaded_date DESC
"""

# ---------------------------------------------------------------------------
# Cohorts
# ---------------------------------------------------------------------------

FETCH_COHORTS = """
MATCH (c:CohortDefinition)
RETURN c.name AS name,
       c.cypher_query AS cypher_query,
       c.created_date AS created_date,
       c.description AS description
ORDER BY c.name
"""

# ---------------------------------------------------------------------------
# Allele frequency arrays (FAST PATH)
# ---------------------------------------------------------------------------

FETCH_VARIANT_POP_ARRAYS = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
RETURN v.variantId AS variantId, v.pos AS pos,
       v.pop_ids AS pop_ids, v.ac AS ac, v.an AS an, v.af AS af
ORDER BY v.pos
"""

FETCH_VARIANT_POP_ARRAYS_REGION = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
WHERE v.pos >= $start AND v.pos <= $end
RETURN v.variantId AS variantId, v.pos AS pos,
       v.pop_ids AS pop_ids, v.ac AS ac, v.an AS an, v.af AS af
ORDER BY v.pos
"""

# ---------------------------------------------------------------------------
# Gene-based variant queries
# ---------------------------------------------------------------------------

GENE_VARIANTS = """
MATCH (v:Variant)-[hc:HAS_CONSEQUENCE]->(g:Gene)
WHERE g.symbol = $gene_symbol OR g.geneId = $gene_symbol
RETURN v.variantId AS variantId, v.chr AS chr, v.pos AS pos,
       v.ref AS ref, v.alt AS alt, v.variant_type AS variant_type,
       v.af_total AS af_total, v.ac_total AS ac_total,
       v.an_total AS an_total, v.call_rate AS call_rate,
       hc.type AS consequence, hc.impact AS impact,
       g.symbol AS gene_symbol
ORDER BY v.pos
"""

# ---------------------------------------------------------------------------
# Annotation-filtered variant queries
# ---------------------------------------------------------------------------

ANNOTATED_VARIANTS = """
MATCH (v:Variant)-[hc:HAS_CONSEQUENCE]->(g:Gene)
WHERE hc.annotation_version = $annotation_version
RETURN v.variantId AS variantId, v.chr AS chr, v.pos AS pos,
       v.ref AS ref, v.alt AS alt, v.variant_type AS variant_type,
       v.af_total AS af_total,
       hc.type AS consequence, hc.impact AS impact,
       g.symbol AS gene_symbol,
       hc.annotation_version AS annotation_version
ORDER BY v.chr, v.pos
"""

# ---------------------------------------------------------------------------
# Cohort sample queries
# ---------------------------------------------------------------------------

COHORT_SAMPLES = """
MATCH (cd:CohortDefinition {name: $cohort_name})
WITH cd.cypher_query AS q
CALL {
  WITH q
  CALL db.query(q) YIELD sampleId
  RETURN sampleId
}
MATCH (s:Sample {sampleId: sampleId})-[:IN_POPULATION]->(p:Population)
WHERE s.excluded IS NULL OR s.excluded = false
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex
ORDER BY s.packed_index
"""

# ---------------------------------------------------------------------------
# Filtered variant queries (parameterized WHERE clauses)
# ---------------------------------------------------------------------------

FILTERED_VARIANTS = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome)
WHERE ($chr IS NULL OR c.chromosomeId = $chr)
  AND ($start IS NULL OR v.pos >= $start)
  AND ($end IS NULL OR v.pos <= $end)
  AND ($variant_type IS NULL OR v.variant_type = $variant_type)
  AND ($maf_min IS NULL OR v.af_total >= $maf_min)
  AND ($maf_max IS NULL OR v.af_total <= $maf_max)
  AND ($populations IS NULL OR
       ANY(p IN v.pop_ids WHERE p IN $populations))
  AND ($consequence IS NULL OR v.consequence = $consequence)
  AND ($impact IS NULL OR v.impact = $impact)
  AND ($gene IS NULL OR v.gene_symbol = $gene)
RETURN v.variantId AS variantId, v.chr AS chr, v.pos AS pos,
       v.ref AS ref, v.alt AS alt, v.variant_type AS variant_type,
       v.af_total AS af_total, v.ac_total AS ac_total,
       v.an_total AS an_total, v.call_rate AS call_rate,
       v.consequence AS consequence, v.impact AS impact,
       v.gene_symbol AS gene_symbol,
       v.sv_type AS sv_type, v.sv_len AS sv_len, v.sv_end AS sv_end
ORDER BY v.chr, v.pos
"""

FETCH_SAMPLES_BY_POPULATION = """
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
WHERE p.populationId IN $populations
  AND (s.excluded IS NULL OR s.excluded = false)
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex
ORDER BY s.packed_index
"""
