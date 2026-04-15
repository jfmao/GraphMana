"""Reusable Cypher query strings for GraphMana."""

# Shared WHERE clause fragment for excluding soft-deleted samples.
# Every query that resolves samples for operational use MUST include this.
ACTIVE_SAMPLE_FILTER = "s.excluded IS NULL OR s.excluded = false"

# Node counts
COUNT_NODES = "MATCH (n:{label}) RETURN count(n) AS c"

# Schema metadata
GET_SCHEMA_METADATA = "MATCH (m:SchemaMetadata) RETURN m LIMIT 1"

# Relationship counts
COUNT_RELATIONSHIPS = "MATCH ()-[r]->() RETURN count(r) AS c"

# ---------------------------------------------------------------------------
# Export queries
# ---------------------------------------------------------------------------

FETCH_SAMPLES = f"""
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
WHERE {ACTIVE_SAMPLE_FILTER}
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex
ORDER BY s.packed_index
"""

FETCH_SAMPLES_BY_POPULATION = f"""
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
WHERE ({ACTIVE_SAMPLE_FILTER})
  AND p.populationId IN $populations
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex
ORDER BY s.packed_index
"""

FETCH_SAMPLES_BY_IDS = f"""
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
WHERE ({ACTIVE_SAMPLE_FILTER})
  AND s.sampleId IN $sample_ids
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex
ORDER BY s.packed_index
"""

FETCH_CHROMOSOMES = """
MATCH (c:Chromosome)
RETURN c.chromosomeId AS chr, c.length AS length
ORDER BY c.chromosomeId
"""

FETCH_VARIANTS_BY_CHR = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
RETURN v
ORDER BY v.pos
"""

# ---------------------------------------------------------------------------
# Smart variant query building blocks
# ---------------------------------------------------------------------------
# Three column sets for different export needs:
#   FAST — population arrays + metadata (no packed genotype arrays)
#   GENOTYPES — packed arrays + metadata (no population arrays)
#   FULL — everything (legacy, avoid for large datasets)
#
# Two ordering modes:
#   Ordered — ORDER BY v.pos (for positional output: BED, TSV, VCF, PLINK)
#   Unordered — no sort (for aggregation: TreeMix, SFS)
#
# Batched pagination: add WHERE v.pos > $last_pos ... LIMIT $batch_size
#   to ordered queries for large chromosomes (avoids GC pauses from sorting
#   millions of rows in Neo4j heap).

_FAST_COLUMNS = """
       v.variantId AS variantId, v.chr AS chr, v.pos AS pos,
       v.ref AS ref, v.alt AS alt, v.variant_type AS variant_type,
       v.pop_ids AS pop_ids, v.ac AS ac, v.an AS an, v.af AS af,
       v.ac_total AS ac_total, v.an_total AS an_total,
       v.af_total AS af_total, v.call_rate AS call_rate,
       v.het_count AS het_count, v.hom_alt_count AS hom_alt_count,
       v.het_exp AS het_exp, v.ancestral_allele AS ancestral_allele,
       v.is_polarized AS is_polarized,
       v.multiallelic_site AS multiallelic_site,
       v.allele_index AS allele_index,
       v.qual AS qual, v.filter AS filter,
       v.consequence AS consequence, v.impact AS impact,
       v.gene_symbol AS gene_symbol,
       v.liftover_status AS liftover_status,
       v.population_specificity AS population_specificity
"""

_GENOTYPES_COLUMNS = """
       v.variantId AS variantId, v.chr AS chr, v.pos AS pos,
       v.ref AS ref, v.alt AS alt, v.variant_type AS variant_type,
       v.gt_packed AS gt_packed, v.phase_packed AS phase_packed,
       v.ploidy_packed AS ploidy_packed,
       v.called_packed AS called_packed,
       v.gt_encoding AS gt_encoding,
       v.ac_total AS ac_total, v.an_total AS an_total,
       v.af_total AS af_total, v.call_rate AS call_rate,
       v.qual AS qual, v.filter AS filter,
       v.multiallelic_site AS multiallelic_site,
       v.allele_index AS allele_index,
       v.info_raw AS info_raw, v.csq_raw AS csq_raw
"""

# --- FAST PATH queries (no packed arrays) ---

FETCH_VARIANTS_BY_CHR_FAST = (
    "MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})\n"
    "RETURN" + _FAST_COLUMNS + "\nORDER BY v.pos"
)

FETCH_VARIANTS_BY_CHR_FAST_UNORDERED = (
    "MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})\n"
    "RETURN" + _FAST_COLUMNS
)

FETCH_VARIANTS_BY_CHR_FAST_BATCHED = (
    "MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})\n"
    "WHERE v.pos > $last_pos\n"
    "RETURN" + _FAST_COLUMNS + "\nORDER BY v.pos\nLIMIT $batch_size"
)

FETCH_VARIANTS_REGION_FAST = (
    "MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})\n"
    "WHERE v.pos >= $start AND v.pos <= $end\n"
    "RETURN" + _FAST_COLUMNS + "\nORDER BY v.pos"
)

FETCH_VARIANTS_REGION_FAST_UNORDERED = (
    "MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})\n"
    "WHERE v.pos >= $start AND v.pos <= $end\n"
    "RETURN" + _FAST_COLUMNS
)

# --- GENOTYPES PATH queries (packed arrays, no pop arrays) ---

FETCH_VARIANTS_BY_CHR_GENOTYPES = (
    "MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})\n"
    "RETURN" + _GENOTYPES_COLUMNS + "\nORDER BY v.pos"
)

FETCH_VARIANTS_BY_CHR_GENOTYPES_BATCHED = (
    "MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})\n"
    "WHERE v.pos > $last_pos\n"
    "RETURN" + _GENOTYPES_COLUMNS + "\nORDER BY v.pos\nLIMIT $batch_size"
)

FETCH_VARIANTS_REGION_GENOTYPES = (
    "MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})\n"
    "WHERE v.pos >= $start AND v.pos <= $end\n"
    "RETURN" + _GENOTYPES_COLUMNS + "\nORDER BY v.pos"
)

FETCH_VCF_HEADER = """
MATCH (h:VCFHeader)
RETURN h ORDER BY h.import_date DESC LIMIT 1
"""

FETCH_VARIANTS_REGION = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
WHERE v.pos >= $start AND v.pos <= $end
RETURN v
ORDER BY v.pos
"""

# Annotation-filtered export queries — use EXISTS subquery with IS NULL pattern
# for optional filter combinations in a single query template.

FETCH_VARIANTS_BY_CHR_ANNOTATED = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
WHERE EXISTS {
  MATCH (v)-[r:HAS_CONSEQUENCE]->(g:Gene)
  WHERE ($consequences IS NULL OR r.consequence IN $consequences)
    AND ($impacts IS NULL OR r.impact IN $impacts)
    AND ($genes IS NULL OR g.symbol IN $genes OR g.geneId IN $genes)
    AND ($annotation_version IS NULL OR r.annotation_version = $annotation_version)
}
RETURN v
ORDER BY v.pos
"""

FETCH_VARIANTS_REGION_ANNOTATED = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
WHERE v.pos >= $start AND v.pos <= $end
  AND EXISTS {
    MATCH (v)-[r:HAS_CONSEQUENCE]->(g:Gene)
    WHERE ($consequences IS NULL OR r.consequence IN $consequences)
      AND ($impacts IS NULL OR r.impact IN $impacts)
      AND ($genes IS NULL OR g.symbol IN $genes OR g.geneId IN $genes)
      AND ($annotation_version IS NULL OR r.annotation_version = $annotation_version)
  }
RETURN v
ORDER BY v.pos
"""

FETCH_VARIANTS_BY_CHR_CADD = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
WHERE v.cadd_phred IS NOT NULL
RETURN v
ORDER BY v.pos
"""

FETCH_VARIANTS_REGION_CADD = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
WHERE v.pos >= $start AND v.pos <= $end
  AND v.cadd_phred IS NOT NULL
RETURN v
ORDER BY v.pos
"""

# ---------------------------------------------------------------------------
# Incremental import queries
# ---------------------------------------------------------------------------

FETCH_MAX_PACKED_INDEX = f"""
MATCH (s:Sample) WHERE {ACTIVE_SAMPLE_FILTER}
RETURN max(s.packed_index) AS max_idx
"""

FETCH_EXISTING_SAMPLE_IDS = f"""
MATCH (s:Sample) WHERE {ACTIVE_SAMPLE_FILTER}
RETURN collect(s.sampleId) AS ids
"""

FETCH_EXISTING_POP_IDS = """
MATCH (p:Population) RETURN collect(p.populationId) AS ids
"""

FETCH_VARIANT_BATCH = """
UNWIND $variant_ids AS vid
MATCH (v:Variant {variantId: vid})
RETURN v.variantId AS variantId,
       v.gt_packed AS gt_packed, v.phase_packed AS phase_packed,
       v.ploidy_packed AS ploidy_packed,
       v.called_packed AS called_packed,
       v.gt_encoding AS gt_encoding,
       v.pop_ids AS pop_ids, v.ac AS ac, v.an AS an, v.af AS af,
       v.het_count AS het_count, v.hom_alt_count AS hom_alt_count,
       v.het_exp AS het_exp,
       v.ac_total AS ac_total, v.an_total AS an_total,
       v.af_total AS af_total, v.call_rate AS call_rate
"""

UPDATE_VARIANT_BATCH = """
UNWIND $updates AS u
MATCH (v:Variant {variantId: u.variantId})
SET v.gt_packed = u.gt_packed,
    v.phase_packed = u.phase_packed,
    v.ploidy_packed = u.ploidy_packed,
    v.called_packed = u.called_packed,
    v.gt_encoding = u.gt_encoding,
    v.pop_ids = u.pop_ids,
    v.ac = u.ac, v.an = u.an, v.af = u.af,
    v.het_count = u.het_count, v.hom_alt_count = u.hom_alt_count,
    v.het_exp = u.het_exp,
    v.ac_total = u.ac_total, v.an_total = u.an_total,
    v.af_total = u.af_total, v.call_rate = u.call_rate
"""

FETCH_VARIANT_IDS_BY_CHR = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
RETURN v.variantId AS variantId
ORDER BY v.pos
"""

CREATE_SAMPLE_BATCH = """
UNWIND $samples AS s
CREATE (n:Sample {
  sampleId: s.sampleId, population: s.population,
  packed_index: s.packed_index, sex: s.sex,
  source_dataset: s.source_dataset, source_file: s.source_file,
  ingestion_date: s.ingestion_date
})
"""

CREATE_IN_POPULATION_BATCH = """
UNWIND $edges AS e
MATCH (s:Sample {sampleId: e.sampleId})
MATCH (p:Population {populationId: e.populationId})
CREATE (s)-[:IN_POPULATION]->(p)
"""

MERGE_POPULATION = """
MERGE (p:Population {populationId: $populationId})
SET p.name = $name, p.n_samples = $n_samples,
    p.a_n = $a_n, p.a_n2 = $a_n2
"""

UPDATE_POPULATION_COUNTS = """
UNWIND $pops AS p
MATCH (pop:Population {populationId: p.populationId})
SET pop.n_samples = p.n_samples, pop.a_n = p.a_n, pop.a_n2 = p.a_n2
"""

CREATE_VARIANT_BATCH = """
UNWIND $variants AS v
CREATE (n:Variant {
  variantId: v.variantId, chr: v.chr, pos: v.pos,
  ref: v.ref, alt: v.alt, variant_type: v.variant_type,
  gt_packed: v.gt_packed, phase_packed: v.phase_packed,
  ploidy_packed: v.ploidy_packed,
  called_packed: v.called_packed,
  gt_encoding: v.gt_encoding,
  pop_ids: v.pop_ids, ac: v.ac, an: v.an, af: v.af,
  het_count: v.het_count, hom_alt_count: v.hom_alt_count,
  het_exp: v.het_exp,
  ac_total: v.ac_total, an_total: v.an_total,
  af_total: v.af_total, call_rate: v.call_rate,
  multiallelic_site: v.multiallelic_site,
  allele_index: v.allele_index
})
"""

CREATE_ON_CHROMOSOME_BATCH = """
UNWIND $edges AS e
MATCH (v:Variant {variantId: e.variantId})
MATCH (c:Chromosome {chromosomeId: e.chr})
CREATE (v)-[:ON_CHROMOSOME]->(c)
"""

DELETE_NEXT_CHAIN_FOR_CHR = """
MATCH (v:Variant)-[r:NEXT]->(v2:Variant)
WHERE v.chr = $chr
DELETE r
"""

REBUILD_NEXT_CHAIN_FOR_CHR = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
WITH v ORDER BY v.pos
WITH collect(v) AS variants
UNWIND range(0, size(variants)-2) AS i
WITH variants[i] AS v1, variants[i+1] AS v2
CREATE (v1)-[:NEXT {distance_bp: v2.pos - v1.pos}]->(v2)
"""

FETCH_N_EXISTING_SAMPLES = f"""
MATCH (s:Sample)
WHERE {ACTIVE_SAMPLE_FILTER}
RETURN count(s) AS n
"""

# ---------------------------------------------------------------------------
# Cohort management queries
# ---------------------------------------------------------------------------

CREATE_COHORT = """
MERGE (c:CohortDefinition {name: $name})
SET c.cypher_query = $cypher_query,
    c.created_date = coalesce(c.created_date, $created_date),
    c.description = $description
RETURN c
"""

GET_COHORT = """
MATCH (c:CohortDefinition {name: $name})
RETURN c
"""

LIST_COHORTS = """
MATCH (c:CohortDefinition)
RETURN c ORDER BY c.name
"""

DELETE_COHORT = """
MATCH (c:CohortDefinition {name: $name})
DELETE c
RETURN count(*) AS deleted
"""

# ---------------------------------------------------------------------------
# Annotation versioning queries
# ---------------------------------------------------------------------------

CREATE_ANNOTATION_VERSION = """
MERGE (a:AnnotationVersion {version_id: $version_id})
SET a.source = $source,
    a.version = $version,
    a.loaded_date = $loaded_date,
    a.n_annotations = $n_annotations,
    a.description = $description
RETURN a
"""

GET_ANNOTATION_VERSION = """
MATCH (a:AnnotationVersion {version_id: $version_id})
RETURN a
"""

LIST_ANNOTATION_VERSIONS = """
MATCH (a:AnnotationVersion)
RETURN a ORDER BY a.loaded_date DESC
"""

DELETE_ANNOTATION_VERSION = """
MATCH (a:AnnotationVersion {version_id: $version_id})
DELETE a
RETURN count(*) AS deleted
"""

COUNT_EDGES_BY_VERSION = """
MATCH ()-[r:HAS_CONSEQUENCE {annotation_version: $version}]->()
RETURN count(r) AS c
"""

DELETE_EDGES_BY_VERSION_BATCH = """
MATCH ()-[r:HAS_CONSEQUENCE {annotation_version: $version}]->()
WITH r LIMIT $batch_size
DELETE r
RETURN count(*) AS deleted
"""

MERGE_GENE_BATCH = """
UNWIND $genes AS g
MERGE (n:Gene {geneId: g.geneId})
SET n.symbol = g.symbol, n.biotype = g.biotype
"""

CREATE_CONSEQUENCE_BATCH = """
UNWIND $edges AS e
MATCH (v:Variant {variantId: e.variantId})
MATCH (g:Gene {geneId: e.geneId})
CREATE (v)-[:HAS_CONSEQUENCE {
    consequence: e.consequence, impact: e.impact,
    feature: e.feature, feature_type: e.feature_type,
    sift_score: e.sift_score, sift_pred: e.sift_pred,
    polyphen_score: e.polyphen_score, polyphen_pred: e.polyphen_pred,
    cadd_phred: e.cadd_phred, revel: e.revel,
    annotation_source: e.annotation_source,
    annotation_version: e.annotation_version
}]->(g)
"""

MERGE_CONSEQUENCE_BATCH = """
UNWIND $edges AS e
MATCH (v:Variant {variantId: e.variantId})
MATCH (g:Gene {geneId: e.geneId})
MERGE (v)-[r:HAS_CONSEQUENCE {annotation_version: e.annotation_version, feature: e.feature}]->(g)
SET r.consequence = e.consequence, r.impact = e.impact,
    r.feature_type = e.feature_type,
    r.sift_score = e.sift_score, r.sift_pred = e.sift_pred,
    r.polyphen_score = e.polyphen_score, r.polyphen_pred = e.polyphen_pred,
    r.cadd_phred = e.cadd_phred, r.revel = e.revel,
    r.annotation_source = e.annotation_source
"""

DELETE_ORPHAN_GENES = """
MATCH (g:Gene)
WHERE NOT EXISTS { (g)<-[:HAS_CONSEQUENCE]-() }
DELETE g
RETURN count(*) AS deleted
"""

# ---------------------------------------------------------------------------
# Sample management queries
# ---------------------------------------------------------------------------

EXCLUDE_SAMPLES = """
UNWIND $sample_ids AS sid
MATCH (s:Sample {sampleId: sid})
SET s.excluded = true, s.exclusion_reason = $reason
RETURN count(s) AS updated
"""

RESTORE_SAMPLES = """
UNWIND $sample_ids AS sid
MATCH (s:Sample {sampleId: sid})
WHERE s.excluded = true
REMOVE s.excluded, s.exclusion_reason
RETURN count(s) AS updated
"""

GET_SAMPLE = f"""
MATCH (s:Sample {{sampleId: $sample_id}})-[:IN_POPULATION]->(p:Population)
WHERE {ACTIVE_SAMPLE_FILTER}
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex,
       s.excluded AS excluded,
       s.exclusion_reason AS exclusion_reason,
       s.source_file AS source_file,
       s.ingestion_date AS ingestion_date
"""

LIST_ALL_SAMPLES = f"""
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
WHERE ({ACTIVE_SAMPLE_FILTER})
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex,
       s.excluded AS excluded,
       s.exclusion_reason AS exclusion_reason
ORDER BY s.packed_index
"""

LIST_ALL_SAMPLES_WITH_EXCLUDED = """
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex,
       s.excluded AS excluded,
       s.exclusion_reason AS exclusion_reason
ORDER BY s.packed_index
"""

LIST_SAMPLES_BY_POPULATION = f"""
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population {{populationId: $population}})
WHERE ({ACTIVE_SAMPLE_FILTER})
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex,
       s.excluded AS excluded,
       s.exclusion_reason AS exclusion_reason
ORDER BY s.packed_index
"""

LIST_SAMPLES_BY_POPULATION_WITH_EXCLUDED = """
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population {populationId: $population})
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex,
       s.excluded AS excluded,
       s.exclusion_reason AS exclusion_reason
ORDER BY s.packed_index
"""

# ---------------------------------------------------------------------------
# Sample reassignment queries
# ---------------------------------------------------------------------------

REASSIGN_SAMPLE_POPULATION = f"""
MATCH (s:Sample {{sampleId: $sample_id}})-[r:IN_POPULATION]->(:Population)
WHERE {ACTIVE_SAMPLE_FILTER}
DELETE r
WITH s
MATCH (p:Population {{populationId: $new_population}})
CREATE (s)-[:IN_POPULATION]->(p)
"""

# ---------------------------------------------------------------------------
# Hard delete queries
# ---------------------------------------------------------------------------

DELETE_SAMPLE_NODE = """
MATCH (s:Sample {sampleId: $sample_id})
DETACH DELETE s
"""

DELETE_POPULATION_IF_EMPTY = """
MATCH (p:Population {populationId: $population_id})
WHERE p.n_samples <= 0
DETACH DELETE p
RETURN count(*) AS deleted
"""

UPDATE_VARIANT_POP_STATS_BATCH = """
UNWIND $updates AS u
MATCH (v:Variant {variantId: u.variantId})
SET v.pop_ids = u.pop_ids,
    v.ac = u.ac, v.an = u.an, v.af = u.af,
    v.het_count = u.het_count, v.hom_alt_count = u.hom_alt_count,
    v.het_exp = u.het_exp,
    v.ac_total = u.ac_total, v.an_total = u.an_total,
    v.af_total = u.af_total, v.call_rate = u.call_rate
"""

UPDATE_VARIANT_HARD_DELETE_BATCH = """
UNWIND $updates AS u
MATCH (v:Variant {variantId: u.variantId})
SET v.gt_packed = u.gt_packed,
    v.phase_packed = u.phase_packed,
    v.called_packed = u.called_packed,
    v.gt_encoding = u.gt_encoding,
    v.pop_ids = u.pop_ids,
    v.ac = u.ac, v.an = u.an, v.af = u.af,
    v.het_count = u.het_count, v.hom_alt_count = u.hom_alt_count,
    v.het_exp = u.het_exp,
    v.ac_total = u.ac_total, v.an_total = u.an_total,
    v.af_total = u.af_total, v.call_rate = u.call_rate
"""

DECREMENT_SCHEMA_SAMPLE_COUNT = """
MATCH (m:SchemaMetadata)
SET m.n_samples = m.n_samples - $n_removed,
    m.last_modified = $modified_date
"""

COUNT_SAMPLES_BY_STATUS = """
MATCH (s:Sample)
RETURN
  count(s) AS total,
  count(CASE WHEN s.excluded = true THEN 1 END) AS excluded,
  count(CASE WHEN s.excluded IS NULL OR s.excluded = false THEN 1 END) AS active
"""

# ---------------------------------------------------------------------------
# QC queries
# ---------------------------------------------------------------------------

QC_VARIANT_SUMMARY = """
MATCH (v:Variant)
RETURN count(v) AS n_variants,
       avg(v.call_rate) AS mean_call_rate,
       min(v.call_rate) AS min_call_rate,
       max(v.call_rate) AS max_call_rate,
       avg(v.af_total) AS mean_af,
       min(v.af_total) AS min_af,
       max(v.af_total) AS max_af,
       count(CASE WHEN v.call_rate < 0.95 THEN 1 END) AS n_low_call_rate,
       count(CASE WHEN v.af_total = 0.0 THEN 1 END) AS n_monomorphic
"""

QC_VARIANT_TYPE_COUNTS = """
MATCH (v:Variant)
RETURN v.variant_type AS variant_type, count(v) AS count
ORDER BY count DESC
"""

QC_VARIANT_CHR_COUNTS = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome)
RETURN c.chromosomeId AS chr, count(v) AS count
ORDER BY c.chromosomeId
"""

QC_POPULATION_SUMMARY = f"""
MATCH (p:Population)
OPTIONAL MATCH (s:Sample)-[:IN_POPULATION]->(p)
WHERE {ACTIVE_SAMPLE_FILTER}
RETURN p.populationId AS population,
       p.n_samples AS n_samples_total,
       count(s) AS n_samples_active
ORDER BY p.populationId
"""

UPDATE_SAMPLE_QC_BATCH = """
UNWIND $stats AS st
MATCH (s:Sample {sampleId: st.sampleId})
SET s.n_het = st.n_het,
    s.n_hom_alt = st.n_hom_alt,
    s.heterozygosity = st.heterozygosity,
    s.call_rate = st.call_rate
"""

# ---------------------------------------------------------------------------
# CADD annotation queries
# ---------------------------------------------------------------------------

UPDATE_VARIANT_CADD_BATCH = """
UNWIND $updates AS u
MATCH (v:Variant {variantId: u.variantId})
SET v.cadd_phred = u.cadd_phred, v.cadd_raw = u.cadd_raw
RETURN count(v) AS matched
"""

# ---------------------------------------------------------------------------
# Gene constraint queries
# ---------------------------------------------------------------------------

UPDATE_GENE_CONSTRAINT_BATCH = """
UNWIND $updates AS u
MATCH (g:Gene {symbol: u.symbol})
SET g.pli = u.pli, g.loeuf = u.loeuf,
    g.mis_z = u.mis_z, g.syn_z = u.syn_z
RETURN count(g) AS matched
"""

# ---------------------------------------------------------------------------
# BED region annotation queries
# ---------------------------------------------------------------------------

MERGE_REGULATORY_ELEMENT_BATCH = """
UNWIND $elements AS e
MERGE (r:RegulatoryElement {id: e.id})
SET r.type = e.type, r.chr = e.chr, r.start = e.start, r.end = e.end,
    r.source = e.source
"""

FIND_VARIANTS_IN_INTERVAL = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
WHERE v.pos >= $start AND v.pos <= $end
RETURN v.variantId AS variantId
"""

CREATE_IN_REGION_BATCH = """
UNWIND $edges AS e
MATCH (v:Variant {variantId: e.variantId})
MATCH (r:RegulatoryElement {id: e.regionId})
CREATE (v)-[:IN_REGION]->(r)
"""

# ---------------------------------------------------------------------------
# GO / Pathway annotation queries
# ---------------------------------------------------------------------------

MERGE_GOTERM_BATCH = """
UNWIND $terms AS t
MERGE (g:GOTerm {id: t.id})
SET g.name = t.name, g.namespace = t.namespace
"""

CREATE_HAS_GO_TERM_BATCH = """
UNWIND $edges AS e
MATCH (g:Gene {symbol: e.gene_symbol})
MATCH (t:GOTerm {id: e.go_id})
MERGE (g)-[:HAS_GO_TERM]->(t)
"""

CREATE_GO_HIERARCHY_BATCH = """
UNWIND $edges AS e
MATCH (child:GOTerm {id: e.child_id})
MATCH (parent:GOTerm {id: e.parent_id})
MERGE (child)-[:IS_A]->(parent)
"""

MERGE_PATHWAY_BATCH = """
UNWIND $pathways AS p
MERGE (pw:Pathway {id: p.id})
SET pw.name = p.name, pw.source = p.source
"""

CREATE_IN_PATHWAY_BATCH = """
UNWIND $edges AS e
MATCH (g:Gene {symbol: e.gene_symbol})
MATCH (pw:Pathway {id: e.pathway_id})
MERGE (g)-[:IN_PATHWAY]->(pw)
"""

# ---------------------------------------------------------------------------
# ClinVar annotation queries
# ---------------------------------------------------------------------------

UPDATE_VARIANT_CLINVAR_BATCH = """
UNWIND $updates AS u
MATCH (v:Variant {variantId: u.variantId})
SET v.clinvar_id = u.clinvar_id,
    v.clinvar_sig = u.clinvar_sig,
    v.clinvar_review = u.clinvar_review,
    v.clinvar_disease = u.clinvar_disease
RETURN count(v) AS matched
"""

# ---------------------------------------------------------------------------
# Liftover queries
# ---------------------------------------------------------------------------

FETCH_VARIANT_COORDS_BY_CHR = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
RETURN v.variantId AS variantId, v.chr AS chr, v.pos AS pos,
       v.ref AS ref, v.alt AS alt
ORDER BY v.pos
"""

LIFTOVER_UPDATE_VARIANT_BATCH = """
UNWIND $updates AS u
MATCH (v:Variant {variantId: u.old_variantId})
SET v.variantId = u.variantId,
    v.chr = u.chr,
    v.pos = u.pos,
    v.ref = u.ref,
    v.alt = u.alt,
    v.liftover_status = u.liftover_status,
    v.original_variantId = u.original_variantId
"""

LIFTOVER_FLAG_UNMAPPED_BATCH = """
UNWIND $updates AS u
MATCH (v:Variant {variantId: u.variantId})
SET v.liftover_status = u.liftover_status,
    v.original_variantId = u.original_variantId
"""

LIFTOVER_REPOINT_ON_CHROMOSOME_BATCH = """
UNWIND $updates AS u
MATCH (v:Variant {variantId: u.variantId})-[r:ON_CHROMOSOME]->(:Chromosome)
DELETE r
WITH v, u
MATCH (c:Chromosome {chromosomeId: u.new_chr})
CREATE (v)-[:ON_CHROMOSOME]->(c)
"""

LIFTOVER_ENSURE_CHROMOSOME = """
MERGE (c:Chromosome {chromosomeId: $chromosomeId})
"""

# ---------------------------------------------------------------------------
# Database merge queries (run on source DB)
# ---------------------------------------------------------------------------

FETCH_SOURCE_VARIANTS_FULL_BY_CHR = """
MATCH (v:Variant)-[:ON_CHROMOSOME]->(c:Chromosome {chromosomeId: $chr})
RETURN v.variantId AS variantId, v.chr AS chr, v.pos AS pos,
       v.ref AS ref, v.alt AS alt, v.variant_type AS variant_type,
       v.gt_packed AS gt_packed, v.phase_packed AS phase_packed,
       v.ploidy_packed AS ploidy_packed,
       v.called_packed AS called_packed,
       v.gt_encoding AS gt_encoding,
       v.pop_ids AS pop_ids, v.ac AS ac, v.an AS an,
       v.het_count AS het_count, v.hom_alt_count AS hom_alt_count,
       v.ac_total AS ac_total, v.an_total AS an_total,
       v.af_total AS af_total, v.call_rate AS call_rate
ORDER BY v.pos
"""

FETCH_SOURCE_SAMPLES = f"""
MATCH (s:Sample)-[:IN_POPULATION]->(p:Population)
WHERE {ACTIVE_SAMPLE_FILTER}
RETURN s.sampleId AS sampleId,
       p.populationId AS population,
       s.packed_index AS packed_index,
       s.sex AS sex,
       s.source_dataset AS source_dataset,
       s.source_file AS source_file
ORDER BY s.packed_index
"""

FETCH_SOURCE_POPULATIONS = """
MATCH (p:Population)
RETURN p.populationId AS populationId, p.name AS name,
       p.n_samples AS n_samples, p.a_n AS a_n, p.a_n2 AS a_n2
ORDER BY p.populationId
"""

UPDATE_SCHEMA_VERSION = """
MATCH (m:SchemaMetadata)
SET m.schema_version = $schema_version,
    m.graphmana_version = $graphmana_version,
    m.last_modified = $last_modified
"""

LIFTOVER_UPDATE_SCHEMA_REFERENCE = """
MATCH (m:SchemaMetadata)
SET m.reference_genome = $reference_genome,
    m.last_modified = $last_modified
"""

# ---------------------------------------------------------------------------
# Provenance queries
# ---------------------------------------------------------------------------

CREATE_INGESTION_LOG = """
CREATE (l:IngestionLog {
    log_id: $log_id,
    source_file: $source_file,
    dataset_id: $dataset_id,
    mode: $mode,
    import_date: $import_date,
    n_samples: $n_samples,
    n_variants: $n_variants,
    filters_applied: $filters_applied,
    fidelity: $fidelity,
    reference_genome: $reference_genome
})
RETURN l
"""

GET_INGESTION_LOG = """
MATCH (l:IngestionLog {log_id: $log_id})
RETURN l
"""

LIST_INGESTION_LOGS = """
MATCH (l:IngestionLog)
RETURN l ORDER BY l.import_date DESC
"""

LIST_VCF_HEADERS = """
MATCH (h:VCFHeader)
RETURN h ORDER BY h.import_date DESC
"""

GET_VCF_HEADER = """
MATCH (h:VCFHeader {dataset_id: $dataset_id})
RETURN h
"""

SEARCH_INGESTION_LOGS = """
MATCH (l:IngestionLog)
WHERE ($since IS NULL OR l.import_date >= $since)
  AND ($until IS NULL OR l.import_date <= $until)
  AND ($dataset_id IS NULL OR l.dataset_id = $dataset_id)
RETURN l ORDER BY l.import_date DESC
"""

PROVENANCE_SUMMARY = """
MATCH (l:IngestionLog)
RETURN count(l) AS n_ingestions,
       sum(l.n_samples) AS total_samples_imported,
       sum(l.n_variants) AS total_variants_imported,
       min(l.import_date) AS first_import,
       max(l.import_date) AS last_import,
       collect(DISTINCT l.source_file) AS source_files
"""
