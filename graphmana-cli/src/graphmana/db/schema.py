"""Schema creation — constraints, indexes, and metadata for the GraphMana graph.

Schema versions:
    1.0 — initial release. Variant nodes carry gt_packed, phase_packed,
          ploidy_packed and pre-computed population arrays.
    1.1 — adds ``called_packed`` (1 bit per sample: 1=interrogated, 0=not looked
          at) and ``gt_encoding`` ("dense" or "sparse_v1") on Variant nodes.
          The called mask preserves the HomRef-vs-Missing distinction across
          incremental batches; sparse encoding is an optional storage
          optimization chosen per variant. Databases created before v1.1 are
          forward-compatible: missing ``called_packed`` is treated as "all
          samples called" and missing ``gt_encoding`` is treated as "dense".
"""

from __future__ import annotations

from datetime import datetime, timezone

CURRENT_SCHEMA_VERSION = "1.1"

CONSTRAINTS = [
    "CREATE CONSTRAINT variant_id IF NOT EXISTS FOR (v:Variant) REQUIRE v.variantId IS UNIQUE",
    "CREATE CONSTRAINT sample_id IF NOT EXISTS FOR (s:Sample) REQUIRE s.sampleId IS UNIQUE",
    (
        "CREATE CONSTRAINT population_id IF NOT EXISTS"
        " FOR (p:Population) REQUIRE p.populationId IS UNIQUE"
    ),
    "CREATE CONSTRAINT gene_id IF NOT EXISTS FOR (g:Gene) REQUIRE g.geneId IS UNIQUE",
    (
        "CREATE CONSTRAINT chromosome_id IF NOT EXISTS"
        " FOR (c:Chromosome) REQUIRE c.chromosomeId IS UNIQUE"
    ),
    (
        "CREATE CONSTRAINT vcf_header_id IF NOT EXISTS"
        " FOR (h:VCFHeader) REQUIRE h.dataset_id IS UNIQUE"
    ),
    (
        "CREATE CONSTRAINT cohort_name IF NOT EXISTS"
        " FOR (c:CohortDefinition) REQUIRE c.name IS UNIQUE"
    ),
    (
        "CREATE CONSTRAINT annotation_version_id IF NOT EXISTS"
        " FOR (a:AnnotationVersion) REQUIRE a.version_id IS UNIQUE"
    ),
    (
        "CREATE CONSTRAINT regulatory_element_id IF NOT EXISTS"
        " FOR (r:RegulatoryElement) REQUIRE r.id IS UNIQUE"
    ),
    "CREATE CONSTRAINT goterm_id IF NOT EXISTS FOR (g:GOTerm) REQUIRE g.id IS UNIQUE",
    "CREATE CONSTRAINT pathway_id IF NOT EXISTS FOR (p:Pathway) REQUIRE p.id IS UNIQUE",
    (
        "CREATE CONSTRAINT ingestion_log_id IF NOT EXISTS"
        " FOR (l:IngestionLog) REQUIRE l.log_id IS UNIQUE"
    ),
]

INDEXES = [
    "CREATE INDEX variant_pos IF NOT EXISTS FOR (v:Variant) ON (v.chr, v.pos)",
    "CREATE INDEX variant_type IF NOT EXISTS FOR (v:Variant) ON (v.variant_type)",
    "CREATE INDEX gene_symbol IF NOT EXISTS FOR (g:Gene) ON (g.symbol)",
    "CREATE INDEX sample_pop IF NOT EXISTS FOR (s:Sample) ON (s.population)",
    "CREATE INDEX sample_excluded IF NOT EXISTS FOR (s:Sample) ON (s.excluded)",
    "CREATE INDEX cohort_created IF NOT EXISTS FOR (c:CohortDefinition) ON (c.created_date)",
    (
        "CREATE INDEX annotation_version_source IF NOT EXISTS"
        " FOR (a:AnnotationVersion) ON (a.source)"
    ),
    (
        "CREATE INDEX has_consequence_version IF NOT EXISTS"
        " FOR ()-[r:HAS_CONSEQUENCE]-() ON (r.annotation_version)"
    ),
    "CREATE INDEX variant_cadd IF NOT EXISTS FOR (v:Variant) ON (v.cadd_phred)",
    (
        "CREATE INDEX has_consequence_impact IF NOT EXISTS"
        " FOR ()-[r:HAS_CONSEQUENCE]-() ON (r.impact)"
    ),
    (
        "CREATE INDEX has_consequence_type IF NOT EXISTS"
        " FOR ()-[r:HAS_CONSEQUENCE]-() ON (r.consequence)"
    ),
    (
        "CREATE INDEX regulatory_element_region IF NOT EXISTS"
        " FOR (r:RegulatoryElement) ON (r.chr, r.start, r.end)"
    ),
    "CREATE INDEX goterm_namespace IF NOT EXISTS FOR (g:GOTerm) ON (g.namespace)",
    "CREATE INDEX pathway_source IF NOT EXISTS FOR (p:Pathway) ON (p.source)",
    "CREATE INDEX ingestion_log_date IF NOT EXISTS FOR (l:IngestionLog) ON (l.import_date)",
]


_SCHEMA_METADATA_QUERY = """\
MERGE (m:SchemaMetadata {id: 'graphmana'})
SET m.schema_version = $schema_version,
    m.graphmana_version = $graphmana_version,
    m.reference_genome = $reference_genome,
    m.created_date = coalesce(m.created_date, $now),
    m.last_modified = $now,
    m.n_samples = $n_samples,
    m.n_variants = $n_variants,
    m.n_populations = $n_populations,
    m.chr_naming_style = $chr_naming_style
RETURN m
"""

_COUNT_QUERY = "MATCH (n:{label}) RETURN count(n) AS c"


def create_schema(conn) -> dict:
    """Create all constraints and indexes. Returns counts of created items."""
    created = {"constraints": 0, "indexes": 0}
    for stmt in CONSTRAINTS:
        conn.execute_write(stmt)
        created["constraints"] += 1
    for stmt in INDEXES:
        conn.execute_write(stmt)
        created["indexes"] += 1
    return created


def create_schema_metadata(
    conn,
    *,
    schema_version: str = CURRENT_SCHEMA_VERSION,
    graphmana_version: str = "1.1.0",
    reference_genome: str = "unknown",
    chr_naming_style: str = "auto",
    n_samples: int = 0,
    n_variants: int = 0,
    n_populations: int = 0,
) -> None:
    """Create or update the SchemaMetadata singleton node.

    Args:
        conn: database connection with execute_write method.
        schema_version: schema version string.
        graphmana_version: GraphMana software version.
        reference_genome: reference genome identifier (e.g. 'GRCh38').
        chr_naming_style: chromosome naming convention used.
        n_samples: total sample count.
        n_variants: total variant count.
        n_populations: total population count.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn.execute_write(
        _SCHEMA_METADATA_QUERY,
        {
            "schema_version": schema_version,
            "graphmana_version": graphmana_version,
            "reference_genome": reference_genome,
            "now": now,
            "chr_naming_style": chr_naming_style,
            "n_samples": n_samples,
            "n_variants": n_variants,
            "n_populations": n_populations,
        },
    )


def _count_nodes(conn, label: str) -> int:
    """Count nodes with the given label."""
    result = conn.execute_read(_COUNT_QUERY.format(label=label))
    record = result.single()
    return record["c"] if record else 0


def ensure_schema(
    conn,
    *,
    reference_genome: str = "unknown",
    chr_naming_style: str = "auto",
) -> dict:
    """Create schema (constraints + indexes) and metadata in one call.

    Queries actual node counts from the database for SchemaMetadata.

    Args:
        conn: database connection with execute_write method.
        reference_genome: reference genome identifier.
        chr_naming_style: chromosome naming convention.

    Returns:
        Dict with counts of created constraints and indexes.
    """
    result = create_schema(conn)
    create_schema_metadata(
        conn,
        reference_genome=reference_genome,
        chr_naming_style=chr_naming_style,
        n_samples=_count_nodes(conn, "Sample"),
        n_variants=_count_nodes(conn, "Variant"),
        n_populations=_count_nodes(conn, "Population"),
    )
    return result
