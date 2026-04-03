"""Database state comparison — diff current state against a snapshot summary."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from graphmana.db.connection import GraphManaConnection
from graphmana.db.queries import ACTIVE_SAMPLE_FILTER

logger = logging.getLogger(__name__)

SUMMARY_SUFFIX = ".summary.json"


def capture_db_summary(conn: GraphManaConnection) -> dict:
    """Capture a lightweight summary of the current database state.

    Returns a dict suitable for JSON serialization and later comparison.
    """
    summary: dict = {}

    # Counts
    for label in ["Variant", "Sample", "Population", "Chromosome", "Gene"]:
        r = conn.execute_read(f"MATCH (n:{label}) RETURN count(n) AS c").single()
        summary[f"n_{label.lower()}s"] = r["c"] if r else 0

    # Active sample count
    r = conn.execute_read(
        f"MATCH (s:Sample) WHERE {ACTIVE_SAMPLE_FILTER} RETURN count(s) AS c"
    ).single()
    summary["n_active_samples"] = r["c"] if r else 0

    # Population list with sample counts
    pop_result = conn.execute_read(
        f"MATCH (s:Sample)-[:IN_POPULATION]->(p:Population) "
        f"WHERE {ACTIVE_SAMPLE_FILTER} "
        "RETURN p.populationId AS pop, count(s) AS n ORDER BY pop"
    )
    summary["populations"] = {r["pop"]: r["n"] for r in pop_result}

    # Variant type breakdown
    vtype_result = conn.execute_read(
        "MATCH (v:Variant) WHERE v.variant_type IS NOT NULL "
        "RETURN v.variant_type AS vt, count(v) AS c"
    )
    summary["variant_types"] = {r["vt"]: r["c"] for r in vtype_result}

    # Schema metadata
    meta_result = conn.execute_read(
        "MATCH (m:SchemaMetadata) RETURN m LIMIT 1"
    ).single()
    if meta_result:
        meta = dict(meta_result["m"])
        summary["reference_genome"] = meta.get("reference_genome", "unknown")
        summary["schema_version"] = meta.get("schema_version", "unknown")
    else:
        summary["reference_genome"] = "unknown"
        summary["schema_version"] = "unknown"

    # Annotation versions
    ann_result = conn.execute_read(
        "MATCH (a:AnnotationVersion) "
        "RETURN a.version_id AS vid, a.source AS src, a.version AS ver"
    )
    summary["annotation_versions"] = [
        {"version_id": r["vid"], "source": r["src"], "version": r["ver"]}
        for r in ann_result
    ]

    # Ingestion count
    prov_result = conn.execute_read(
        "MATCH (l:IngestionLog) RETURN count(l) AS n, max(l.import_date) AS last"
    ).single()
    summary["n_ingestions"] = prov_result["n"] if prov_result else 0
    summary["last_import"] = prov_result["last"] if prov_result else None

    return summary


def save_summary(summary: dict, path: Path) -> None:
    """Write a database summary to a JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(summary, f, indent=2, default=str)


def load_summary(path: Path) -> dict:
    """Load a previously saved database summary."""
    with open(path) as f:
        return json.load(f)


def diff_summaries(state_a: dict, state_b: dict, label_a: str = "A", label_b: str = "B") -> list[str]:
    """Compare two database summaries and return a list of human-readable diff lines."""
    lines = []

    # Count changes
    count_keys = [
        ("n_variants", "Variants"),
        ("n_samples", "Samples (total)"),
        ("n_active_samples", "Samples (active)"),
        ("n_populations", "Populations"),
        ("n_chromosomes", "Chromosomes"),
        ("n_genes", "Genes"),
        ("n_ingestions", "Ingestion logs"),
    ]
    lines.append("--- Count Changes ---")
    any_change = False
    for key, label in count_keys:
        va = state_a.get(key, 0)
        vb = state_b.get(key, 0)
        if va != vb:
            delta = vb - va
            sign = "+" if delta > 0 else ""
            lines.append(f"  {label + ':':25s} {va:>10,} -> {vb:>10,}  ({sign}{delta:,})")
            any_change = True
    if not any_change:
        lines.append("  (no changes)")

    # Population changes
    pops_a = state_a.get("populations", {})
    pops_b = state_b.get("populations", {})
    added_pops = set(pops_b) - set(pops_a)
    removed_pops = set(pops_a) - set(pops_b)
    changed_pops = {
        p for p in set(pops_a) & set(pops_b) if pops_a[p] != pops_b[p]
    }
    if added_pops or removed_pops or changed_pops:
        lines.append("")
        lines.append("--- Population Changes ---")
        for p in sorted(added_pops):
            lines.append(f"  + {p} ({pops_b[p]} samples)")
        for p in sorted(removed_pops):
            lines.append(f"  - {p} ({pops_a[p]} samples)")
        for p in sorted(changed_pops):
            lines.append(f"  ~ {p}: {pops_a[p]} -> {pops_b[p]} samples")

    # Variant type changes
    vt_a = state_a.get("variant_types", {})
    vt_b = state_b.get("variant_types", {})
    if vt_a != vt_b:
        lines.append("")
        lines.append("--- Variant Type Changes ---")
        all_types = sorted(set(vt_a) | set(vt_b))
        for vt in all_types:
            ca = vt_a.get(vt, 0)
            cb = vt_b.get(vt, 0)
            if ca != cb:
                delta = cb - ca
                sign = "+" if delta > 0 else ""
                lines.append(f"  {(vt or 'unknown') + ':':15s} {ca:>10,} -> {cb:>10,}  ({sign}{delta:,})")

    # Annotation version changes
    ann_a = {v.get("version_id", ""): v for v in state_a.get("annotation_versions", [])}
    ann_b = {v.get("version_id", ""): v for v in state_b.get("annotation_versions", [])}
    added_ann = set(ann_b) - set(ann_a)
    removed_ann = set(ann_a) - set(ann_b)
    if added_ann or removed_ann:
        lines.append("")
        lines.append("--- Annotation Changes ---")
        for vid in sorted(added_ann):
            a = ann_b[vid]
            lines.append(f"  + {a.get('source', '?')} v{a.get('version', '?')}")
        for vid in sorted(removed_ann):
            a = ann_a[vid]
            lines.append(f"  - {a.get('source', '?')} v{a.get('version', '?')}")

    # Reference genome change
    ref_a = state_a.get("reference_genome", "unknown")
    ref_b = state_b.get("reference_genome", "unknown")
    if ref_a != ref_b:
        lines.append("")
        lines.append(f"--- Reference Genome: {ref_a} -> {ref_b} ---")

    return lines
