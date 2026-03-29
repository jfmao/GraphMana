"""GraphMana Jupyter client — returns pandas DataFrames from a GraphMana database.

Usage::

    from graphmana_py import GraphManaClient

    with GraphManaClient("bolt://localhost:7687") as client:
        samples_df = client.samples()
        variants_df = client.variants(chr="22", start=16000000, end=17000000)
        gt_matrix = client.genotype_matrix(chr="22", start=16000000, end=17000000)
"""

from __future__ import annotations

import os

import numpy as np
import pandas as pd
from neo4j import GraphDatabase

from graphmana_py import _queries as Q
from graphmana_py._unpack import unpack_genotypes, unpack_phase, unpack_ploidy

_DEFAULT_PASSWORD = os.environ.get("GRAPHMANA_NEO4J_PASSWORD", "graphmana")


class GraphManaClient:
    """Pandas DataFrame interface to a GraphMana Neo4j database.

    Args:
        uri: Neo4j Bolt URI (e.g. ``bolt://localhost:7687``).
        user: Neo4j username.
        password: Neo4j password. Defaults to ``GRAPHMANA_NEO4J_PASSWORD``
            environment variable or ``"graphmana"``.
    """

    def __init__(
        self,
        uri: str = "bolt://localhost:7687",
        user: str = "neo4j",
        password: str = _DEFAULT_PASSWORD,
    ) -> None:
        self._uri = uri
        self._user = user
        self._password = password
        self._driver = None

    # -- Context manager ---------------------------------------------------

    def __enter__(self) -> GraphManaClient:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    def connect(self) -> None:
        """Open the Neo4j driver connection."""
        if self._driver is None:
            self._driver = GraphDatabase.driver(self._uri, auth=(self._user, self._password))
            self._driver.verify_connectivity()

    def close(self) -> None:
        """Close the Neo4j driver connection."""
        if self._driver is not None:
            self._driver.close()
            self._driver = None

    @property
    def driver(self):
        """Access the underlying neo4j Driver (auto-connects if needed)."""
        if self._driver is None:
            self.connect()
        return self._driver

    # -- Query helpers -----------------------------------------------------

    def _run(self, query: str, params: dict | None = None) -> list[dict]:
        """Run a Cypher query and return list of record dicts."""
        with self.driver.session() as session:
            result = session.run(query, params or {})
            return [dict(r) for r in result]

    def _run_single(self, query: str, params: dict | None = None) -> dict | None:
        """Run a Cypher query and return a single record dict or None."""
        with self.driver.session() as session:
            record = session.run(query, params or {}).single()
            return dict(record) if record else None

    def _to_df(self, records: list[dict]) -> pd.DataFrame:
        """Convert a list of record dicts to a DataFrame."""
        if not records:
            return pd.DataFrame()
        return pd.DataFrame(records)

    # -- Public API --------------------------------------------------------

    def status(self) -> dict:
        """Return database summary: node counts, schema metadata.

        Returns:
            Dict with ``counts`` (per-label) and ``schema`` metadata.
        """
        counts = {}
        for label in [
            "Variant",
            "Sample",
            "Population",
            "Chromosome",
            "Gene",
            "VCFHeader",
            "CohortDefinition",
            "AnnotationVersion",
        ]:
            rec = self._run_single(Q.STATUS_NODE_COUNT.format(label=label))
            counts[label] = rec["c"] if rec else 0

        meta_rec = self._run_single(Q.GET_SCHEMA_METADATA)
        schema = {}
        if meta_rec and "m" in meta_rec:
            node = meta_rec["m"]
            schema = dict(node) if hasattr(node, "items") else dict(node.items())

        return {"counts": counts, "schema": schema}

    def samples(self, *, include_excluded: bool = False) -> pd.DataFrame:
        """Return a DataFrame of samples.

        Args:
            include_excluded: If True, include soft-deleted samples.

        Returns:
            DataFrame with columns: sampleId, population, packed_index,
            sex, source_file, ingestion_date, and QC columns if available.
        """
        query = Q.FETCH_ALL_SAMPLES if include_excluded else Q.FETCH_SAMPLES
        return self._to_df(self._run(query))

    def populations(self) -> pd.DataFrame:
        """Return a DataFrame of populations with sample counts.

        Returns:
            DataFrame with columns: populationId, name, n_samples,
            n_active_samples, a_n, a_n2.
        """
        return self._to_df(self._run(Q.FETCH_POPULATIONS))

    def chromosomes(self) -> pd.DataFrame:
        """Return a DataFrame of chromosomes with variant counts.

        Returns:
            DataFrame with columns: chromosomeId, length, n_variants, aliases.
        """
        return self._to_df(self._run(Q.FETCH_CHROMOSOMES))

    def variants(
        self,
        chr: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> pd.DataFrame:
        """Return a DataFrame of variants on a chromosome or region.

        Args:
            chr: Chromosome ID (e.g. ``"22"`` or ``"chr22"``).
            start: Start position (inclusive). If None, returns all on chr.
            end: End position (inclusive).

        Returns:
            DataFrame with columns: variantId, chr, pos, ref, alt,
            variant_type, af_total, ac_total, an_total, call_rate,
            consequence, impact, gene_symbol.
        """
        if start is not None and end is not None:
            records = self._run(Q.FETCH_VARIANTS_REGION, {"chr": chr, "start": start, "end": end})
        else:
            records = self._run(Q.FETCH_VARIANTS_BY_CHR, {"chr": chr})
        return self._to_df(records)

    def genotype_matrix(
        self,
        chr: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> pd.DataFrame:
        """Return a samples-by-variants genotype matrix as a DataFrame.

        Values: 0=HomRef, 1=Het, 2=HomAlt, 3=Missing.

        This is a FULL PATH operation — it unpacks gt_packed arrays.
        Memory usage scales with n_samples * n_variants.

        Args:
            chr: Chromosome ID.
            start: Start position (inclusive).
            end: End position (inclusive).

        Returns:
            DataFrame with sample IDs as index, variant IDs as columns,
            and integer genotype codes as values.
        """
        # Get sample IDs and count
        sample_records = self._run(Q.FETCH_SAMPLES)
        if not sample_records:
            return pd.DataFrame()
        sample_ids = [r["sampleId"] for r in sample_records]
        n_samples = len(sample_ids)

        # Get variant genotypes
        if start is not None and end is not None:
            gt_records = self._run(
                Q.FETCH_VARIANT_GENOTYPES_REGION,
                {"chr": chr, "start": start, "end": end},
            )
        else:
            gt_records = self._run(Q.FETCH_VARIANT_GENOTYPES_BY_CHR, {"chr": chr})

        if not gt_records:
            return pd.DataFrame(index=sample_ids)

        # Build matrix: rows = samples, cols = variants
        variant_ids = []
        columns = []
        for rec in gt_records:
            gt_packed = rec["gt_packed"]
            if gt_packed is None:
                continue
            if isinstance(gt_packed, (list, bytearray)):
                gt_packed = bytes(gt_packed)
            gt = unpack_genotypes(gt_packed, n_samples)
            variant_ids.append(rec["variantId"])
            columns.append(gt)

        if not columns:
            return pd.DataFrame(index=sample_ids)

        matrix = np.column_stack(columns)
        return pd.DataFrame(matrix, index=sample_ids, columns=variant_ids)

    def allele_frequencies(
        self,
        chr: str,
        *,
        start: int | None = None,
        end: int | None = None,
    ) -> pd.DataFrame:
        """Return per-population allele frequencies (FAST PATH).

        Returns one row per variant with per-population ac, an, af arrays
        expanded into columns named ``af_<popId>``, ``ac_<popId>``, ``an_<popId>``.

        Args:
            chr: Chromosome ID.
            start: Start position (inclusive).
            end: End position (inclusive).

        Returns:
            DataFrame with variant info and per-population frequency columns.
        """
        if start is not None and end is not None:
            records = self._run(
                Q.FETCH_VARIANT_POP_ARRAYS_REGION,
                {"chr": chr, "start": start, "end": end},
            )
        else:
            records = self._run(Q.FETCH_VARIANT_POP_ARRAYS, {"chr": chr})

        if not records:
            return pd.DataFrame()

        rows = []
        for rec in records:
            row = {"variantId": rec["variantId"], "pos": rec["pos"]}
            pop_ids = rec.get("pop_ids") or []
            ac_arr = rec.get("ac") or []
            an_arr = rec.get("an") or []
            af_arr = rec.get("af") or []
            for i, pop in enumerate(pop_ids):
                row[f"ac_{pop}"] = ac_arr[i] if i < len(ac_arr) else None
                row[f"an_{pop}"] = an_arr[i] if i < len(an_arr) else None
                row[f"af_{pop}"] = af_arr[i] if i < len(af_arr) else None
            rows.append(row)

        return pd.DataFrame(rows)

    def annotation_versions(self) -> pd.DataFrame:
        """Return a DataFrame of annotation versions.

        Returns:
            DataFrame with columns: version_id, source, version,
            loaded_date, n_annotations, description.
        """
        return self._to_df(self._run(Q.FETCH_ANNOTATION_VERSIONS))

    def cohorts(self) -> pd.DataFrame:
        """Return a DataFrame of cohort definitions.

        Returns:
            DataFrame with columns: name, cypher_query, created_date, description.
        """
        return self._to_df(self._run(Q.FETCH_COHORTS))

    def gene_variants(self, gene_symbol: str) -> pd.DataFrame:
        """Return variants associated with a gene via HAS_CONSEQUENCE edges.

        Args:
            gene_symbol: Gene symbol (e.g. ``"BRCA1"``) or Ensembl ID.

        Returns:
            DataFrame with variant info and consequence/impact columns.
        """
        records = self._run(Q.GENE_VARIANTS, {"gene_symbol": gene_symbol})
        return self._to_df(records)

    def annotated_variants(self, annotation_version: str) -> pd.DataFrame:
        """Return variants with a specific annotation version.

        Args:
            annotation_version: Annotation version label.

        Returns:
            DataFrame with variant info, consequence, and annotation version.
        """
        records = self._run(Q.ANNOTATED_VARIANTS, {"annotation_version": annotation_version})
        return self._to_df(records)

    def cohort_samples(self, cohort_name: str) -> pd.DataFrame:
        """Return samples matching a cohort definition.

        Args:
            cohort_name: Name of the cohort definition.

        Returns:
            DataFrame with sampleId, population, packed_index, sex.
        """
        records = self._run(Q.COHORT_SAMPLES, {"cohort_name": cohort_name})
        return self._to_df(records)

    def filtered_variants(
        self,
        *,
        chr: str | None = None,
        start: int | None = None,
        end: int | None = None,
        variant_type: str | None = None,
        maf_min: float | None = None,
        maf_max: float | None = None,
        populations: list[str] | None = None,
        consequence: str | None = None,
        impact: str | None = None,
        gene: str | None = None,
    ) -> pd.DataFrame:
        """Return variants matching filter criteria.

        All parameters are optional — unset filters are ignored.

        Args:
            chr: Chromosome ID.
            start: Start position (inclusive).
            end: End position (inclusive).
            variant_type: Variant type (e.g. ``"SNP"``, ``"INDEL"``, ``"SV"``).
            maf_min: Minimum allele frequency.
            maf_max: Maximum allele frequency.
            populations: Filter to variants present in these populations.
            consequence: Consequence type (e.g. ``"missense_variant"``).
            impact: Impact level (e.g. ``"HIGH"``).
            gene: Gene symbol or Ensembl ID.

        Returns:
            DataFrame with variant info columns.
        """
        params = {
            "chr": chr,
            "start": start,
            "end": end,
            "variant_type": variant_type,
            "maf_min": maf_min,
            "maf_max": maf_max,
            "populations": populations,
            "consequence": consequence,
            "impact": impact,
            "gene": gene,
        }
        records = self._run(Q.FILTERED_VARIANTS, params)
        return self._to_df(records)

    def to_vcf(self, output_path: str, **filters) -> None:
        """Export data to VCF format via graphmana CLI.

        Args:
            output_path: Output VCF file path.
            **filters: Filter arguments passed to export command.
        """
        self._run_cli_export("vcf", output_path, **filters)

    def to_plink(self, output_path: str, **filters) -> None:
        """Export data to PLINK 1.9 format via graphmana CLI.

        Args:
            output_path: Output PLINK file prefix.
            **filters: Filter arguments passed to export command.
        """
        self._run_cli_export("plink", output_path, **filters)

    def to_treemix(self, output_path: str) -> None:
        """Export data to TreeMix format via graphmana CLI.

        Args:
            output_path: Output TreeMix file path.
        """
        self._run_cli_export("treemix", output_path)

    def _run_cli_export(self, fmt: str, output_path: str, **filters) -> None:
        """Run a graphmana CLI export command."""
        import subprocess

        cmd = [
            "graphmana",
            "export",
            "--format",
            fmt,
            "--output",
            output_path,
            "--neo4j-uri",
            self._uri,
            "--neo4j-user",
            self._user,
            "--neo4j-password",
            self._password,
        ]
        for key, val in filters.items():
            if val is not None:
                flag = f"--{key.replace('_', '-')}"
                cmd.extend([flag, str(val)])

        subprocess.run(cmd, check=True, capture_output=True, text=True)

    def query(self, cypher: str, params: dict | None = None) -> pd.DataFrame:
        """Run an arbitrary Cypher query and return results as a DataFrame.

        Args:
            cypher: Cypher query string.
            params: Query parameters.

        Returns:
            DataFrame with one column per RETURN field.
        """
        return self._to_df(self._run(cypher, params))
