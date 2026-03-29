"""Structural safeguard tests — static analysis to prevent regression of known bug classes.

These tests scan source files for patterns that have caused bugs in the past,
without requiring a running Neo4j instance. They act as automated code review
for the most common and costly mistake patterns.

Bug classes covered:
1. Soft-delete enforcement: queries touching Sample nodes must filter excluded.
2. Export return contract: all exporters must return required summary keys.
3. Serialization completeness: ExportFilterConfig fields must survive pickling.
4. Phase convention consistency: all exporters must agree on phase-bit meaning.
5. ACTIVE_SAMPLE_FILTER constant usage: no hand-written filter strings.
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np

# Root of graphmana source
SRC_ROOT = Path(__file__).resolve().parent.parent / "src" / "graphmana"
EXPORT_DIR = SRC_ROOT / "export"


# ---------------------------------------------------------------------------
# B1. Soft-delete enforcement
# ---------------------------------------------------------------------------


class TestSoftDeleteEnforcement:
    """Verify every Cypher query touching Sample nodes filters soft-deleted samples.

    Scans all .py files under src/graphmana for Cypher query strings that MATCH
    on :Sample nodes. Each such query must either:
    - contain 'excluded' (indicating awareness of soft-delete), OR
    - be in an explicit whitelist of admin/maintenance queries.
    """

    # Queries that intentionally see ALL samples (including excluded)
    ADMIN_QUERY_NAMES = {
        # Sample management — must see excluded to operate on them
        "LIST_ALL_SAMPLES",
        "LIST_SAMPLES_BY_POPULATION",
        "GET_SAMPLE",
        "EXCLUDE_SAMPLES",
        "RESTORE_SAMPLES",
        "DELETE_SAMPLE_NODE",
        # Status/count queries that report both active and excluded
        "COUNT_SAMPLES_BY_STATUS",
        "COUNT_NODES",
        # Batch operations that work on all nodes regardless
        "CREATE_SAMPLE_BATCH",
        "CREATE_IN_POPULATION_BATCH",
        "UPDATE_SAMPLE_QC_BATCH",
        # Reassignment operates on specific named samples
        "REASSIGN_SAMPLE_POPULATION",
    }

    # Pattern: a triple-quoted string containing MATCH and :Sample
    _MATCH_SAMPLE_RE = re.compile(r"MATCH\s+\([^)]*:Sample[^)]*\)", re.IGNORECASE)

    def _find_sample_queries(self) -> list[tuple[str, str, str]]:
        """Find all (file, variable_name, query_string) tuples with MATCH (:Sample)."""
        results = []
        for py_file in SRC_ROOT.rglob("*.py"):
            text = py_file.read_text()
            # Find module-level string assignments (query constants)
            # Pattern: NAME = """ ... MATCH (:Sample ...) ... """
            for match in re.finditer(
                r'^([A-Z_]+)\s*=\s*f?"""(.*?)"""',
                text,
                re.MULTILINE | re.DOTALL,
            ):
                var_name = match.group(1)
                query_body = match.group(2)
                if self._MATCH_SAMPLE_RE.search(query_body):
                    results.append((str(py_file.relative_to(SRC_ROOT)), var_name, query_body))

            # Check inline session.run() / execute_read() calls.
            # Grab a broad context window (up to closing paren) to handle
            # multi-line f-string concatenation like:
            #   session.run(
            #       "MATCH (s:Sample) ... "
            #       f"AND ({ACTIVE_SAMPLE_FILTER}) "
            #       "RETURN ...",
            #       {"ids": ...},
            #   )
            for match in re.finditer(
                r"(?:execute_read|session\.run)\(",
                text,
            ):
                # Grab up to 600 chars after the call start to capture the full query
                call_context = text[match.start() : match.start() + 600]
                if self._MATCH_SAMPLE_RE.search(call_context):
                    line_no = text[: match.start()].count("\n") + 1
                    rel_path = str(py_file.relative_to(SRC_ROOT))
                    pseudo_name = f"inline@{rel_path}:{line_no}"
                    results.append((rel_path, pseudo_name, call_context))

        return results

    def test_all_sample_queries_filter_excluded(self):
        """Every non-admin MATCH (:Sample) query must reference 'excluded'."""
        queries = self._find_sample_queries()
        assert len(queries) > 0, "No Sample queries found — scan logic may be broken"

        violations = []
        for file_path, var_name, query_body in queries:
            # Skip whitelisted admin queries
            if var_name in self.ADMIN_QUERY_NAMES:
                continue
            # Check if query references 'excluded' anywhere
            if "excluded" not in query_body and "ACTIVE_SAMPLE_FILTER" not in query_body:
                violations.append(f"  {var_name} in {file_path}")

        assert not violations, (
            "Cypher queries touching :Sample without soft-delete filter:\n"
            + "\n".join(violations)
            + "\n\nEither add WHERE (ACTIVE_SAMPLE_FILTER) or whitelist in "
            "TestSoftDeleteEnforcement.ADMIN_QUERY_NAMES."
        )


# ---------------------------------------------------------------------------
# B2. Export return contract
# ---------------------------------------------------------------------------


class TestExportReturnContract:
    """Verify all exporter source files include the 4 required return dict keys."""

    REQUIRED_KEYS = {"n_variants", "n_samples", "chromosomes", "format"}

    def _get_exporter_files(self) -> list[Path]:
        """Get all *_export.py files."""
        return sorted(EXPORT_DIR.glob("*_export.py"))

    def test_all_exporters_exist(self):
        """Sanity check: we have exporter files to test."""
        files = self._get_exporter_files()
        assert len(files) >= 10, f"Expected 10+ exporter files, found {len(files)}"

    def test_all_exporters_return_required_keys(self):
        """Every exporter's return dict must include n_variants, n_samples, chromosomes, format."""
        files = self._get_exporter_files()
        violations = []

        for export_file in files:
            text = export_file.read_text()
            # Look for return { ... } patterns in the export method
            # We check if the file mentions all 4 required keys as dict keys
            missing = []
            for key in self.REQUIRED_KEYS:
                # Match "key": or 'key': patterns (dict literal keys)
                pattern = rf'"{key}"\s*:|' + rf"'{key}'\s*:"
                if not re.search(pattern, text):
                    missing.append(key)

            if missing:
                violations.append(f"  {export_file.name}: missing {', '.join(sorted(missing))}")

        assert not violations, "Exporter files missing required return dict keys:\n" + "\n".join(
            violations
        )


# ---------------------------------------------------------------------------
# B3. Serialization completeness
# ---------------------------------------------------------------------------


class TestSerializationCompleteness:
    """Verify ExportFilterConfig fields are fully serialized for parallel export."""

    def test_export_filter_config_fully_serialized(self):
        """Every ExportFilterConfig field must appear in _get_filter_config_dict."""
        from graphmana.export.parallel import _get_filter_config_dict
        from graphmana.filtering.export_filters import ExportFilterConfig

        dummy = ExportFilterConfig()
        serialized = _get_filter_config_dict(dummy)
        assert serialized is not None, "_get_filter_config_dict returned None for non-None config"

        missing = []
        for field_name in ExportFilterConfig.__dataclass_fields__:
            if field_name not in serialized:
                missing.append(field_name)

        assert not missing, (
            f"ExportFilterConfig fields missing from _get_filter_config_dict: {missing}\n"
            "Add them to graphmana/export/parallel.py::_get_filter_config_dict()."
        )

    def test_serialized_values_match_original(self):
        """Serialized values must match the original config field values."""
        from graphmana.export.parallel import _get_filter_config_dict
        from graphmana.filtering.export_filters import ExportFilterConfig

        config = ExportFilterConfig(
            populations=["POP1", "POP2"],
            maf_min=0.01,
            impacts=["HIGH"],
            sv_types={"DEL", "DUP"},
        )
        serialized = _get_filter_config_dict(config)

        assert serialized["populations"] == ["POP1", "POP2"]
        assert serialized["maf_min"] == 0.01
        assert serialized["impacts"] == ["HIGH"]
        assert serialized["sv_types"] == {"DEL", "DUP"}
        # chromosomes is intentionally overridden to None
        assert serialized["chromosomes"] is None

    def test_none_config_returns_none(self):
        """_get_filter_config_dict(None) should return None."""
        from graphmana.export.parallel import _get_filter_config_dict

        assert _get_filter_config_dict(None) is None


# ---------------------------------------------------------------------------
# B4. Phase convention consistency
# ---------------------------------------------------------------------------


class TestPhaseConventionConsistency:
    """Verify all phase-aware exporters agree on the phase-bit convention.

    Canonical reference: vcf_export.py::format_gt
      - phase_bit=1 at Het -> 0|1 (ALT on second haplotype)
      - phase_bit=0 at Het -> 1|0 (ALT on first haplotype)
    """

    def test_vcf_format_gt_canonical(self):
        """Verify format_gt is the canonical reference for phase convention."""
        from graphmana.export.vcf_export import format_gt

        # phase=1, Het -> 0|1 (ALT on second haplotype)
        assert format_gt(gt=1, phase=1, haploid=False, phased=True) == "0|1"
        # phase=0, Het -> 1|0 (ALT on first haplotype)
        assert format_gt(gt=1, phase=0, haploid=False, phased=True) == "1|0"
        # Non-het cases
        assert format_gt(gt=0, phase=0, haploid=False, phased=True) == "0|0"
        assert format_gt(gt=2, phase=0, haploid=False, phased=True) == "1|1"
        assert format_gt(gt=3, phase=0, haploid=False, phased=True) == ".|."

    def test_hap_export_phase_consistency(self):
        """HAP exporter phase convention must match VCF canonical."""
        from graphmana.export.hap_export import gt_phase_to_haplotypes

        gt = np.array([1, 1, 0, 2, 3], dtype=np.int8)
        phase = np.array([1, 0, 0, 0, 0], dtype=np.uint8)

        hap1, hap2 = gt_phase_to_haplotypes(gt, phase)

        # Het, phase=1: ALT on second haplotype -> hap1=0(REF), hap2=1(ALT)
        assert hap1[0] == 0 and hap2[0] == 1
        # Het, phase=0: ALT on first haplotype -> hap1=1(ALT), hap2=0(REF)
        assert hap1[1] == 1 and hap2[1] == 0
        # HomRef: 0, 0
        assert hap1[2] == 0 and hap2[2] == 0
        # HomAlt: 1, 1
        assert hap1[3] == 1 and hap2[3] == 1
        # Missing: 0, 0 (treated as ref)
        assert hap1[4] == 0 and hap2[4] == 0

    def test_structure_export_phase_consistency(self):
        """STRUCTURE exporter phase convention must match VCF canonical."""
        from graphmana.export.structure_export import gt_to_structure_alleles

        gt = np.array([1, 1, 0, 2, 3], dtype=np.int8)
        phase = np.array([1, 0, 0, 0, 0], dtype=np.uint8)

        a1, a2 = gt_to_structure_alleles(gt, phase)

        # Het, phase=1: ALT on second -> a1=1(REF), a2=2(ALT)
        assert a1[0] == 1 and a2[0] == 2
        # Het, phase=0: ALT on first -> a1=2(ALT), a2=1(REF)
        assert a1[1] == 2 and a2[1] == 1
        # HomRef: 1, 1
        assert a1[2] == 1 and a2[2] == 1
        # HomAlt: 2, 2
        assert a1[3] == 2 and a2[3] == 2
        # Missing: -9, -9
        assert a1[4] == -9 and a2[4] == -9

    def test_beagle_export_phase_consistency(self):
        """Beagle exporter phase convention must match VCF canonical."""
        from graphmana.export.beagle_export import format_beagle_variant_line

        gt = np.array([1, 1], dtype=np.int8)
        phase = np.array([1, 0], dtype=np.uint8)
        props = {"variantId": "chr1_100_A_T"}

        line = format_beagle_variant_line(props, gt, phase, "A", "T")
        parts = line.split("\t")

        # parts[0]=marker, parts[1]=ref, parts[2]=alt, then pairs per sample
        # Sample 0: Het, phase=1 -> (REF, ALT) = (A, T)
        assert parts[3] == "A" and parts[4] == "T"
        # Sample 1: Het, phase=0 -> (ALT, REF) = (T, A)
        assert parts[5] == "T" and parts[6] == "A"


# ---------------------------------------------------------------------------
# B5. ACTIVE_SAMPLE_FILTER constant usage
# ---------------------------------------------------------------------------


class TestActiveFilterConstant:
    """Verify ACTIVE_SAMPLE_FILTER constant is used everywhere, not hand-written copies."""

    def test_constant_value_is_correct(self):
        """Verify the constant has the expected value."""
        from graphmana.db.queries import ACTIVE_SAMPLE_FILTER

        assert ACTIVE_SAMPLE_FILTER == "s.excluded IS NULL OR s.excluded = false"

    def test_no_handwritten_filter_strings(self):
        """No source file should contain hand-written 's.excluded IS NULL' outside the constant def.

        Allows:
        - The ACTIVE_SAMPLE_FILTER constant definition itself.
        - f-string queries that reference {ACTIVE_SAMPLE_FILTER}.
        - CASE WHEN expressions (e.g., COUNT_SAMPLES_BY_STATUS) that use the
          pattern for conditional counting, not as a WHERE filter.
        """
        # The literal string that should only appear in the constant definition
        handwritten_pattern = re.compile(
            r"s\.excluded\s+IS\s+NULL\s+OR\s+s\.excluded\s*=\s*false",
            re.IGNORECASE,
        )

        violations = []
        for py_file in SRC_ROOT.rglob("*.py"):
            text = py_file.read_text()
            rel_path = str(py_file.relative_to(SRC_ROOT))

            for match in handwritten_pattern.finditer(text):
                # Find the line
                line_no = text[: match.start()].count("\n") + 1
                line = text.splitlines()[line_no - 1].strip()

                # Allow the constant definition itself
                if "ACTIVE_SAMPLE_FILTER" in line and "=" in line.split("ACTIVE_SAMPLE_FILTER")[0]:
                    continue

                # Allow f-string interpolations that reference the constant
                context_start = max(0, match.start() - 200)
                context = text[context_start : match.end() + 50]
                if "ACTIVE_SAMPLE_FILTER" in context:
                    continue

                # Allow CASE WHEN expressions (used for conditional counting)
                if "CASE WHEN" in line or "CASE WHEN" in text.splitlines()[max(0, line_no - 2)]:
                    continue

                violations.append(f"  {rel_path}:{line_no}: {line}")

        assert not violations, (
            "Hand-written soft-delete filter strings found (use ACTIVE_SAMPLE_FILTER instead):\n"
            + "\n".join(violations)
        )

    def test_constant_is_importable(self):
        """ACTIVE_SAMPLE_FILTER must be importable from db.queries."""
        from graphmana.db.queries import ACTIVE_SAMPLE_FILTER

        assert isinstance(ACTIVE_SAMPLE_FILTER, str)
        assert len(ACTIVE_SAMPLE_FILTER) > 10
