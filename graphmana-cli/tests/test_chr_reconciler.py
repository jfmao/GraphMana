"""Tests for chromosome naming reconciliation."""

import pytest

from graphmana.ingest.chr_reconciler import ChrReconciler


class TestAutoDetect:
    """Test automatic chromosome style detection."""

    def test_ucsc_majority(self):
        rec = ChrReconciler()
        style = rec.detect_style(["chr1", "chr2", "chr3", "chrX", "chrM"])
        assert style == "ucsc"

    def test_ensembl_majority(self):
        rec = ChrReconciler()
        style = rec.detect_style(["1", "2", "3", "X", "MT"])
        assert style == "ensembl"

    def test_mixed_ucsc_wins(self):
        rec = ChrReconciler()
        style = rec.detect_style(["chr1", "chr2", "3", "chrX"])
        assert style == "ucsc"

    def test_empty(self):
        rec = ChrReconciler()
        style = rec.detect_style([])
        assert style == "unknown"

    def test_non_standard_passthrough(self):
        """Non-standard names (e.g. rice chromosomes) result in unknown."""
        rec = ChrReconciler()
        style = rec.detect_style(["Os01", "Os02", "Os03"])
        assert style == "unknown"


class TestNormalize:
    """Test chromosome name normalization."""

    def test_ensembl_to_ucsc(self):
        rec = ChrReconciler(chr_style="ucsc")
        assert rec.normalize("1") == "chr1"
        assert rec.normalize("X") == "chrX"
        assert rec.normalize("22") == "chr22"

    def test_ucsc_to_ensembl(self):
        rec = ChrReconciler(chr_style="ensembl")
        assert rec.normalize("chr1") == "1"
        assert rec.normalize("chrX") == "X"
        assert rec.normalize("chr22") == "22"

    def test_mito_to_ucsc(self):
        rec = ChrReconciler(chr_style="ucsc")
        assert rec.normalize("MT") == "chrM"
        assert rec.normalize("chrMT") == "chrM"
        assert rec.normalize("M") == "chrM"

    def test_mito_to_ensembl(self):
        rec = ChrReconciler(chr_style="ensembl")
        assert rec.normalize("chrM") == "MT"
        assert rec.normalize("chrMT") == "MT"

    def test_unknown_passthrough(self):
        """Unrecognized names pass through unchanged in ucsc mode."""
        rec = ChrReconciler(chr_style="ucsc")
        assert rec.normalize("scaffold_123") == "scaffold_123"

    def test_original_passthrough(self):
        """'original' mode never transforms."""
        rec = ChrReconciler(chr_style="original")
        assert rec.normalize("chr1") == "chr1"
        assert rec.normalize("1") == "1"
        assert rec.normalize("MT") == "MT"

    def test_auto_with_detection(self):
        """Auto mode uses detected style."""
        rec = ChrReconciler(chr_style="auto")
        rec.detect_style(["chr1", "chr2", "chrX"])
        # Detected as ucsc — so ensembl names get converted to ucsc
        assert rec.normalize("1") == "chr1"


class TestChrMap:
    """Test custom chromosome mapping file."""

    def test_custom_mapping(self, tmp_path):
        map_file = tmp_path / "chr_map.tsv"
        map_file.write_text("Os01\tchr1\nOs02\tchr2\n")
        rec = ChrReconciler(chr_map_path=map_file)
        assert rec.normalize("Os01") == "chr1"
        assert rec.normalize("Os02") == "chr2"
        assert rec.normalize("Os03") == "Os03"  # unmapped passthrough

    def test_with_header(self, tmp_path):
        """Header line starting with 'source' is skipped."""
        map_file = tmp_path / "chr_map.tsv"
        map_file.write_text("source\ttarget\nscaffold_1\tchr1\n")
        rec = ChrReconciler(chr_map_path=map_file)
        assert rec.normalize("scaffold_1") == "chr1"

    def test_custom_overrides_style(self, tmp_path):
        """Custom map takes priority over style conversion."""
        map_file = tmp_path / "chr_map.tsv"
        map_file.write_text("chr1\tChromosome_1\n")
        rec = ChrReconciler(chr_style="ensembl", chr_map_path=map_file)
        # Custom map overrides ensembl conversion
        assert rec.normalize("chr1") == "Chromosome_1"


class TestAliases:
    """Test alias tracking during normalization."""

    def test_aliases_recorded(self):
        rec = ChrReconciler(chr_style="ucsc")
        rec.normalize("1")
        rec.normalize("X")
        rec.normalize("chr1")  # already ucsc, no alias

        aliases = rec.aliases
        assert "chr1" in aliases
        assert "1" in aliases["chr1"]
        assert "chrX" in aliases
        assert "X" in aliases["chrX"]

    def test_no_aliases_for_original(self):
        rec = ChrReconciler(chr_style="original")
        rec.normalize("chr1")
        rec.normalize("1")
        assert rec.aliases == {}


class TestInvalidStyle:
    def test_rejects_bad_style(self):
        with pytest.raises(ValueError, match="Invalid chr_style"):
            ChrReconciler(chr_style="grch38")
