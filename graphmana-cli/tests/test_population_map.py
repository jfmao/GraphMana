"""Tests for population map loading and building."""

import numpy as np
import pytest

from graphmana.ingest.population_map import build_pop_map, load_panel


class TestLoadPanel:
    """Test panel/PED file loading."""

    def test_panel_with_gender(self, tmp_path):
        """1000G panel format with 'gender' column."""
        panel = tmp_path / "panel.tsv"
        panel.write_text(
            "sample\tpop\tsuper_pop\tgender\n"
            "S1\tGBR\tEUR\tmale\n"
            "S2\tGBR\tEUR\tfemale\n"
            "S3\tYRI\tAFR\tmale\n"
        )
        sample_to_pop, sample_to_sex = load_panel(panel, "superpopulation")
        assert sample_to_pop == {"S1": "EUR", "S2": "EUR", "S3": "AFR"}
        assert sample_to_sex == {"S1": 1, "S2": 2, "S3": 1}

    def test_ped_with_sex_numeric(self, tmp_path):
        """PED format with numeric 'Sex' column (1=male, 2=female)."""
        ped = tmp_path / "panel.ped"
        ped.write_text(
            "SampleID\tPopulation\tSuperpopulation\tSex\n"
            "NA001\tGBR\tEUR\t1\n"
            "NA002\tGBR\tEUR\t2\n"
            "NA003\tCHS\tEAS\t1\n"
        )
        sample_to_pop, sample_to_sex = load_panel(ped, "superpopulation")
        assert sample_to_pop == {"NA001": "EUR", "NA002": "EUR", "NA003": "EAS"}
        assert sample_to_sex == {"NA001": 1, "NA002": 2, "NA003": 1}

    def test_panel_without_sex(self, tmp_path):
        """Panel without sex/gender column → empty sex dict."""
        panel = tmp_path / "panel.tsv"
        panel.write_text("sample\tpop\tsuper_pop\n" "R1\tJAP\tEAS\n" "R2\tIND\tSAS\n")
        sample_to_pop, sample_to_sex = load_panel(panel, "superpopulation")
        assert sample_to_pop == {"R1": "EAS", "R2": "SAS"}
        assert sample_to_sex == {}

    def test_population_stratify(self, tmp_path):
        """Stratify by 'population' instead of 'superpopulation'."""
        panel = tmp_path / "panel.tsv"
        panel.write_text("sample\tpop\tsuper_pop\n" "S1\tGBR\tEUR\n" "S2\tYRI\tAFR\n")
        sample_to_pop, _ = load_panel(panel, "population")
        assert sample_to_pop == {"S1": "GBR", "S2": "YRI"}

    def test_whitespace_separated(self, tmp_path):
        """Whitespace-separated PED file."""
        ped = tmp_path / "panel.ped"
        ped.write_text("SampleID Population Superpopulation\n" "NA001 GBR EUR\n" "NA002 CHS EAS\n")
        sample_to_pop, _ = load_panel(ped, "superpopulation")
        assert sample_to_pop == {"NA001": "EUR", "NA002": "EAS"}

    def test_missing_columns(self, tmp_path):
        """Raise ValueError when required columns are missing."""
        panel = tmp_path / "bad.tsv"
        panel.write_text("col_a\tcol_b\nval1\tval2\n")
        with pytest.raises(ValueError, match="Cannot find sample/population columns"):
            load_panel(panel, "population")


class TestBuildPopMap:
    """Test population map construction from VCF samples and panel data."""

    def test_intersection(self):
        """Only samples in both VCF and panel are included."""
        vcf_samples = ["S1", "S2", "S3", "S4"]
        sample_to_pop = {"S1": "POP_A", "S2": "POP_A", "S3": "POP_B"}
        # S4 is in VCF but not in panel → excluded

        pm = build_pop_map(vcf_samples, sample_to_pop)
        assert pm.sample_ids == ["S1", "S2", "S3"]
        assert pm.n_vcf_samples == 4
        assert set(pm.pop_ids) == {"POP_A", "POP_B"}

    def test_pop_indices(self):
        """Per-population index arrays point to VCF column positions."""
        vcf_samples = ["S1", "S2", "S3"]
        sample_to_pop = {"S1": "A", "S2": "B", "S3": "A"}

        pm = build_pop_map(vcf_samples, sample_to_pop)
        np.testing.assert_array_equal(pm.pop_to_indices["A"], [0, 2])
        np.testing.assert_array_equal(pm.pop_to_indices["B"], [1])

    def test_packed_index(self):
        """sample_packed_index maps to VCF column positions."""
        vcf_samples = ["X", "Y", "Z"]
        sample_to_pop = {"Y": "P1", "Z": "P1"}

        pm = build_pop_map(vcf_samples, sample_to_pop)
        assert pm.sample_packed_index == {"Y": 1, "Z": 2}

    def test_missing_samples_warning(self, caplog):
        """Warning is logged when VCF samples are missing from panel."""
        import logging

        with caplog.at_level(logging.WARNING):
            pm = build_pop_map(["A", "B", "C"], {"A": "P1"})
        assert pm.sample_ids == ["A"]
        assert "2 samples in VCF not found in panel" in caplog.text

    def test_sex_metadata(self):
        """Sex metadata is passed through."""
        vcf_samples = ["S1", "S2"]
        sample_to_pop = {"S1": "P", "S2": "P"}
        sample_to_sex = {"S1": 1, "S2": 2}

        pm = build_pop_map(vcf_samples, sample_to_pop, sample_to_sex)
        assert pm.sample_to_sex == {"S1": 1, "S2": 2}

    def test_n_samples_per_pop(self):
        """n_samples_per_pop counts are correct."""
        vcf_samples = ["S1", "S2", "S3", "S4"]
        sample_to_pop = {"S1": "A", "S2": "A", "S3": "B", "S4": "B"}

        pm = build_pop_map(vcf_samples, sample_to_pop)
        assert pm.n_samples_per_pop == {"A": 2, "B": 2}
