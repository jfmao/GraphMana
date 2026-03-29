"""Tests for bcftools norm wrapper."""

from unittest.mock import MagicMock, patch

import pytest

from graphmana.ingest.normalizer import NormalizationResult, normalize_vcf


class TestNormalizeVcf:
    """Test normalize_vcf function."""

    @patch("graphmana.ingest.normalizer.shutil.which", return_value="/usr/bin/bcftools")
    @patch("graphmana.ingest.normalizer.subprocess.run")
    def test_basic_normalization(self, mock_run, mock_which, tmp_path):
        mock_run.return_value = MagicMock(
            returncode=0,
            stderr=("Lines   total/split/realigned/skipped:\t1000/50/30/5\n"),
        )
        result = normalize_vcf(
            str(tmp_path / "input.vcf.gz"),
            str(tmp_path / "output.vcf.gz"),
            str(tmp_path / "ref.fa"),
        )
        assert isinstance(result, NormalizationResult)
        assert result.input_path == str(tmp_path / "input.vcf.gz")
        assert result.output_path == str(tmp_path / "output.vcf.gz")
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "/usr/bin/bcftools"
        assert "norm" in cmd

    @patch("graphmana.ingest.normalizer.shutil.which", return_value="/usr/bin/bcftools")
    @patch("graphmana.ingest.normalizer.subprocess.run")
    def test_left_align_adds_ref_flag(self, mock_run, mock_which, tmp_path):
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        normalize_vcf(tmp_path / "in.vcf", tmp_path / "out.vcf", tmp_path / "ref.fa")
        cmd = mock_run.call_args[0][0]
        assert "-f" in cmd
        ref_idx = cmd.index("-f")
        assert cmd[ref_idx + 1] == str(tmp_path / "ref.fa")

    @patch("graphmana.ingest.normalizer.shutil.which", return_value="/usr/bin/bcftools")
    @patch("graphmana.ingest.normalizer.subprocess.run")
    def test_no_left_align(self, mock_run, mock_which, tmp_path):
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        normalize_vcf(
            tmp_path / "in.vcf",
            tmp_path / "out.vcf",
            tmp_path / "ref.fa",
            left_align=False,
        )
        cmd = mock_run.call_args[0][0]
        assert "-f" not in cmd

    @patch("graphmana.ingest.normalizer.shutil.which", return_value="/usr/bin/bcftools")
    @patch("graphmana.ingest.normalizer.subprocess.run")
    def test_split_multiallelic_flag(self, mock_run, mock_which, tmp_path):
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        normalize_vcf(tmp_path / "in.vcf", tmp_path / "out.vcf", tmp_path / "ref.fa")
        cmd = mock_run.call_args[0][0]
        assert "-m" in cmd
        m_idx = cmd.index("-m")
        assert cmd[m_idx + 1] == "-both"

    @patch("graphmana.ingest.normalizer.shutil.which", return_value="/usr/bin/bcftools")
    @patch("graphmana.ingest.normalizer.subprocess.run")
    def test_no_split_multiallelic(self, mock_run, mock_which, tmp_path):
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        normalize_vcf(
            tmp_path / "in.vcf",
            tmp_path / "out.vcf",
            tmp_path / "ref.fa",
            split_multiallelic=False,
        )
        cmd = mock_run.call_args[0][0]
        assert "-m" not in cmd

    @patch("graphmana.ingest.normalizer.shutil.which", return_value=None)
    def test_bcftools_not_found(self, mock_which, tmp_path):
        with pytest.raises(RuntimeError, match="bcftools not found"):
            normalize_vcf(tmp_path / "in.vcf", tmp_path / "out.vcf", tmp_path / "ref.fa")

    @patch("graphmana.ingest.normalizer.shutil.which", return_value="/usr/bin/bcftools")
    @patch("graphmana.ingest.normalizer.subprocess.run")
    def test_bcftools_nonzero_exit(self, mock_run, mock_which, tmp_path):
        mock_run.return_value = MagicMock(
            returncode=1,
            stderr="Error: failed to open reference",
        )
        with pytest.raises(RuntimeError, match="bcftools norm failed"):
            normalize_vcf(tmp_path / "in.vcf", tmp_path / "out.vcf", tmp_path / "ref.fa")

    @patch("graphmana.ingest.normalizer.shutil.which", return_value="/usr/bin/bcftools")
    @patch("graphmana.ingest.normalizer.subprocess.run")
    def test_output_format_gz(self, mock_run, mock_which, tmp_path):
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        normalize_vcf(tmp_path / "in.vcf", tmp_path / "out.vcf.gz", tmp_path / "ref.fa")
        cmd = mock_run.call_args[0][0]
        assert "-Oz" in cmd

    @patch("graphmana.ingest.normalizer.shutil.which", return_value="/usr/bin/bcftools")
    @patch("graphmana.ingest.normalizer.subprocess.run")
    def test_output_format_bcf(self, mock_run, mock_which, tmp_path):
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        normalize_vcf(tmp_path / "in.vcf", tmp_path / "out.bcf", tmp_path / "ref.fa")
        cmd = mock_run.call_args[0][0]
        assert "-Ob" in cmd

    @patch("graphmana.ingest.normalizer.shutil.which", return_value="/usr/bin/bcftools")
    @patch("graphmana.ingest.normalizer.subprocess.run")
    def test_output_format_plain_vcf(self, mock_run, mock_which, tmp_path):
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        normalize_vcf(tmp_path / "in.vcf", tmp_path / "out.vcf", tmp_path / "ref.fa")
        cmd = mock_run.call_args[0][0]
        assert "-Ov" in cmd

    @patch("graphmana.ingest.normalizer.shutil.which", return_value="/usr/bin/bcftools")
    @patch("graphmana.ingest.normalizer.subprocess.run")
    def test_trim_implicit_with_left_align(self, mock_run, mock_which, tmp_path):
        """Trimming is implicit with -f (left-alignment); no separate flag."""
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        normalize_vcf(tmp_path / "in.vcf", tmp_path / "out.vcf", tmp_path / "ref.fa")
        cmd = mock_run.call_args[0][0]
        assert "-f" in cmd
        assert "-D" not in cmd

    @patch("graphmana.ingest.normalizer.shutil.which", return_value="/usr/bin/bcftools")
    @patch("graphmana.ingest.normalizer.subprocess.run")
    def test_trim_no_effect_without_left_align(self, mock_run, mock_which, tmp_path):
        """trim=True has no effect when left_align=False (no bcftools trim flag)."""
        mock_run.return_value = MagicMock(returncode=0, stderr="")
        normalize_vcf(
            tmp_path / "in.vcf",
            tmp_path / "out.vcf",
            tmp_path / "ref.fa",
            left_align=False,
            trim=True,
        )
        cmd = mock_run.call_args[0][0]
        assert "-f" not in cmd
        assert "-D" not in cmd
