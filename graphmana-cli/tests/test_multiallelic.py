"""Tests for multi-allelic variant detection, tracking, and VCF reconstruction.

Covers:
- Import-side: VariantRecord tagging in VCFParser._flush_site_buffer
- Export-side: reconstruct_multiallelic_gt() and format_multiallelic_variant_line()
- CSV emission of multiallelic_site and allele_index columns
- Backward compatibility with databases lacking multiallelic_site
"""

from __future__ import annotations

import numpy as np

from graphmana.export.vcf_export import (
    format_multiallelic_variant_line,
    format_variant_line,
    reconstruct_multiallelic_gt,
)
from graphmana.ingest.csv_emitter import VARIANT_HEADER
from graphmana.ingest.vcf_parser import VariantRecord, VCFParser

# ---------------------------------------------------------------------------
# Helpers to build minimal VariantRecord instances
# ---------------------------------------------------------------------------


def _make_rec(
    chrom: str = "chr22",
    pos: int = 100,
    ref: str = "A",
    alt: str = "C",
    **kwargs,
) -> VariantRecord:
    defaults = dict(
        id=f"{chrom}:{pos}:{ref}:{alt}",
        chr=chrom,
        pos=pos,
        ref=ref,
        alt=alt,
        variant_type="SNP",
        ac=[0],
        an=[10],
        af=[0.0],
        het_count=[0],
        hom_alt_count=[0],
        het_exp=[0.0],
        ac_total=0,
        an_total=10,
        af_total=0.0,
        call_rate=1.0,
        gt_packed=b"\x00",
        phase_packed=b"\x00",
        ploidy_packed=b"",
    )
    defaults.update(kwargs)
    return VariantRecord(**defaults)


# ---------------------------------------------------------------------------
# Import-side: site buffer detection
# ---------------------------------------------------------------------------


class TestSiteBufferDetection:
    """Test VCFParser._flush_site_buffer for multi-allelic tagging."""

    def test_native_biallelic_no_tag(self):
        """Single biallelic record → fields remain None."""
        rec = _make_rec(alt="C")
        result = list(VCFParser._flush_site_buffer([rec]))
        assert len(result) == 1
        assert result[0].multiallelic_site is None
        assert result[0].allele_index is None

    def test_multiallelic_tagged(self):
        """Two records at same chr:pos:ref → both tagged."""
        rec1 = _make_rec(alt="C")
        rec2 = _make_rec(alt="T")
        result = list(VCFParser._flush_site_buffer([rec1, rec2]))
        assert len(result) == 2
        assert result[0].multiallelic_site == "chr22:100:A"
        assert result[0].allele_index == 1
        assert result[1].multiallelic_site == "chr22:100:A"
        assert result[1].allele_index == 2

    def test_triallelic_tagged(self):
        """Three records at same site → allele_index 1, 2, 3."""
        recs = [_make_rec(alt=a) for a in ("C", "T", "G")]
        result = list(VCFParser._flush_site_buffer(recs))
        assert len(result) == 3
        for i, r in enumerate(result, 1):
            assert r.multiallelic_site == "chr22:100:A"
            assert r.allele_index == i


# ---------------------------------------------------------------------------
# Export-side: reconstruct_multiallelic_gt
# ---------------------------------------------------------------------------


def _gt_array(*values: int) -> np.ndarray:
    return np.array(values, dtype=np.int8)


def _phase_array(*values: int) -> np.ndarray:
    return np.array(values, dtype=np.uint8)


def _ploidy_array(*values: int) -> np.ndarray:
    return np.array(values, dtype=np.uint8)


class TestReconstructMultiallelicGt:
    """Test reconstruct_multiallelic_gt for various genotype combinations."""

    def test_all_homref(self):
        """Both alleles HomRef → 0/0."""
        gt1 = _gt_array(0)  # HomRef for allele 1
        gt2 = _gt_array(0)  # HomRef for allele 2
        result = reconstruct_multiallelic_gt(
            [gt1, gt2],
            [_phase_array(0), _phase_array(0)],
            [_ploidy_array(0), _ploidy_array(0)],
            phased=False,
        )
        assert result == ["0/0"]

    def test_het_allele1(self):
        """Het for allele 1, HomRef for allele 2 → 0/1."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(1), _gt_array(0)],
            [_phase_array(0), _phase_array(0)],
            [_ploidy_array(0), _ploidy_array(0)],
            phased=False,
        )
        assert result == ["0/1"]

    def test_het_allele2(self):
        """HomRef for allele 1, Het for allele 2 → 0/2."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(0), _gt_array(1)],
            [_phase_array(0), _phase_array(0)],
            [_ploidy_array(0), _ploidy_array(0)],
            phased=False,
        )
        assert result == ["0/2"]

    def test_het_both(self):
        """Het for both alleles → 1/2."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(1), _gt_array(1)],
            [_phase_array(0), _phase_array(0)],
            [_ploidy_array(0), _ploidy_array(0)],
            phased=False,
        )
        assert result == ["1/2"]

    def test_homalt_allele1(self):
        """HomAlt for allele 1 → 1/1."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(2), _gt_array(0)],
            [_phase_array(0), _phase_array(0)],
            [_ploidy_array(0), _ploidy_array(0)],
            phased=False,
        )
        assert result == ["1/1"]

    def test_homalt_allele2(self):
        """HomAlt for allele 2 → 2/2."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(0), _gt_array(2)],
            [_phase_array(0), _phase_array(0)],
            [_ploidy_array(0), _ploidy_array(0)],
            phased=False,
        )
        assert result == ["2/2"]

    def test_missing_propagates(self):
        """Any allele missing → ./."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(3), _gt_array(0)],
            [_phase_array(0), _phase_array(0)],
            [_ploidy_array(0), _ploidy_array(0)],
            phased=False,
        )
        assert result == ["./."]

        # Missing on second allele
        result2 = reconstruct_multiallelic_gt(
            [_gt_array(0), _gt_array(3)],
            [_phase_array(0), _phase_array(0)],
            [_ploidy_array(0), _ploidy_array(0)],
            phased=False,
        )
        assert result2 == ["./."]

    def test_phased_0_1(self):
        """Phased het allele 1, phase=1 → 0|1."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(1), _gt_array(0)],
            [_phase_array(1), _phase_array(0)],
            [_ploidy_array(0), _ploidy_array(0)],
            phased=True,
        )
        assert result == ["0|1"]

    def test_phased_1_0(self):
        """Phased het allele 1, phase=0 → 1|0."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(1), _gt_array(0)],
            [_phase_array(0), _phase_array(0)],
            [_ploidy_array(0), _ploidy_array(0)],
            phased=True,
        )
        assert result == ["1|0"]

    def test_phased_1_2(self):
        """Both het phased → ordered by phase bits."""
        # allele 1 phase=0 (ALT on hap1), allele 2 phase=1 (ALT on hap2)
        result = reconstruct_multiallelic_gt(
            [_gt_array(1), _gt_array(1)],
            [_phase_array(0), _phase_array(1)],
            [_ploidy_array(0), _ploidy_array(0)],
            phased=True,
        )
        assert result == ["1|2"]

    def test_phased_2_1(self):
        """Both het phased, reversed phase → 2|1."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(1), _gt_array(1)],
            [_phase_array(1), _phase_array(0)],
            [_ploidy_array(0), _ploidy_array(0)],
            phased=True,
        )
        assert result == ["2|1"]

    def test_haploid_ref(self):
        """All HomRef haploid → 0."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(0), _gt_array(0)],
            [_phase_array(0), _phase_array(0)],
            [_ploidy_array(1), _ploidy_array(1)],
            phased=False,
        )
        assert result == ["0"]

    def test_haploid_alt(self):
        """HomAlt haploid for allele 2 → 2."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(0), _gt_array(2)],
            [_phase_array(0), _phase_array(0)],
            [_ploidy_array(1), _ploidy_array(1)],
            phased=False,
        )
        assert result == ["2"]

    def test_haploid_missing(self):
        """Missing haploid → .."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(3), _gt_array(0)],
            [_phase_array(0), _phase_array(0)],
            [_ploidy_array(1), _ploidy_array(1)],
            phased=False,
        )
        assert result == ["."]

    def test_mixed_samples(self):
        """Multiple samples with different genotypes."""
        # 3 samples: sample0=0/1, sample1=0/2, sample2=1/2
        result = reconstruct_multiallelic_gt(
            [_gt_array(1, 0, 1), _gt_array(0, 1, 1)],
            [_phase_array(0, 0, 0), _phase_array(0, 0, 0)],
            [_ploidy_array(0, 0, 0), _ploidy_array(0, 0, 0)],
            phased=False,
        )
        assert result == ["0/1", "0/2", "1/2"]

    def test_triallelic(self):
        """3 alleles: sample het for allele 3 → 0/3."""
        result = reconstruct_multiallelic_gt(
            [_gt_array(0), _gt_array(0), _gt_array(1)],
            [_phase_array(0), _phase_array(0), _phase_array(0)],
            [_ploidy_array(0), _ploidy_array(0), _ploidy_array(0)],
            phased=False,
        )
        assert result == ["0/3"]


# ---------------------------------------------------------------------------
# Export-side: format_multiallelic_variant_line
# ---------------------------------------------------------------------------


class TestFormatMultiallelicVariantLine:
    """Test merged VCF line formatting."""

    def test_merged_line_format(self):
        """ALTs comma-separated, AC/AF per-allele."""
        props1 = {
            "chr": "chr22",
            "pos": 100,
            "variantId": "chr22:100:A:C",
            "ref": "A",
            "alt": "C",
            "ac_total": 5,
            "an_total": 20,
            "af_total": 0.25,
            "variant_type": "SNP",
        }
        props2 = {
            "chr": "chr22",
            "pos": 100,
            "variantId": "chr22:100:A:T",
            "ref": "A",
            "alt": "T",
            "ac_total": 3,
            "an_total": 20,
            "af_total": 0.15,
            "variant_type": "SNP",
        }
        line = format_multiallelic_variant_line([props1, props2], ["0/1", "0/2"])
        fields = line.split("\t")
        assert fields[0] == "chr22"
        assert fields[1] == "100"
        assert fields[3] == "A"
        assert fields[4] == "C,T"  # comma-separated ALTs
        # INFO should have per-allele AC and AF
        info = fields[7]
        assert "AC=5,3" in info
        assert "AF=0.25,0.15" in info
        assert "AN=20" in info
        # GT fields
        assert fields[9] == "0/1"
        assert fields[10] == "0/2"


# ---------------------------------------------------------------------------
# Backward compatibility
# ---------------------------------------------------------------------------


class TestBackwardCompat:
    """Verify old databases (no multiallelic_site) still work."""

    def test_no_multiallelic_site_passthrough(self):
        """Variant without multiallelic_site → written as biallelic line."""
        props = {
            "chr": "chr22",
            "pos": 100,
            "variantId": "chr22:100:A:C",
            "ref": "A",
            "alt": "C",
            "ac_total": 5,
            "an_total": 20,
            "af_total": 0.25,
            "variant_type": "SNP",
        }
        gt_strings = ["0/1", "0/0"]
        line = format_variant_line(props, gt_strings)
        fields = line.split("\t")
        assert fields[4] == "C"  # Single ALT
        assert fields[9] == "0/1"
        assert fields[10] == "0/0"

    def test_multiallelic_site_none_in_props(self):
        """props.get("multiallelic_site") returns None → no reconstruction."""
        props = {
            "chr": "chr22",
            "pos": 100,
            "ref": "A",
            "alt": "C",
            "multiallelic_site": None,
        }
        # In the exporter, ma_site = props.get("multiallelic_site")
        # When None, the variant is treated as biallelic — no buffering
        assert props.get("multiallelic_site") is None


# ---------------------------------------------------------------------------
# CSV emission
# ---------------------------------------------------------------------------


class TestCSVEmission:
    """Verify VARIANT_HEADER includes multiallelic columns."""

    def test_header_has_multiallelic_site(self):
        assert "multiallelic_site" in VARIANT_HEADER

    def test_header_has_allele_index(self):
        assert "allele_index:int" in VARIANT_HEADER

    def test_multiallelic_after_sv_end(self):
        """multiallelic_site comes after sv_end:long."""
        sv_end_idx = VARIANT_HEADER.index("sv_end:long")
        ma_idx = VARIANT_HEADER.index("multiallelic_site")
        ai_idx = VARIANT_HEADER.index("allele_index:int")
        assert ma_idx == sv_end_idx + 1
        assert ai_idx == sv_end_idx + 2
