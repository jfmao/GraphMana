"""Tests for import-time variant filters."""

from graphmana.filtering.import_filters import ImportFilterChain, ImportFilterConfig
from graphmana.ingest.vcf_parser import VariantRecord


def _make_record(
    *,
    qual=30.0,
    call_rate=0.95,
    af_total=0.1,
    variant_type="SNP",
    filter_status=None,
):
    """Create a minimal VariantRecord for filter testing."""
    return VariantRecord(
        id="chr1:100:A:T",
        chr="chr1",
        pos=100,
        ref="A",
        alt="T",
        variant_type=variant_type,
        ac=[10],
        an=[100],
        af=[0.1],
        het_count=[8],
        hom_alt_count=[1],
        het_exp=[0.18],
        ac_total=10,
        an_total=100,
        af_total=af_total,
        call_rate=call_rate,
        qual=qual,
        filter_status=filter_status,
    )


# ---------------------------------------------------------------------------
# Individual filter tests
# ---------------------------------------------------------------------------


class TestQualFilter:
    def test_above_threshold_passes(self):
        cfg = ImportFilterConfig(min_qual=20.0)
        chain = ImportFilterChain(cfg)
        rec = _make_record(qual=30.0)
        assert chain.accepts(rec)

    def test_below_threshold_rejected(self):
        cfg = ImportFilterConfig(min_qual=50.0)
        chain = ImportFilterChain(cfg)
        rec = _make_record(qual=30.0)
        assert not chain.accepts(rec)

    def test_equal_threshold_passes(self):
        cfg = ImportFilterConfig(min_qual=30.0)
        chain = ImportFilterChain(cfg)
        rec = _make_record(qual=30.0)
        assert chain.accepts(rec)

    def test_none_qual_passes(self):
        """Missing QUAL should not be rejected."""
        cfg = ImportFilterConfig(min_qual=20.0)
        chain = ImportFilterChain(cfg)
        rec = _make_record(qual=None)
        assert chain.accepts(rec)

    def test_no_filter_passes_all(self):
        cfg = ImportFilterConfig()
        chain = ImportFilterChain(cfg)
        rec = _make_record(qual=1.0)
        assert chain.accepts(rec)


class TestCallRateFilter:
    def test_above_threshold_passes(self):
        cfg = ImportFilterConfig(min_call_rate=0.9)
        chain = ImportFilterChain(cfg)
        rec = _make_record(call_rate=0.95)
        assert chain.accepts(rec)

    def test_below_threshold_rejected(self):
        cfg = ImportFilterConfig(min_call_rate=0.99)
        chain = ImportFilterChain(cfg)
        rec = _make_record(call_rate=0.95)
        assert not chain.accepts(rec)


class TestMafFilter:
    def test_within_range_passes(self):
        cfg = ImportFilterConfig(maf_min=0.01, maf_max=0.49)
        chain = ImportFilterChain(cfg)
        rec = _make_record(af_total=0.1)
        assert chain.accepts(rec)

    def test_below_min_rejected(self):
        cfg = ImportFilterConfig(maf_min=0.05)
        chain = ImportFilterChain(cfg)
        rec = _make_record(af_total=0.01)
        assert not chain.accepts(rec)

    def test_above_max_rejected(self):
        cfg = ImportFilterConfig(maf_max=0.05)
        chain = ImportFilterChain(cfg)
        rec = _make_record(af_total=0.1)
        assert not chain.accepts(rec)

    def test_maf_uses_minor_allele(self):
        """af_total=0.9 → MAF=0.1, should pass maf_min=0.05."""
        cfg = ImportFilterConfig(maf_min=0.05)
        chain = ImportFilterChain(cfg)
        rec = _make_record(af_total=0.9)
        assert chain.accepts(rec)

    def test_monomorphic_rejected_by_maf_min(self):
        cfg = ImportFilterConfig(maf_min=0.01)
        chain = ImportFilterChain(cfg)
        rec = _make_record(af_total=0.0)
        assert not chain.accepts(rec)


class TestVariantTypeFilter:
    def test_snp_only(self):
        cfg = ImportFilterConfig(variant_types={"SNP"})
        chain = ImportFilterChain(cfg)
        assert chain.accepts(_make_record(variant_type="SNP"))
        assert not chain.accepts(_make_record(variant_type="INDEL"))

    def test_snp_and_indel(self):
        cfg = ImportFilterConfig(variant_types={"SNP", "INDEL"})
        chain = ImportFilterChain(cfg)
        assert chain.accepts(_make_record(variant_type="SNP"))
        assert chain.accepts(_make_record(variant_type="INDEL"))
        assert not chain.accepts(_make_record(variant_type="SV"))

    def test_no_filter_passes_all(self):
        cfg = ImportFilterConfig()
        chain = ImportFilterChain(cfg)
        assert chain.accepts(_make_record(variant_type="SV"))


# ---------------------------------------------------------------------------
# Chain composition
# ---------------------------------------------------------------------------


class TestChainComposition:
    def test_multiple_filters_all_pass(self):
        cfg = ImportFilterConfig(min_qual=20.0, maf_min=0.01, variant_types={"SNP"})
        chain = ImportFilterChain(cfg)
        rec = _make_record(qual=30.0, af_total=0.1, variant_type="SNP")
        assert chain.accepts(rec)

    def test_first_filter_rejects(self):
        cfg = ImportFilterConfig(min_qual=50.0, maf_min=0.01)
        chain = ImportFilterChain(cfg)
        rec = _make_record(qual=30.0, af_total=0.1)
        assert not chain.accepts(rec)

    def test_summary_counts(self):
        cfg = ImportFilterConfig(min_qual=25.0, variant_types={"SNP"})
        chain = ImportFilterChain(cfg)

        chain.accepts(_make_record(qual=30.0, variant_type="SNP"))  # pass
        chain.accepts(_make_record(qual=10.0, variant_type="SNP"))  # rejected_qual
        chain.accepts(_make_record(qual=30.0, variant_type="INDEL"))  # rejected_variant_type

        s = chain.summary()
        assert s["total"] == 3
        assert s["passed"] == 1
        assert s["rejected_qual"] == 1
        assert s["rejected_variant_type"] == 1


# ---------------------------------------------------------------------------
# Filter iterator
# ---------------------------------------------------------------------------


class TestFilterIterator:
    def test_yields_only_passing(self):
        cfg = ImportFilterConfig(min_qual=25.0)
        chain = ImportFilterChain(cfg)

        records = [
            _make_record(qual=30.0),
            _make_record(qual=10.0),
            _make_record(qual=50.0),
        ]
        result = list(chain.filter(iter(records)))
        assert len(result) == 2
        assert all(r.qual >= 25.0 for r in result)

    def test_empty_input(self):
        cfg = ImportFilterConfig(min_qual=25.0)
        chain = ImportFilterChain(cfg)
        result = list(chain.filter(iter([])))
        assert result == []
