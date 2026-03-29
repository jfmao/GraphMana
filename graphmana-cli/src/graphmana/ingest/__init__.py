"""GraphMana ingest pipeline — VCF parsing, CSV generation, Neo4j loading,
and incremental import."""

from graphmana.ingest.array_ops import (
    extend_gt_packed,
    extend_phase_packed,
    extend_ploidy_packed,
    merge_pop_stats,
    pad_gt_for_new_variant,
    pad_phase_for_new_variant,
)
from graphmana.ingest.chr_reconciler import ChrReconciler
from graphmana.ingest.csv_emitter import CSVEmitter
from graphmana.ingest.genotype_packer import (
    GT_REMAP,
    build_ploidy_packed,
    pack_phase,
    unpack_genotypes,
    unpack_phase,
    unpack_ploidy,
    vectorized_gt_pack,
)
from graphmana.ingest.incremental import IncrementalIngester
from graphmana.ingest.loader import run_load_csv
from graphmana.ingest.ploidy_detector import detect_ploidy
from graphmana.ingest.population_map import PopulationMap, build_pop_map, load_panel
from graphmana.ingest.vcf_parser import VariantRecord, VCFParser, classify_variant
from graphmana.ingest.vep_parser import VEPParser

__all__ = [
    "ChrReconciler",
    "CSVEmitter",
    "GT_REMAP",
    "IncrementalIngester",
    "PopulationMap",
    "VCFParser",
    "VEPParser",
    "VariantRecord",
    "build_ploidy_packed",
    "build_pop_map",
    "classify_variant",
    "detect_ploidy",
    "extend_gt_packed",
    "extend_phase_packed",
    "extend_ploidy_packed",
    "load_panel",
    "merge_pop_stats",
    "pack_phase",
    "pad_gt_for_new_variant",
    "pad_phase_for_new_variant",
    "run_load_csv",
    "unpack_genotypes",
    "unpack_phase",
    "unpack_ploidy",
    "vectorized_gt_pack",
]


def run_prepare_csv(*args, **kwargs):
    """Lazy import wrapper to avoid circular imports."""
    from graphmana.ingest.pipeline import run_prepare_csv as _run

    return _run(*args, **kwargs)


def run_ingest(*args, **kwargs):
    """Lazy import wrapper to avoid circular imports."""
    from graphmana.ingest.pipeline import run_ingest as _run

    return _run(*args, **kwargs)


def run_incremental(*args, **kwargs):
    """Lazy import wrapper to avoid circular imports."""
    from graphmana.ingest.pipeline import run_incremental as _run

    return _run(*args, **kwargs)
