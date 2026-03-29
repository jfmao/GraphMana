"""GraphMana export pipeline."""

__all__ = [
    "BaseExporter",
    "BeagleExporter",
    "BEDExporter",
    "BGENExporter",
    "EIGENSTRATExporter",
    "GDSExporter",
    "GenepopExporter",
    "HAPExporter",
    "JSONExporter",
    "PLINKExporter",
    "PLINK2Exporter",
    "SFSDadiExporter",
    "SFSFscExporter",
    "STRUCTUREExporter",
    "TSVExporter",
    "TreeMixExporter",
    "VCFExporter",
    "ZarrExporter",
]


def __getattr__(name):  # noqa: F401
    if name == "BaseExporter":
        from graphmana.export.base import BaseExporter

        return BaseExporter
    if name == "VCFExporter":
        from graphmana.export.vcf_export import VCFExporter

        return VCFExporter
    if name == "PLINKExporter":
        from graphmana.export.plink_export import PLINKExporter

        return PLINKExporter
    if name == "PLINK2Exporter":
        from graphmana.export.plink2_export import PLINK2Exporter

        return PLINK2Exporter
    if name == "TSVExporter":
        from graphmana.export.tsv_export import TSVExporter

        return TSVExporter
    if name == "BEDExporter":
        from graphmana.export.bed_export import BEDExporter

        return BEDExporter
    if name == "TreeMixExporter":
        from graphmana.export.treemix_export import TreeMixExporter

        return TreeMixExporter
    if name == "EIGENSTRATExporter":
        from graphmana.export.eigenstrat_export import EIGENSTRATExporter

        return EIGENSTRATExporter
    if name == "SFSDadiExporter":
        from graphmana.export.sfs_dadi_export import SFSDadiExporter

        return SFSDadiExporter
    if name == "SFSFscExporter":
        from graphmana.export.sfs_fsc_export import SFSFscExporter

        return SFSFscExporter
    if name == "BeagleExporter":
        from graphmana.export.beagle_export import BeagleExporter

        return BeagleExporter
    if name == "STRUCTUREExporter":
        from graphmana.export.structure_export import STRUCTUREExporter

        return STRUCTUREExporter
    if name == "GenepopExporter":
        from graphmana.export.genepop_export import GenepopExporter

        return GenepopExporter
    if name == "HAPExporter":
        from graphmana.export.hap_export import HAPExporter

        return HAPExporter
    if name == "JSONExporter":
        from graphmana.export.json_export import JSONExporter

        return JSONExporter
    if name == "ZarrExporter":
        from graphmana.export.zarr_export import ZarrExporter

        return ZarrExporter
    if name == "GDSExporter":
        from graphmana.export.gds_export import GDSExporter

        return GDSExporter
    if name == "BGENExporter":
        from graphmana.export.bgen_export import BGENExporter

        return BGENExporter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
