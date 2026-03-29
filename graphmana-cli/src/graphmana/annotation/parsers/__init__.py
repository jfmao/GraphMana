"""Annotation parsers for various data sources."""

from graphmana.annotation.parsers.base import BaseAnnotationParser
from graphmana.annotation.parsers.bed_region import BEDRegionParser
from graphmana.annotation.parsers.cadd import CADDParser
from graphmana.annotation.parsers.clinvar import ClinVarParser
from graphmana.annotation.parsers.constraint import GeneConstraintParser
from graphmana.annotation.parsers.go_pathway import GOParser, PathwayParser

__all__ = [
    "BaseAnnotationParser",
    "BEDRegionParser",
    "CADDParser",
    "ClinVarParser",
    "GeneConstraintParser",
    "GOParser",
    "PathwayParser",
]
