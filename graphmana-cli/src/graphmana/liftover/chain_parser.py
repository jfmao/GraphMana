"""Liftover coordinate conversion via pyliftover.

Thin wrapper around the ``pyliftover`` library that converts VCF-style
1-based variant coordinates between reference genome assemblies using UCSC
chain files.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from pyliftover import LiftOver

logger = logging.getLogger(__name__)

_COMPLEMENT = {"A": "T", "T": "A", "C": "G", "G": "C", "a": "t", "t": "a", "c": "g", "g": "c"}


def complement(base: str) -> str:
    """Return the complement of a single nucleotide base.

    Args:
        base: A single character (A, T, C, or G, case-insensitive).

    Returns:
        Complementary base preserving case.

    Raises:
        KeyError: If base is not a standard nucleotide.
    """
    return _COMPLEMENT[base]


def reverse_complement(seq: str) -> str:
    """Return the reverse complement of a nucleotide sequence.

    Args:
        seq: Nucleotide sequence (e.g. "ATG").

    Returns:
        Reverse-complemented sequence (e.g. "CAT").
    """
    if not seq:
        return seq
    return "".join(_COMPLEMENT[b] for b in reversed(seq))


@dataclass(frozen=True)
class LiftoverResult:
    """Successful coordinate conversion result."""

    new_chr: str
    new_pos: int
    strand: str
    score: float
    new_ref: str
    new_alt: str
    new_variant_id: str


@dataclass(frozen=True)
class UnmappedVariant:
    """Variant that could not be mapped to the target assembly."""

    variant_id: str
    chr: str
    pos: int
    ref: str
    alt: str
    reason: str  # "unmapped", "ambiguous", or "collision"


class LiftoverConverter:
    """Convert variant coordinates between assemblies using a UCSC chain file.

    Args:
        chain_path: Path to a ``.chain`` or ``.chain.gz`` file.

    Raises:
        FileNotFoundError: If the chain file does not exist.
    """

    def __init__(self, chain_path: str | Path) -> None:
        chain_path = Path(chain_path)
        if not chain_path.exists():
            raise FileNotFoundError(f"Chain file not found: {chain_path}")
        self._lo = LiftOver(str(chain_path))
        self._chain_path = chain_path

    def convert(
        self,
        variant_id: str,
        chr: str,
        pos: int,
        ref: str,
        alt: str,
    ) -> LiftoverResult | UnmappedVariant:
        """Convert a single variant's coordinates.

        pyliftover uses 0-based coordinates; VCF uses 1-based.  We convert
        pos-1 on input and add 1 on output.

        Args:
            variant_id: Current variant identifier (chr-pos-ref-alt).
            chr: Chromosome name (e.g. "chr1" or "1").
            pos: 1-based VCF position.
            ref: Reference allele.
            alt: Alternate allele.

        Returns:
            ``LiftoverResult`` on success, ``UnmappedVariant`` on failure.
        """
        # pyliftover expects 0-based coordinates
        results = self._lo.convert_coordinate(chr, pos - 1)

        if results is None or len(results) == 0:
            return UnmappedVariant(variant_id, chr, pos, ref, alt, "unmapped")

        if len(results) > 1:
            return UnmappedVariant(variant_id, chr, pos, ref, alt, "ambiguous")

        new_chr, new_pos_0based, new_strand, score = results[0]
        new_pos = int(new_pos_0based) + 1  # back to 1-based

        # Handle strand flips
        new_ref = ref
        new_alt = alt
        if new_strand == "-":
            new_ref = reverse_complement(ref)
            new_alt = reverse_complement(alt)

        new_variant_id = f"{new_chr}-{new_pos}-{new_ref}-{new_alt}"

        return LiftoverResult(
            new_chr=new_chr,
            new_pos=new_pos,
            strand=new_strand,
            score=score,
            new_ref=new_ref,
            new_alt=new_alt,
            new_variant_id=new_variant_id,
        )
