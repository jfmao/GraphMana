"""Reference allele verification against a FASTA genome.

Streams variants from Neo4j and compares their stored REF allele against
the reference genome sequence at the corresponding position.
"""

from __future__ import annotations

import logging
from pathlib import Path

from graphmana.db.connection import GraphManaConnection

logger = logging.getLogger(__name__)


def load_fasta_index(fasta_path: Path) -> dict[str, tuple[int, int, int, int]]:
    """Load a .fai index for a FASTA file.

    Returns dict mapping sequence name -> (length, offset, line_bases, line_width).
    If no .fai exists, returns empty dict (will fall back to full load).
    """
    fai_path = Path(str(fasta_path) + ".fai")
    if not fai_path.exists():
        return {}
    index = {}
    with open(fai_path) as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) >= 5:
                name = parts[0]
                length = int(parts[1])
                offset = int(parts[2])
                line_bases = int(parts[3])
                line_width = int(parts[4])
                index[name] = (length, offset, line_bases, line_width)
    return index


def fetch_ref_base_indexed(
    fasta_path: Path,
    fai: dict,
    chrom: str,
    pos: int,
    length: int = 1,
) -> str | None:
    """Fetch reference sequence using .fai index (0-based internally, 1-based pos input)."""
    if chrom not in fai:
        return None
    seq_len, offset, line_bases, line_width = fai[chrom]
    zero_pos = pos - 1
    if zero_pos < 0 or zero_pos >= seq_len:
        return None

    with open(fasta_path, "rb") as f:
        bases = []
        for i in range(length):
            p = zero_pos + i
            if p >= seq_len:
                break
            line_num = p // line_bases
            col = p % line_bases
            file_offset = offset + line_num * line_width + col
            f.seek(file_offset)
            bases.append(f.read(1).decode("ascii").upper())
    return "".join(bases)


def load_fasta_sequence(fasta_path: Path, chrom: str) -> str | None:
    """Load entire chromosome sequence from FASTA (fallback when no .fai)."""
    lines = []
    found = False
    with open(fasta_path) as f:
        for line in f:
            if line.startswith(">"):
                if found:
                    break  # Next chromosome, stop
                header_name = line[1:].split()[0]
                if header_name == chrom:
                    found = True
                continue
            if found:
                lines.append(line.strip().upper())
    return "".join(lines) if found else None


def check_ref_alleles(
    conn: GraphManaConnection,
    fasta_path: Path,
    *,
    chromosomes: list[str] | None = None,
    max_mismatches: int = 0,
) -> dict:
    """Compare stored REF alleles against a FASTA reference genome.

    Args:
        conn: Live Neo4j connection.
        fasta_path: Path to reference FASTA (optionally with .fai index).
        chromosomes: Limit check to these chromosomes. None = all.
        max_mismatches: Stop early after this many mismatches (0 = report all).

    Returns:
        Dict with n_checked, n_matched, n_mismatched, mismatches (list of dicts).
    """
    fai = load_fasta_index(fasta_path)
    use_index = len(fai) > 0

    # Get chromosomes to check
    if chromosomes:
        target_chroms = chromosomes
    else:
        result = conn.execute_read(
            "MATCH (c:Chromosome) RETURN c.chromosomeId AS chr ORDER BY c.chromosomeId"
        )
        target_chroms = [r["chr"] for r in result]

    n_checked = 0
    n_matched = 0
    mismatches = []
    chrom_cache: str | None = None
    chrom_seq: str | None = None

    for chrom in target_chroms:
        logger.info("Checking chromosome %s", chrom)

        # Load sequence for this chromosome (if not using index)
        if not use_index:
            if chrom != chrom_cache:
                chrom_seq = load_fasta_sequence(fasta_path, chrom)
                chrom_cache = chrom
            if chrom_seq is None:
                logger.warning("Chromosome %s not found in FASTA, skipping", chrom)
                continue

        # Stream variants
        result = conn.execute_read(
            "MATCH (v:Variant {chr: $chr}) "
            "RETURN v.variantId AS vid, v.pos AS pos, v.ref AS ref "
            "ORDER BY v.pos",
            {"chr": chrom},
        )

        for record in result:
            vid = record["vid"]
            pos = record["pos"]
            stored_ref = record["ref"]
            if stored_ref is None:
                continue

            ref_len = len(stored_ref)

            if use_index:
                genome_ref = fetch_ref_base_indexed(fasta_path, fai, chrom, pos, ref_len)
            else:
                zero_pos = pos - 1
                genome_ref = chrom_seq[zero_pos : zero_pos + ref_len] if chrom_seq else None

            n_checked += 1
            if genome_ref is None:
                mismatches.append({
                    "variantId": vid,
                    "chr": chrom,
                    "pos": pos,
                    "stored_ref": stored_ref,
                    "genome_ref": "N/A (position out of range)",
                })
            elif genome_ref.upper() != stored_ref.upper():
                mismatches.append({
                    "variantId": vid,
                    "chr": chrom,
                    "pos": pos,
                    "stored_ref": stored_ref,
                    "genome_ref": genome_ref,
                })
            else:
                n_matched += 1

            if max_mismatches > 0 and len(mismatches) >= max_mismatches:
                logger.info("Reached max_mismatches=%d, stopping early", max_mismatches)
                return {
                    "n_checked": n_checked,
                    "n_matched": n_matched,
                    "n_mismatched": len(mismatches),
                    "mismatches": mismatches,
                    "stopped_early": True,
                }

    return {
        "n_checked": n_checked,
        "n_matched": n_matched,
        "n_mismatched": len(mismatches),
        "mismatches": mismatches,
        "stopped_early": False,
    }
