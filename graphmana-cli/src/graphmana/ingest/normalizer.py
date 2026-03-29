"""Variant normalization via bcftools norm.

Wraps bcftools norm for left-alignment, multiallelic splitting, and
indel trimming. Requires bcftools to be installed and on PATH.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class NormalizationResult:
    """Summary of a bcftools norm run."""

    input_path: str
    output_path: str
    total_records: int
    split_records: int
    realigned_records: int
    skipped_records: int


def _find_bcftools() -> str:
    """Find bcftools executable on PATH."""
    path = shutil.which("bcftools")
    if path is None:
        raise RuntimeError(
            "bcftools not found on PATH. Install bcftools "
            "(https://samtools.github.io/bcftools/) or add it to PATH."
        )
    return path


def normalize_vcf(
    input_path: str | Path,
    output_path: str | Path,
    reference_fasta: str | Path,
    *,
    left_align: bool = True,
    split_multiallelic: bool = True,
    trim: bool = True,
) -> NormalizationResult:
    """Run bcftools norm on a VCF/BCF file.

    Args:
        input_path: input VCF/BCF file path.
        output_path: output VCF/BCF file path (compressed if .gz).
        reference_fasta: reference FASTA for left-alignment.
        left_align: left-align indels (requires reference).
        split_multiallelic: split multiallelic sites into biallelic.
        trim: trim redundant bases from indels (automatic with left_align).

    Returns:
        NormalizationResult with counts.

    Raises:
        RuntimeError: if bcftools is not found or returns non-zero exit.
    """
    bcftools = _find_bcftools()
    input_path = str(input_path)
    output_path = str(output_path)

    cmd = [bcftools, "norm"]

    if left_align:
        cmd.extend(["-f", str(reference_fasta)])

    if split_multiallelic:
        cmd.extend(["-m", "-both"])

    # Note: trimming of redundant bases is automatic with -f (left-alignment).
    # bcftools norm has no standalone trim flag; the trim parameter is kept
    # for API compatibility but has no effect when left_align=False.

    # Output format
    if str(output_path).endswith(".gz"):
        cmd.extend(["-Oz"])
    elif str(output_path).endswith(".bcf"):
        cmd.extend(["-Ob"])
    else:
        cmd.extend(["-Ov"])

    cmd.extend(["-o", output_path, input_path])

    logger.info("Running normalization: %s", " ".join(cmd))

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"bcftools norm failed (exit {result.returncode}):\n" f"stderr: {result.stderr.strip()}"
        )

    # Parse bcftools norm stderr for summary statistics
    total = 0
    split = 0
    realigned = 0
    skipped = 0

    for line in result.stderr.strip().split("\n"):
        line = line.strip()
        if "total" in line.lower() and "record" in line.lower():
            parts = line.split()
            for p in parts:
                if p.isdigit():
                    total = int(p)
                    break
        elif "split" in line.lower():
            parts = line.split()
            for p in parts:
                if p.isdigit():
                    split = int(p)
                    break
        elif "realigned" in line.lower() or "left-aligned" in line.lower():
            parts = line.split()
            for p in parts:
                if p.isdigit():
                    realigned = int(p)
                    break
        elif "skipped" in line.lower():
            parts = line.split()
            for p in parts:
                if p.isdigit():
                    skipped = int(p)
                    break

    logger.info(
        "Normalization complete: %d total, %d split, %d realigned, %d skipped",
        total,
        split,
        realigned,
        skipped,
    )

    return NormalizationResult(
        input_path=input_path,
        output_path=output_path,
        total_records=total,
        split_records=split,
        realigned_records=realigned,
        skipped_records=skipped,
    )
