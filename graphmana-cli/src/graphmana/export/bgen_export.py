"""BGEN 1.2 format exporter (FULL PATH).

Exports variant data as BGEN 1.2 with Layout 2 and zlib compression.
Binary format suitable for UK Biobank-style workflows and tools like
BOLT-LMM, SAIGE, and bgenix.

For biallelic diploid genotypes, encodes 3 probabilities per sample:
P(RR), P(RA), P(AA) as uint16 values scaled to [0, 65535].
"""

from __future__ import annotations

import logging
import struct
import zlib
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)

# BGEN magic number
_BGEN_MAGIC = b"bgen"
_LAYOUT2_COMPRESSED_SNPBLOCK = 2  # Layout 2
_COMPRESSION_ZLIB = 1


class BGENExporter(BaseExporter):
    """Export variant data as BGEN 1.2 (FULL PATH)."""

    def export(self, output: Path, **kwargs) -> dict:
        """Export BGEN format.

        Args:
            output: Output BGEN file path.

        Returns:
            Summary dict with n_variants, n_samples, format.
        """
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        samples = self._load_samples()
        n_samples = len(samples)
        packed_indices = np.array(sorted(s["packed_index"] for s in samples), dtype=np.int64)
        idx_to_sample = {s["packed_index"]: s["sampleId"] for s in samples}
        sample_ids = [idx_to_sample[idx] for idx in packed_indices]

        target_chroms = self._get_target_chromosomes()

        # First pass: count variants without buffering data
        n_variants = 0
        for chrom in target_chroms:
            for _props in self._iter_variants(chrom):
                n_variants += 1

        with open(output, "wb") as f:
            self._write_header(f, n_variants, n_samples, sample_ids)

            # Second pass: stream variant blocks directly
            for chrom in target_chroms:
                for props in self._iter_variants(chrom):
                    gt_codes, phase_bits, ploidy_flags = self._unpack_variant_genotypes(
                        props, packed_indices
                    )
                    props = self._maybe_recalculate_af(props, gt_codes, ploidy_flags)
                    self._write_variant_block(f, props, gt_codes, n_samples)

        logger.info("BGEN export: %d variants, %d samples", n_variants, n_samples)
        return {
            "n_variants": n_variants,
            "n_samples": n_samples,
            "chromosomes": target_chroms,
            "format": "bgen",
        }

    def _write_header(
        self,
        f,
        n_variants: int,
        n_samples: int,
        sample_ids: list[str],
    ) -> None:
        """Write BGEN file header and sample identifier block."""
        # Calculate header length
        # Header block: offset(4) + header_size(4) + n_variants(4) + n_samples(4) +
        #               magic(4) + free_data(0) + flags(4) = 24 bytes min
        header_size = 20  # bytes for fixed header fields
        free_data = b""

        # Flags: CompressedSNPBlocks=1 (zlib), Layout=2, SampleIdentifiers=1
        flags = (_COMPRESSION_ZLIB) | (_LAYOUT2_COMPRESSED_SNPBLOCK << 2) | (1 << 31)

        # Sample identifier block
        sample_block = bytearray()
        for sid in sample_ids:
            sid_bytes = sid.encode("utf-8")
            sample_block.extend(struct.pack("<H", len(sid_bytes)))
            sample_block.extend(sid_bytes)
        sample_block_size = len(sample_block) + 8  # +8 for block_size + n_samples fields

        # Offset = header_size + sample_block_size
        offset = header_size + len(free_data) + sample_block_size

        # Write header block
        f.write(struct.pack("<I", offset))  # offset to first variant data block
        f.write(struct.pack("<I", header_size + len(free_data)))  # LH: header length
        f.write(struct.pack("<I", n_variants))  # number of variant data blocks
        f.write(struct.pack("<I", n_samples))  # number of samples
        f.write(_BGEN_MAGIC)  # magic number
        f.write(free_data)  # free data area
        f.write(struct.pack("<I", flags))  # flags

        # Write sample identifier block
        f.write(struct.pack("<I", sample_block_size))  # block size
        f.write(struct.pack("<I", n_samples))  # number of samples
        f.write(bytes(sample_block))  # sample IDs

    def _write_variant_block(
        self,
        f,
        props: dict,
        gt_codes: np.ndarray,
        n_samples: int,
    ) -> None:
        """Write a single variant data block in Layout 2 format."""
        # Variant identifying data
        # Number of alleles (K=2 for biallelic)
        n_alleles = 2

        varid = props.get("variantId", "")
        rsid = varid  # Use variantId as RSID
        chrom = props.get("chr", "")
        pos = props.get("pos", 0)
        ref = props.get("ref", "")
        alt = props.get("alt", "")

        varid_bytes = varid.encode("utf-8")
        rsid_bytes = rsid.encode("utf-8")
        chrom_bytes = chrom.encode("utf-8")
        ref_bytes = ref.encode("utf-8")
        alt_bytes = alt.encode("utf-8")

        # Variant identifying data
        f.write(struct.pack("<H", len(varid_bytes)))
        f.write(varid_bytes)
        f.write(struct.pack("<H", len(rsid_bytes)))
        f.write(rsid_bytes)
        f.write(struct.pack("<H", len(chrom_bytes)))
        f.write(chrom_bytes)
        f.write(struct.pack("<I", pos))
        f.write(struct.pack("<H", n_alleles))

        # Alleles
        f.write(struct.pack("<I", len(ref_bytes)))
        f.write(ref_bytes)
        f.write(struct.pack("<I", len(alt_bytes)))
        f.write(alt_bytes)

        # Genotype probability data (Layout 2)
        prob_data = self._encode_probabilities(gt_codes, n_samples, n_alleles)
        compressed = zlib.compress(prob_data)

        # Length of rest of data for this variant
        f.write(struct.pack("<I", len(compressed) + 4))  # total compressed block size
        f.write(struct.pack("<I", len(prob_data)))  # uncompressed size
        f.write(compressed)

    def _encode_probabilities(
        self,
        gt_codes: np.ndarray,
        n_samples: int,
        n_alleles: int,
    ) -> bytes:
        """Encode genotype probabilities as Layout 2 probability data.

        For biallelic diploid: 3 probabilities per sample (P(RR), P(RA), P(AA)).
        Only K-1=2 probabilities stored (last derived from sum=1).

        Returns:
            Bytes of the uncompressed probability data block.
        """
        buf = bytearray()

        # Probability data header
        buf.extend(struct.pack("<I", n_samples))  # N
        buf.extend(struct.pack("<H", n_alleles))  # number of alleles
        buf.extend(struct.pack("<B", 2))  # min ploidy
        buf.extend(struct.pack("<B", 2))  # max ploidy

        # Per-sample ploidy + missingness (1 byte each)
        for i in range(n_samples):
            gt = int(gt_codes[i]) if i < len(gt_codes) else 3
            is_missing = 1 if gt == 3 else 0
            ploidy_byte = 2 | (is_missing << 7)
            buf.append(ploidy_byte)

        # Phased flag
        buf.append(0)  # unphased

        # Bits per probability
        bits_per_prob = 16
        buf.append(bits_per_prob)

        # Probabilities: for each sample, store K-1 = 2 uint16 values
        # P(RR), P(RA) — P(AA) is derived as 65535 - P(RR) - P(RA)
        max_val = 65535
        for i in range(n_samples):
            gt = int(gt_codes[i]) if i < len(gt_codes) else 3
            if gt == 0:  # HomRef: P(RR)=1, P(RA)=0, P(AA)=0
                buf.extend(struct.pack("<HH", max_val, 0))
            elif gt == 1:  # Het: P(RR)=0, P(RA)=1, P(AA)=0
                buf.extend(struct.pack("<HH", 0, max_val))
            elif gt == 2:  # HomAlt: P(RR)=0, P(RA)=0, P(AA)=1
                buf.extend(struct.pack("<HH", 0, 0))
            else:  # Missing: all zero
                buf.extend(struct.pack("<HH", 0, 0))

        return bytes(buf)
