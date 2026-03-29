"""VCF format exporter (FULL PATH).

Exports variant data with per-sample genotypes in VCF 4.3 format.
Proves roundtrip fidelity: ingest VCF -> export VCF -> diff.
"""

from __future__ import annotations

import gzip
import io
import logging
import struct
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter

logger = logging.getLogger(__name__)

# VCF genotype strings for diploid calls (unphased / phased)
_GT_UNPHASED = ["0/0", "0/1", "1/1", "./."]
_GT_PHASED = ["0|0", "0|1", "1|1", ".|."]
# Haploid
_GT_HAPLOID = ["0", "1", "1", "."]


def format_gt(gt: int, phase: int, haploid: bool, phased: bool) -> str:
    """Convert packed genotype code to VCF GT string.

    Args:
        gt: Packed code (0=HomRef, 1=Het, 2=HomAlt, 3=Missing).
        phase: Phase bit (1=ALT on second haplotype for het).
        haploid: Whether this sample is haploid at this locus.
        phased: Whether to output phased separator (|).

    Returns:
        VCF GT string (e.g. "0/1", "0|1", "1|0", "0", ".").
    """
    if haploid:
        return _GT_HAPLOID[gt]

    if not phased:
        return _GT_UNPHASED[gt]

    # Phased output
    if gt == 1:  # Het
        # phase=1 means ALT on second haplotype → 0|1
        # phase=0 means ALT on first haplotype → 1|0
        return "0|1" if phase else "1|0"
    return _GT_PHASED[gt]


def recalculate_af_from_genotypes(gt_codes: np.ndarray, ploidy_flags: np.ndarray) -> dict:
    """Compute AC, AN, AF, and call_rate from subset genotypes.

    Args:
        gt_codes: int8 array (0=HomRef, 1=Het, 2=HomAlt, 3=Missing).
        ploidy_flags: uint8 array (1=haploid).

    Returns:
        Dict with ac_total, an_total, af_total, call_rate.
    """
    non_missing = gt_codes != 3
    diploid_alleles = np.where(ploidy_flags == 0, 2, 1)
    an = int(np.sum(diploid_alleles[non_missing]))
    ac = int(
        np.sum(np.where(gt_codes[non_missing] == 1, 1, 0))
        + np.sum(np.where(gt_codes[non_missing] == 2, diploid_alleles[non_missing], 0))
    )
    af = ac / an if an > 0 else 0.0
    n_total = len(gt_codes)
    call_rate = int(np.sum(non_missing)) / n_total if n_total > 0 else 0.0
    return {"ac_total": ac, "an_total": an, "af_total": af, "call_rate": call_rate}


def format_variant_line(
    props: dict,
    gt_strings: list[str],
) -> str:
    """Format a single VCF data line.

    Args:
        props: Variant node properties.
        gt_strings: List of GT strings, one per sample.

    Returns:
        Tab-separated VCF line (no trailing newline).
    """
    chrom = props.get("chr", ".")
    pos = props.get("pos", 0)
    vid = props.get("variantId", ".")
    ref = props.get("ref", ".")
    alt = props.get("alt", ".")
    qual = props.get("qual")
    qual_str = str(qual) if qual is not None else "."
    filt = props.get("filter")
    filt_str = filt if filt else "."

    # INFO field
    info_parts = []
    ac = props.get("ac_total")
    if ac is not None:
        info_parts.append(f"AC={ac}")
    an = props.get("an_total")
    if an is not None:
        info_parts.append(f"AN={an}")
    af = props.get("af_total")
    if af is not None:
        info_parts.append(f"AF={af:.6g}")
    vtype = props.get("variant_type")
    if vtype:
        info_parts.append(f"VT={vtype}")
    info_str = ";".join(info_parts) if info_parts else "."

    fields = [chrom, str(pos), vid, ref, alt, qual_str, filt_str, info_str, "GT"]
    fields.extend(gt_strings)
    return "\t".join(fields)


def reconstruct_multiallelic_gt(
    allele_gt_arrays: list[np.ndarray],
    allele_phase_arrays: list[np.ndarray],
    allele_ploidy_arrays: list[np.ndarray],
    phased: bool,
) -> list[str]:
    """Reconstruct multi-allelic GT strings from K biallelic GT arrays.

    Args:
        allele_gt_arrays: K arrays of packed GT codes (0=HomRef, 1=Het, 2=HomAlt, 3=Missing).
        allele_phase_arrays: K arrays of phase bits.
        allele_ploidy_arrays: K arrays of ploidy flags (1=haploid).
        phased: Whether to output phased separator.

    Returns:
        List of VCF GT strings, one per sample.
    """
    n_samples = len(allele_gt_arrays[0])
    k = len(allele_gt_arrays)
    sep = "|" if phased else "/"
    result = []

    for i in range(n_samples):
        gts = [int(allele_gt_arrays[a][i]) for a in range(k)]
        phases = [int(allele_phase_arrays[a][i]) for a in range(k)]
        haploid = bool(allele_ploidy_arrays[0][i])

        # Missing: any allele has code 3
        if any(g == 3 for g in gts):
            if haploid:
                result.append(".")
            else:
                result.append(f".{sep}.")
            continue

        if haploid:
            # Find first HomAlt or Het allele
            allele_num = 0
            for a in range(k):
                if gts[a] == 2 or gts[a] == 1:  # HomAlt or Het
                    allele_num = a + 1
                    break
            result.append(str(allele_num))
            continue

        # Diploid
        hom_alt_alleles = [a for a in range(k) if gts[a] == 2]
        het_alleles = [a for a in range(k) if gts[a] == 1]

        if hom_alt_alleles:
            # HomAlt for one allele: allele/allele
            a = hom_alt_alleles[0] + 1
            result.append(f"{a}{sep}{a}")
        elif len(het_alleles) >= 2:
            # Het for two alleles: min/max (or phased order)
            a1, a2 = het_alleles[0] + 1, het_alleles[1] + 1
            if phased:
                # Use phase bits to determine haplotype assignment
                # For allele a1: phase=0 → ALT on first hap, phase=1 → ALT on second
                # For allele a2: same convention
                # In a multi-allelic het like 1/2, one ALT is on hap1 and other on hap2
                hap1, hap2 = 0, 0
                for a_idx in het_alleles:
                    if phases[a_idx] == 0:
                        hap1 = a_idx + 1  # ALT on first haplotype
                    else:
                        hap2 = a_idx + 1  # ALT on second haplotype
                # Fallback if both have same phase bit
                if hap1 == 0:
                    hap1 = a1
                if hap2 == 0:
                    hap2 = a2
                result.append(f"{hap1}|{hap2}")
            else:
                result.append(f"{min(a1, a2)}/{max(a1, a2)}")
        elif len(het_alleles) == 1:
            # Het for one allele: 0/allele
            a = het_alleles[0] + 1
            if phased:
                if phases[het_alleles[0]] == 0:
                    result.append(f"{a}|0")
                else:
                    result.append(f"0|{a}")
            else:
                result.append(f"0/{a}")
        else:
            # All HomRef
            result.append(f"0{sep}0")

    return result


def format_multiallelic_variant_line(
    allele_props_list: list[dict],
    gt_strings: list[str],
) -> str:
    """Format a merged multi-allelic VCF data line.

    Args:
        allele_props_list: List of K Variant property dicts (one per allele).
        gt_strings: Merged GT strings, one per sample.

    Returns:
        Tab-separated VCF line (no trailing newline).
    """
    first = allele_props_list[0]
    chrom = first.get("chr", ".")
    pos = first.get("pos", 0)
    vid = first.get("variantId", ".")
    ref = first.get("ref", ".")
    alts = ",".join(p.get("alt", ".") for p in allele_props_list)

    qual = first.get("qual")
    qual_str = str(qual) if qual is not None else "."
    filt = first.get("filter")
    filt_str = filt if filt else "."

    # INFO: per-allele AC and AF, single AN
    info_parts = []
    ac_values = [str(p.get("ac_total", 0)) for p in allele_props_list]
    info_parts.append(f"AC={','.join(ac_values)}")
    an = first.get("an_total")
    if an is not None:
        info_parts.append(f"AN={an}")
    af_values = [f"{p.get('af_total', 0.0):.6g}" for p in allele_props_list]
    info_parts.append(f"AF={','.join(af_values)}")
    vtype = first.get("variant_type")
    if vtype:
        info_parts.append(f"VT={vtype}")
    info_str = ";".join(info_parts)

    fields = [chrom, str(pos), vid, ref, alts, qual_str, filt_str, info_str, "GT"]
    fields.extend(gt_strings)
    return "\t".join(fields)


def _resolve_output_type(output: Path, output_type: str | None) -> str:
    """Resolve output type from explicit flag or file extension.

    Args:
        output: Output file path.
        output_type: Explicit type ("v", "z", "b") or None for auto-detect.

    Returns:
        "v" for plain VCF, "z" for BGZF-compressed VCF, "b" for BCF.
    """
    if output_type is not None:
        return output_type
    name = str(output).lower()
    if name.endswith(".vcf.gz") or name.endswith(".gz"):
        return "z"
    if name.endswith(".bcf"):
        return "b"
    return "v"


# BGZF EOF marker — a valid empty BGZF block that signals end-of-file.
# Required by htslib/samtools/bcftools for proper BGZF files.
_BGZF_EOF = (
    b"\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff"
    b"\x06\x00\x42\x43\x02\x00\x1b\x00"
    b"\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00"
)


class BGZFWriter:
    """Write BGZF (blocked gzip) format compatible with htslib/bcftools.

    BGZF is a series of gzip blocks, each with an extra field (SI1=66, SI2=67)
    containing the block size. Each block is at most 64 KiB compressed.
    """

    _MAX_RAW = 65280  # Max uncompressed bytes per block (< 64 KiB)

    def __init__(self, fileobj):
        self._fileobj = fileobj
        self._buffer = bytearray()

    def write(self, data: str) -> None:
        """Buffer text data and flush complete blocks."""
        self._buffer.extend(data.encode("utf-8"))
        while len(self._buffer) >= self._MAX_RAW:
            self._flush_block(self._buffer[: self._MAX_RAW])
            self._buffer = self._buffer[self._MAX_RAW :]

    def _flush_block(self, raw: bytes) -> None:
        """Write one BGZF block."""
        # Compress the raw data
        deflated = gzip.compress(raw, compresslevel=6)
        # gzip.compress produces a full gzip member; we need to rebuild it
        # with the BGZF extra field. Instead, use raw deflate + manual framing.
        import zlib

        deflated_data = zlib.compress(raw, 6)
        # Strip zlib header (2 bytes) and checksum (4 bytes) to get raw deflate
        deflate_raw = deflated_data[2:-4]

        crc = zlib.crc32(raw) & 0xFFFFFFFF
        raw_size = len(raw)

        # BGZF block = gzip header with extra field + deflate data + crc32 + isize
        # Block size (BSIZE) = total block size - 1
        extra = struct.pack("<BBHBBH", 66, 67, 2, 0, 0, 0)  # placeholder
        header = (
            b"\x1f\x8b"  # gzip magic
            b"\x08"  # CM = deflate
            b"\x04"  # FLG = FEXTRA
            b"\x00\x00\x00\x00"  # MTIME
            b"\x00"  # XFL
            b"\xff"  # OS = unknown
        )
        # Extra field: XLEN(2) + SI1(1) + SI2(1) + SLEN(2) + BSIZE(2)
        xlen = 6
        bsize = len(header) + 2 + xlen + len(deflate_raw) + 8 - 1  # -1 per spec
        extra_field = struct.pack("<HBBHH", xlen, 66, 67, 2, bsize)
        trailer = struct.pack("<II", crc, raw_size & 0xFFFFFFFF)

        self._fileobj.write(header)
        self._fileobj.write(extra_field)
        self._fileobj.write(deflate_raw)
        self._fileobj.write(trailer)

    def close(self) -> None:
        """Flush remaining data and write EOF marker."""
        if self._buffer:
            self._flush_block(bytes(self._buffer))
            self._buffer = bytearray()
        self._fileobj.write(_BGZF_EOF)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class VCFExporter(BaseExporter):
    """Export to VCF 4.3 format."""

    def export(
        self,
        output: Path,
        *,
        phased: bool = False,
        vcf_version: str = "4.3",
        output_type: str | None = None,
        reconstruct_multiallelic: bool = True,
    ) -> dict:
        """Export VCF format.

        Args:
            output: Output file path.
            phased: If True, output phased genotypes (|).
            vcf_version: VCF format version (e.g. "4.3").
            output_type: Output type: "v" (VCF), "z" (gzipped), "b" (BCF).
                Defaults to auto-detect from output file extension.
            reconstruct_multiallelic: If True, merge split variants that share
                the same multiallelic_site back into multi-allelic VCF lines.

        Returns:
            Summary dict with n_variants, n_samples, chromosomes.
        """
        self._vcf_version = vcf_version
        output = Path(output)
        resolved_type = _resolve_output_type(output, output_type)

        if resolved_type == "b":
            raise NotImplementedError("BCF output (--output-type b) is not yet supported.")

        if self._threads > 1:
            return self._export_parallel(
                output,
                phased=phased,
                reconstruct_multiallelic=reconstruct_multiallelic,
                output_type=resolved_type,
            )

        samples = self._load_samples()
        chromosomes = self._load_chromosomes()
        target_chroms = self._get_target_chromosomes()
        vcf_header = self._load_vcf_header()

        # Build packed_indices array sorted by packed_index
        packed_indices = np.array([s["packed_index"] for s in samples], dtype=np.int64)
        sample_ids = [s["sampleId"] for s in samples]

        n_variants = 0
        output.parent.mkdir(parents=True, exist_ok=True)

        with self._open_output(output, resolved_type) as f:
            self._write_header(f, sample_ids, chromosomes, vcf_header=vcf_header)

            for chrom in target_chroms:
                # Buffer for multi-allelic reconstruction
                ma_buffer: list[tuple[dict, np.ndarray, np.ndarray, np.ndarray]] = []
                current_ma_site: str | None = None

                for props in self._iter_variants(chrom):
                    gt_codes, phase_bits, ploidy_flags = self._unpack_variant_genotypes(
                        props, packed_indices
                    )
                    if self._recalculate_af:
                        af_info = recalculate_af_from_genotypes(gt_codes, ploidy_flags)
                        props = {**props, **af_info}

                    ma_site = props.get("multiallelic_site")

                    if reconstruct_multiallelic and ma_site is not None:
                        if current_ma_site is not None and ma_site != current_ma_site:
                            # Flush previous group
                            n_variants += self._flush_ma_buffer(f, ma_buffer, sample_ids, phased)
                            ma_buffer = []
                        current_ma_site = ma_site
                        ma_buffer.append((props, gt_codes, phase_bits, ploidy_flags))
                    else:
                        # Flush any pending multi-allelic buffer
                        if ma_buffer:
                            n_variants += self._flush_ma_buffer(f, ma_buffer, sample_ids, phased)
                            ma_buffer = []
                            current_ma_site = None

                        # Write single biallelic line
                        gt_strings = [
                            format_gt(
                                int(gt_codes[i]),
                                int(phase_bits[i]),
                                bool(ploidy_flags[i]),
                                phased,
                            )
                            for i in range(len(sample_ids))
                        ]
                        f.write(format_variant_line(props, gt_strings))
                        f.write("\n")
                        n_variants += 1

                # Flush remaining buffer for this chromosome
                if ma_buffer:
                    n_variants += self._flush_ma_buffer(f, ma_buffer, sample_ids, phased)

        logger.info(
            "VCF export: %d variants, %d samples, %d chromosomes",
            n_variants,
            len(sample_ids),
            len(target_chroms),
        )
        return {
            "n_variants": n_variants,
            "n_samples": len(sample_ids),
            "chromosomes": target_chroms,
            "format": "vcf",
        }

    @staticmethod
    def _flush_ma_buffer(
        f,
        buffer: list[tuple[dict, np.ndarray, np.ndarray, np.ndarray]],
        sample_ids: list[str],
        phased: bool,
    ) -> int:
        """Flush a multi-allelic buffer, writing merged or individual lines.

        Returns:
            Number of VCF lines written.
        """
        if len(buffer) == 1:
            # Single allele remaining after filtering — write as biallelic
            props, gt_codes, phase_bits, ploidy_flags = buffer[0]
            gt_strings = [
                format_gt(int(gt_codes[i]), int(phase_bits[i]), bool(ploidy_flags[i]), phased)
                for i in range(len(sample_ids))
            ]
            f.write(format_variant_line(props, gt_strings))
            f.write("\n")
            return 1

        # Merge K alleles into one multi-allelic line
        allele_props = [b[0] for b in buffer]
        allele_gt = [b[1] for b in buffer]
        allele_phase = [b[2] for b in buffer]
        allele_ploidy = [b[3] for b in buffer]

        gt_strings = reconstruct_multiallelic_gt(allele_gt, allele_phase, allele_ploidy, phased)
        f.write(format_multiallelic_variant_line(allele_props, gt_strings))
        f.write("\n")
        return 1

    @staticmethod
    def _open_output(output: Path, output_type: str):
        """Return a context-managed writable file handle.

        Args:
            output: Output file path.
            output_type: "v" for plain text, "z" for BGZF.

        Returns:
            A context manager yielding a file-like object with .write(str).
        """
        if output_type == "z":
            return BGZFWriter(open(output, "wb"))
        return open(output, "w")

    def _export_parallel(
        self,
        output: Path,
        *,
        phased: bool = False,
        reconstruct_multiallelic: bool = True,
        output_type: str = "v",
    ) -> dict:
        """Parallel VCF export: header once, data lines per chromosome."""
        from graphmana.export.parallel import run_export_parallel

        samples = self._load_samples()
        chromosomes = self._load_chromosomes()
        target_chroms = self._get_target_chromosomes()
        vcf_header = self._load_vcf_header()
        sample_ids = [s["sampleId"] for s in samples]

        def write_header(out_path, conn):
            out_path = Path(out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with self._open_output(out_path, output_type) as f:
                self._write_header(f, sample_ids, chromosomes, vcf_header=vcf_header)

        def bgzf_merge(final_output, chr_files):
            """Merge per-chromosome plain text files into a BGZF output."""
            with BGZFWriter(open(final_output, "ab")) as writer:
                for _chrom, tmp_path in chr_files:
                    if not tmp_path.exists():
                        continue
                    with open(tmp_path, "r") as in_f:
                        for i, line in enumerate(in_f):
                            if i == 0:
                                continue  # Skip worker header
                            writer.write(line)

        summary = run_export_parallel(
            VCFExporter,
            self._conn,
            threads=self._threads,
            output=Path(output),
            filter_config=self._filter_config,
            target_chroms=target_chroms,
            export_kwargs={
                "phased": phased,
                "reconstruct_multiallelic": reconstruct_multiallelic,
            },
            header_writer=write_header,
            merge_func=bgzf_merge if output_type == "z" else None,
            recalculate_af=self._recalculate_af,
        )
        summary["n_samples"] = len(sample_ids)
        summary["format"] = "vcf"
        return summary

    def _load_vcf_header(self) -> dict | None:
        """Fetch VCFHeader node from Neo4j. Returns None if absent."""
        from graphmana.db.queries import FETCH_VCF_HEADER

        try:
            result = self._conn.execute_read(FETCH_VCF_HEADER)
            record = result.single()
            if record:
                return dict(record["h"])
        except Exception:
            logger.debug("No VCFHeader node found (pre-v0.5 database or empty)")
        return None

    def _write_header(
        self,
        f,
        sample_ids: list[str],
        chromosomes: list[dict],
        *,
        vcf_header: dict | None = None,
    ) -> None:
        """Write VCF header lines.

        If a VCFHeader node is available, use the original ## meta-info lines
        and add GraphMana provenance. Otherwise fall back to the default
        hardcoded header.
        """
        if vcf_header is not None:
            self._write_preserved_header(f, sample_ids, vcf_header)
        else:
            self._write_default_header(f, sample_ids, chromosomes)

    def _write_default_header(self, f, sample_ids: list[str], chromosomes: list[dict]) -> None:
        """Write hardcoded VCF header (backward compatible with pre-v0.5 databases)."""
        version = getattr(self, "_vcf_version", "4.3")
        f.write(f"##fileformat=VCFv{version}\n")
        f.write("##source=GraphMana\n")
        for chrom in chromosomes:
            length = chrom.get("length")
            if length:
                f.write(f'##contig=<ID={chrom["chr"]},length={length}>\n')
            else:
                f.write(f'##contig=<ID={chrom["chr"]}>\n')
        f.write("##INFO=<ID=AC,Number=A,Type=Integer," 'Description="Allele count in genotypes">\n')
        f.write(
            "##INFO=<ID=AN,Number=1,Type=Integer,"
            'Description="Total number of alleles in called genotypes">\n'
        )
        f.write("##INFO=<ID=AF,Number=A,Type=Float," 'Description="Allele frequency">\n')
        f.write("##INFO=<ID=VT,Number=1,Type=String," 'Description="Variant type">\n')
        f.write("##FORMAT=<ID=GT,Number=1,Type=String," 'Description="Genotype">\n')
        self._write_chrom_line(f, sample_ids)

    def _write_preserved_header(self, f, sample_ids: list[str], vcf_header: dict) -> None:
        """Write original VCF ## meta-info lines with GraphMana provenance."""
        header_text = vcf_header.get("header_text", "")
        # Unescape newlines stored in Neo4j
        header_text = header_text.replace("\\n", "\n").replace("\\\\", "\\")

        for line in header_text.split("\n"):
            line = line.strip()
            if line:
                f.write(line + "\n")

        # Add GraphMana provenance
        f.write("##source=GraphMana-export\n")
        self._write_chrom_line(f, sample_ids)

    @staticmethod
    def _write_chrom_line(f, sample_ids: list[str]) -> None:
        """Write the #CHROM header line with current sample set."""
        header_cols = [
            "#CHROM",
            "POS",
            "ID",
            "REF",
            "ALT",
            "QUAL",
            "FILTER",
            "INFO",
            "FORMAT",
        ]
        header_cols.extend(sample_ids)
        f.write("\t".join(header_cols) + "\n")
