"""Emit Neo4j admin-import CSV files from parsed VCF data.

Adapted from GraphPop csv_emitter.py with GraphMana-specific enhancements:
- Enhanced SAMPLE_HEADER with source_dataset, source_file, ingestion_date
- Sort-order warning for unsorted VCF input
- Optional ImportFilterChain integration
"""

from __future__ import annotations

import csv
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, TextIO

from graphmana.ingest.population_map import PopulationMap

if TYPE_CHECKING:
    from graphmana.filtering.import_filters import ImportFilterChain
    from graphmana.ingest.vcf_parser import VariantRecord, VCFParser

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Chromosome lengths (GRCh38)
# ---------------------------------------------------------------------------

CHR_LENGTHS: dict[str, int] = {
    "chr1": 248956422,
    "chr2": 242193529,
    "chr3": 198295559,
    "chr4": 190214555,
    "chr5": 181538259,
    "chr6": 170805979,
    "chr7": 159345973,
    "chr8": 145138636,
    "chr9": 138394717,
    "chr10": 133797422,
    "chr11": 135086622,
    "chr12": 133275309,
    "chr13": 114364328,
    "chr14": 107043718,
    "chr15": 101991189,
    "chr16": 90338345,
    "chr17": 83257441,
    "chr18": 80373285,
    "chr19": 58617616,
    "chr20": 64444167,
    "chr21": 46709983,
    "chr22": 50818468,
    "chrX": 156040895,
    "chrY": 57227415,
    # Also support without 'chr' prefix
    "1": 248956422,
    "2": 242193529,
    "3": 198295559,
    "4": 190214555,
    "5": 181538259,
    "6": 170805979,
    "7": 159345973,
    "8": 145138636,
    "9": 138394717,
    "10": 133797422,
    "11": 135086622,
    "12": 133275309,
    "13": 114364328,
    "14": 107043718,
    "15": 101991189,
    "16": 90338345,
    "17": 83257441,
    "18": 80373285,
    "19": 58617616,
    "20": 64444167,
    "21": 46709983,
    "22": 50818468,
    "X": 156040895,
    "Y": 57227415,
    # Rice (IRGSP-1.0)
    "Chr1": 43270923,
    "Chr2": 35937250,
    "Chr3": 36413819,
    "Chr4": 35502694,
    "Chr5": 29958434,
    "Chr6": 31248787,
    "Chr7": 29697621,
    "Chr8": 28443022,
    "Chr9": 23012720,
    "Chr10": 23207287,
    "Chr11": 29021106,
    "Chr12": 27531856,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _harmonic(n: int) -> float:
    """Compute the n-th harmonic number: sum(1/i for i in 1..n)."""
    return sum(1.0 / i for i in range(1, n + 1))


def _harmonic2(n: int) -> float:
    """Compute the n-th generalized harmonic number of order 2: sum(1/i^2 for i in 1..n)."""
    return sum(1.0 / (i * i) for i in range(1, n + 1))


def _fmt_float(v: float) -> str:
    """Format a float, stripping trailing zeros."""
    return f"{v:.8g}"


# ---------------------------------------------------------------------------
# CSV headers (neo4j-admin import format)
# ---------------------------------------------------------------------------

VARIANT_HEADER = [
    "variantId:ID(Variant)",
    ":LABEL",
    "chr",
    "pos:long",
    "ref",
    "alt",
    "variant_type",
    "pop_ids:string[]",
    "ac:int[]",
    "an:int[]",
    "af:float[]",
    "het_count:int[]",
    "hom_alt_count:int[]",
    "ac_total:int",
    "an_total:int",
    "af_total:float",
    "call_rate:float",
    "het_exp:float[]",
    "ancestral_allele",
    "is_polarized:boolean",
    "qual:float",
    "filter",
    "gt_packed:byte[]",
    "phase_packed:byte[]",
    "ploidy_packed:byte[]",
    "called_packed:byte[]",
    "gt_encoding",
    "sv_type",
    "sv_len:long",
    "sv_end:long",
    "multiallelic_site",
    "allele_index:int",
]

SAMPLE_HEADER = [
    "sampleId:ID(Sample)",
    ":LABEL",
    "population",
    "packed_index:int",
    "sex:int",
    "source_dataset",
    "source_file",
    "ingestion_date",
]

POPULATION_HEADER = [
    "populationId:ID(Population)",
    ":LABEL",
    "name",
    "n_samples:int",
    "a_n:float",
    "a_n2:float",
]

CHROMOSOME_HEADER = ["chromosomeId:ID(Chromosome)", ":LABEL", "length:long"]

NEXT_HEADER = [":START_ID(Variant)", ":END_ID(Variant)", ":TYPE", "distance_bp:long"]

ON_CHROMOSOME_HEADER = [":START_ID(Variant)", ":END_ID(Chromosome)", ":TYPE"]

IN_POPULATION_HEADER = [":START_ID(Sample)", ":END_ID(Population)", ":TYPE"]

VCFHEADER_HEADER = [
    "dataset_id:ID(VCFHeader)",
    ":LABEL",
    "source_file",
    "header_text",
    "file_format",
    "reference",
    "caller",
    "import_date",
    "info_fields:string[]",
    "format_fields:string[]",
    "filter_fields:string[]",
    "sample_fields_stored:string[]",
]

# Regex patterns for parsing VCF meta-info lines
_RE_META_ID = re.compile(r"^##(\w+)=<ID=([^,>]+)")
_RE_FILEFORMAT = re.compile(r"^##fileformat=(.+)")
_RE_REFERENCE = re.compile(r"^##reference=(.+)")
_RE_SOURCE = re.compile(r"^##source=(.+)")


# ---------------------------------------------------------------------------
# CSVEmitter
# ---------------------------------------------------------------------------


class CSVEmitter:
    """Write node and relationship CSVs for neo4j-admin import.

    Produces the full set of CSV files needed by ``neo4j-admin database import``
    to construct the GraphMana property graph from parsed VCF data.
    """

    def __init__(
        self,
        out_dir: Path,
        pop_map: PopulationMap,
        *,
        array_delimiter: str = ";",
        contig_lengths: dict[str, int] | None = None,
        dataset_id: str = "",
        source_file: str = "",
    ) -> None:
        self._out_dir = Path(out_dir)
        self._pop_map = pop_map
        self._delim = array_delimiter
        self._contig_lengths = contig_lengths or {}
        self._dataset_id = dataset_id
        self._source_file = source_file
        self._ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        # Streaming file handles (opened in _open_streaming_files)
        self._variant_fh: TextIO | None = None
        self._next_fh: TextIO | None = None
        self._on_chrom_fh: TextIO | None = None

        self._variant_writer: csv.writer | None = None
        self._next_writer: csv.writer | None = None
        self._on_chrom_writer: csv.writer | None = None

        # NEXT-edge tracking: chr -> (prev_variant_id, prev_pos)
        self._prev_variant: dict[str, tuple[str, int]] = {}

        # Boundary tracking for within-chromosome parallel merge
        self._first_variant: dict[str, tuple[str, int]] = {}  # chr -> (id, pos)

        # Chromosomes seen (for chromosome_nodes.csv)
        self._chromosomes_seen: set[str] = set()

        # Counters
        self._n_variants = 0
        self._n_next = 0
        self._n_on_chrom = 0
        self._sort_warnings = 0

    # -- Static nodes (written before streaming) ----------------------------

    def write_static_nodes(self) -> None:
        """Write Sample, Population, and IN_POPULATION CSVs from PopulationMap."""
        self._out_dir.mkdir(parents=True, exist_ok=True)
        self._write_sample_nodes()
        self._write_population_nodes()
        self._write_in_population_edges()
        logger.info(
            "Static CSVs written: %d samples, %d populations",
            len(self._pop_map.sample_ids),
            len(self._pop_map.pop_ids),
        )

    def _write_sample_nodes(self) -> None:
        path = self._out_dir / "sample_nodes.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(SAMPLE_HEADER)
            for sid in self._pop_map.sample_ids:
                packed_idx = self._pop_map.sample_packed_index.get(sid, -1)
                sex = self._pop_map.sample_to_sex.get(sid, 0)
                w.writerow(
                    [
                        sid,
                        "Sample",
                        self._pop_map.sample_to_pop[sid],
                        packed_idx,
                        sex,
                        self._dataset_id,
                        self._source_file,
                        self._ingestion_date,
                    ]
                )

    def _write_population_nodes(self) -> None:
        path = self._out_dir / "population_nodes.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(POPULATION_HEADER)
            for pop in self._pop_map.pop_ids:
                n = self._pop_map.n_samples_per_pop[pop]
                two_n_minus_1 = 2 * n - 1
                a_n = _harmonic(two_n_minus_1) if two_n_minus_1 > 0 else 0.0
                a_n2 = _harmonic2(two_n_minus_1) if two_n_minus_1 > 0 else 0.0
                w.writerow([pop, "Population", pop, n, _fmt_float(a_n), _fmt_float(a_n2)])

    def _write_in_population_edges(self) -> None:
        path = self._out_dir / "in_population_edges.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(IN_POPULATION_HEADER)
            for sid in self._pop_map.sample_ids:
                w.writerow([sid, self._pop_map.sample_to_pop[sid], "IN_POPULATION"])

    # -- VCFHeader node -----------------------------------------------------

    def write_vcf_header_node(self, raw_header: str) -> None:
        """Write vcf_header_nodes.csv from raw VCF meta-information lines.

        Args:
            raw_header: VCF ## meta-info lines joined by newlines.
        """
        if not raw_header.strip():
            return

        file_format = ""
        reference = ""
        caller = ""
        info_ids: list[str] = []
        format_ids: list[str] = []
        filter_ids: list[str] = []

        for line in raw_header.split("\n"):
            line = line.strip()
            if not line:
                continue

            m = _RE_FILEFORMAT.match(line)
            if m:
                file_format = m.group(1)
                continue
            m = _RE_REFERENCE.match(line)
            if m:
                reference = m.group(1)
                continue
            m = _RE_SOURCE.match(line)
            if m:
                caller = m.group(1)
                continue

            m = _RE_META_ID.match(line)
            if m:
                tag, field_id = m.group(1), m.group(2)
                if tag == "INFO":
                    info_ids.append(field_id)
                elif tag == "FORMAT":
                    format_ids.append(field_id)
                elif tag == "FILTER":
                    filter_ids.append(field_id)

        # Escape newlines for CSV storage (neo4j-admin safe)
        escaped_header = raw_header.replace("\\", "\\\\").replace("\n", "\\n")

        delim = self._delim
        dataset_id = self._dataset_id or self._source_file or "unknown"

        path = self._out_dir / "vcf_header_nodes.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(VCFHEADER_HEADER)
            w.writerow(
                [
                    dataset_id,
                    "VCFHeader",
                    self._source_file,
                    escaped_header,
                    file_format,
                    reference,
                    caller,
                    self._ingestion_date,
                    delim.join(info_ids),
                    delim.join(format_ids),
                    delim.join(filter_ids),
                    delim.join(self._pop_map.sample_ids),
                ]
            )
        logger.info("VCFHeader CSV written: %s", path)

    # -- Streaming files ----------------------------------------------------

    def _open_streaming_files(self) -> None:
        """Open file handles for variant/next/on_chromosome CSVs."""
        self._variant_fh = open(self._out_dir / "variant_nodes.csv", "w", newline="")
        self._next_fh = open(self._out_dir / "next_edges.csv", "w", newline="")
        self._on_chrom_fh = open(self._out_dir / "on_chromosome_edges.csv", "w", newline="")

        self._variant_writer = csv.writer(self._variant_fh)
        self._next_writer = csv.writer(self._next_fh)
        self._on_chrom_writer = csv.writer(self._on_chrom_fh)

        self._variant_writer.writerow(VARIANT_HEADER)
        self._next_writer.writerow(NEXT_HEADER)
        self._on_chrom_writer.writerow(ON_CHROMOSOME_HEADER)

    # -- Chunk processing ---------------------------------------------------

    def process_chunk(self, chunk: list[VariantRecord]) -> None:
        """Append Variant nodes, NEXT and ON_CHROMOSOME edges for a chunk."""
        if self._variant_writer is None:
            self._open_streaming_files()

        delim = self._delim
        pop_ids_str = delim.join(self._pop_map.pop_ids)

        vw = self._variant_writer
        nw = self._next_writer
        ow = self._on_chrom_writer

        for rec in chunk:
            # -- Variant node --
            ancestral = rec.ancestral_allele or ""
            is_pol = rec.is_polarized

            # QUAL and FILTER
            qual_str = str(rec.qual) if rec.qual is not None else ""
            filter_str = rec.filter_status if rec.filter_status is not None else "PASS"

            # Pack byte arrays as semicolon-separated signed Java bytes
            gt_packed_str = (
                delim.join(str(b if b < 128 else b - 256) for b in rec.gt_packed)
                if rec.gt_packed
                else ""
            )
            phase_packed_str = (
                delim.join(str(b if b < 128 else b - 256) for b in rec.phase_packed)
                if rec.phase_packed
                else ""
            )
            ploidy_packed_str = (
                delim.join(str(b if b < 128 else b - 256) for b in rec.ploidy_packed)
                if rec.ploidy_packed
                else ""
            )
            called_packed_str = (
                delim.join(str(b if b < 128 else b - 256) for b in rec.called_packed)
                if rec.called_packed
                else ""
            )
            gt_encoding_str = rec.gt_encoding or "dense"

            vw.writerow(
                [
                    rec.id,
                    "Variant",
                    rec.chr,
                    rec.pos,
                    rec.ref,
                    rec.alt,
                    rec.variant_type,
                    pop_ids_str,
                    delim.join(str(x) for x in rec.ac),
                    delim.join(str(x) for x in rec.an),
                    delim.join(_fmt_float(x) for x in rec.af),
                    delim.join(str(x) for x in rec.het_count),
                    delim.join(str(x) for x in rec.hom_alt_count),
                    rec.ac_total,
                    rec.an_total,
                    _fmt_float(rec.af_total),
                    _fmt_float(rec.call_rate),
                    delim.join(_fmt_float(x) for x in rec.het_exp),
                    ancestral,
                    str(is_pol).lower(),
                    qual_str,
                    filter_str,
                    gt_packed_str,
                    phase_packed_str,
                    ploidy_packed_str,
                    called_packed_str,
                    gt_encoding_str,
                    rec.sv_type or "",
                    rec.sv_len if rec.sv_len is not None else "",
                    rec.sv_end if rec.sv_end is not None else "",
                    rec.multiallelic_site or "",
                    rec.allele_index if rec.allele_index is not None else "",
                ]
            )
            self._n_variants += 1

            # -- ON_CHROMOSOME edge --
            chrom = rec.chr
            ow.writerow([rec.id, chrom, "ON_CHROMOSOME"])
            self._n_on_chrom += 1
            self._chromosomes_seen.add(chrom)

            # -- Track first variant per chromosome (for parallel merge) --
            if chrom not in self._first_variant:
                self._first_variant[chrom] = (rec.id, rec.pos)

            # -- NEXT edge (with sort-order check) --
            prev = self._prev_variant.get(chrom)
            if prev is not None:
                prev_id, prev_pos = prev
                if rec.pos < prev_pos and self._sort_warnings < 5:
                    logger.warning(
                        "VCF not sorted by position on %s: pos %d after %d",
                        chrom,
                        rec.pos,
                        prev_pos,
                    )
                    self._sort_warnings += 1
                distance = rec.pos - prev_pos
                nw.writerow([prev_id, rec.id, "NEXT", distance])
                self._n_next += 1
            self._prev_variant[chrom] = (rec.id, rec.pos)

    # -- Finalize -----------------------------------------------------------

    def finalize(self) -> None:
        """Write Chromosome nodes, close streaming file handles, log summary."""
        path = self._out_dir / "chromosome_nodes.csv"
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(CHROMOSOME_HEADER)
            for chrom in sorted(self._chromosomes_seen):
                length = self._contig_lengths.get(chrom, CHR_LENGTHS.get(chrom, 0))
                w.writerow([chrom, "Chromosome", length])

        for fh in (self._variant_fh, self._next_fh, self._on_chrom_fh):
            if fh is not None:
                fh.close()

        self._variant_fh = None
        self._next_fh = None
        self._on_chrom_fh = None

        logger.info(
            "CSV emission complete: %d variants, "
            "%d next edges, %d on_chromosome edges, %d chromosomes",
            self._n_variants,
            self._n_next,
            self._n_on_chrom,
            len(self._chromosomes_seen),
        )

    # -- Convenience runner -------------------------------------------------

    @staticmethod
    def run(
        parser: VCFParser,
        out_dir: str | Path,
        *,
        chunk_size: int = 100_000,
        filter_chain: ImportFilterChain | None = None,
        dataset_id: str = "",
        source_file: str = "",
    ) -> CSVEmitter:
        """Wire parser -> emitter end-to-end and return the emitter.

        Usage::

            emitter = CSVEmitter.run(parser, Path("output/csv"))
        """

        out_dir = Path(out_dir)
        emitter = CSVEmitter(
            out_dir,
            parser.pop_map,
            contig_lengths=parser.contig_lengths,
            dataset_id=dataset_id,
            source_file=source_file,
        )

        emitter.write_static_nodes()

        for chunk in parser.iter_chunks(chunk_size):
            if filter_chain is not None:
                chunk = list(filter_chain.filter(iter(chunk)))
                if not chunk:
                    continue
            emitter.process_chunk(chunk)

        emitter.finalize()
        return emitter

    # -- Properties for inspection ------------------------------------------

    @property
    def n_variants(self) -> int:
        return self._n_variants

    @property
    def n_next(self) -> int:
        return self._n_next

    @property
    def n_on_chrom(self) -> int:
        return self._n_on_chrom

    @property
    def chromosomes_seen(self) -> set[str]:
        return self._chromosomes_seen

    @property
    def first_variant(self) -> dict[str, tuple[str, int]]:
        """First variant per chromosome: chr -> (variant_id, pos)."""
        return self._first_variant

    @property
    def last_variant(self) -> dict[str, tuple[str, int]]:
        """Last variant per chromosome: chr -> (variant_id, pos)."""
        return self._prev_variant
