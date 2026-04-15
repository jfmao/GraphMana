"""v1.1 micro-benchmarks: called_packed overhead and sparse gt_packed compression.

Measures four quantities that are directly affected by the schema v1.1
changes (called_packed mask + optional sparse gt_packed encoding):

  1. Parser overhead — time to build called_packed per variant, relative to
     the v1.0 packer baseline.
  2. Exporter unpack overhead — time to unpack gt_packed + phase + ploidy +
     called_packed and coerce uncalled slots to Missing, relative to v1.0.
  3. Storage delta — exact byte count of the called_packed column in a
     synthetic VCF-derived variant record set. No Neo4j required.
  4. Sparse gt_packed compression ratio — measured over a realistic allele
     frequency distribution. This is what extends the practical ceiling.

All numbers are reported at sample count N = 3,202 (1000 Genomes chr22 scale)
and whole-genome projections for ~85M variants are computed analytically from
per-variant measurements.

Run from the repo root:

    python benchmarks/bench_v11_called_packed.py

Output goes to benchmarks/results/v1_1_bench.md (Markdown table) and, as a
machine-readable companion, v1_1_bench.jsonl.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np

from graphmana.export.base import BaseExporter
from graphmana.ingest.array_ops import _pack_codes_direct
from graphmana.ingest.genotype_packer import (
    build_called_packed,
    build_called_packed_all,
    decode_gt_blob,
    encode_gt_blob,
    pack_phase,
    unpack_called_packed,
    vectorized_gt_pack,
)

RESULTS_DIR = Path(__file__).parent / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
MD_PATH = RESULTS_DIR / "v1_1_bench.md"
JSONL_PATH = RESULTS_DIR / "v1_1_bench.jsonl"

# 1KGP chr22 dimensions (used for extrapolations)
N_1KGP = 3202
N_CHR22_VARIANTS = 1_066_555
N_WGS_VARIANTS = 85_000_000


def _timed(fn, *, iters: int, warmup: int = 3):
    """Run fn iters times, return mean seconds per call."""
    for _ in range(warmup):
        fn()
    t0 = time.perf_counter()
    for _ in range(iters):
        fn()
    return (time.perf_counter() - t0) / iters


def _draw_realistic_gt_types(n_samples: int, alt_af: float, rng: np.random.Generator) -> np.ndarray:
    """Draw cyvcf2-coded genotype vector under HWE at the given ALT allele frequency.

    cyvcf2 codes: 0=HomRef, 1=Het, 2=Missing, 3=HomAlt. We use a tiny missingness
    rate (1%) to reflect real joint-called data.
    """
    p = float(alt_af)
    p2 = p * p
    q2 = (1.0 - p) * (1.0 - p)
    pq2 = 2.0 * p * (1.0 - p)
    probs = np.array([q2 * 0.99, pq2 * 0.99, 0.01, p2 * 0.99])
    probs /= probs.sum()
    return rng.choice(np.array([0, 1, 2, 3], dtype=np.int8), size=n_samples, p=probs)


def bench_parser_overhead(n_samples: int, iters: int) -> dict:
    """Measure the added cost of build_called_packed on top of vectorized_gt_pack."""
    rng = np.random.default_rng(42)
    gt = _draw_realistic_gt_types(n_samples, 0.05, rng)

    # Baseline: v1.0 packer (gt + phase + no called_packed)
    def v10_pack():
        vectorized_gt_pack(gt)
        het_idx = np.flatnonzero(gt == 1)
        if len(het_idx):
            pack_phase(n_samples, het_idx, [[0, 1, True]] * len(gt))
        else:
            bytes((n_samples + 7) >> 3)

    # v1.1 packer: same + called_packed
    def v11_pack():
        vectorized_gt_pack(gt)
        het_idx = np.flatnonzero(gt == 1)
        if len(het_idx):
            pack_phase(n_samples, het_idx, [[0, 1, True]] * len(gt))
        else:
            bytes((n_samples + 7) >> 3)
        build_called_packed(gt)

    t_v10 = _timed(v10_pack, iters=iters)
    t_v11 = _timed(v11_pack, iters=iters)
    return {
        "bench": "parser_overhead",
        "n_samples": n_samples,
        "iters": iters,
        "v1_0_sec_per_variant": t_v10,
        "v1_1_sec_per_variant": t_v11,
        "overhead_sec": t_v11 - t_v10,
        "overhead_pct": 100.0 * (t_v11 - t_v10) / t_v10,
    }


def bench_export_unpack_overhead(n_samples: int, iters: int) -> dict:
    """Measure BaseExporter._unpack_variant_genotypes cost with and without called_packed.

    Compares:
      - v1.0-style: no called_packed property, helper returns legacy semantics.
      - v1.1-style: called_packed present, uncalled→Missing coercion applied.
    """
    rng = np.random.default_rng(42)
    gt = _draw_realistic_gt_types(n_samples, 0.05, rng)
    gt_packed = vectorized_gt_pack(gt)

    # Fake variant dict in both shapes.
    props_v10 = {
        "gt_packed": gt_packed,
        "phase_packed": b"",
        "ploidy_packed": None,
    }
    props_v11 = dict(props_v10)
    # Full-ones called mask (joint-called cohort, no missingness change in semantics)
    props_v11["called_packed"] = build_called_packed_all(n_samples, 1)

    class _Dummy(BaseExporter):
        def export(self, output):  # pragma: no cover
            return {}

    dummy = _Dummy.__new__(_Dummy)
    BaseExporter.__init__(dummy, conn=None)
    idx = np.arange(n_samples, dtype=np.int64)

    def v10_unpack():
        dummy._unpack_variant_genotypes(props_v10, idx)

    def v11_unpack():
        dummy._unpack_variant_genotypes(props_v11, idx)

    t_v10 = _timed(v10_unpack, iters=iters)
    t_v11 = _timed(v11_unpack, iters=iters)
    return {
        "bench": "export_unpack_overhead",
        "n_samples": n_samples,
        "iters": iters,
        "v1_0_sec_per_variant": t_v10,
        "v1_1_sec_per_variant": t_v11,
        "overhead_sec": t_v11 - t_v10,
        "overhead_pct": 100.0 * (t_v11 - t_v10) / t_v10,
    }


def bench_storage_delta(n_samples: int, n_variants: int) -> dict:
    """Measure exact byte counts of gt_packed + phase_packed + called_packed.

    Reports per-variant bytes for each packed array and extrapolated totals at
    chr22 scale (1.07M variants) and whole-genome scale (~85M variants).
    """
    rng = np.random.default_rng(42)
    gt = _draw_realistic_gt_types(n_samples, 0.05, rng)
    gt_packed_len = len(vectorized_gt_pack(gt))
    phase_packed_len = (n_samples + 7) >> 3
    called_packed_len = (n_samples + 7) >> 3

    v10_per_variant = gt_packed_len + phase_packed_len
    v11_per_variant = gt_packed_len + phase_packed_len + called_packed_len

    return {
        "bench": "storage_delta",
        "n_samples": n_samples,
        "gt_packed_bytes": gt_packed_len,
        "phase_packed_bytes": phase_packed_len,
        "called_packed_bytes": called_packed_len,
        "v1_0_per_variant_bytes": v10_per_variant,
        "v1_1_per_variant_bytes": v11_per_variant,
        "added_bytes_per_variant": called_packed_len,
        "added_pct": 100.0 * called_packed_len / v10_per_variant,
        "chr22_total_v1_0_mb": v10_per_variant * N_CHR22_VARIANTS / (1024**2),
        "chr22_total_v1_1_mb": v11_per_variant * N_CHR22_VARIANTS / (1024**2),
        "wgs_total_v1_0_gb": v10_per_variant * N_WGS_VARIANTS / (1024**3),
        "wgs_total_v1_1_gb": v11_per_variant * N_WGS_VARIANTS / (1024**3),
    }


def bench_sparse_compression(n_samples: int) -> dict:
    """Measure sparse vs dense gt_packed size across a realistic SFS.

    Draws variants at a grid of allele frequencies spanning the rare-to-common
    spectrum and reports the mean compression ratio weighted by a neutral SFS.
    Under neutrality the variant count at frequency f is ~1/f, so rare variants
    dominate the cohort and sparse encoding wins on most sites.
    """
    rng = np.random.default_rng(7)
    # Allele frequency grid and neutral weighting (1/f truncated at singleton).
    afs = np.array([0.001, 0.003, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.3, 0.5])
    weights_neutral = 1.0 / afs
    weights_neutral /= weights_neutral.sum()

    rows = []
    n_per_bin = 200
    for af in afs:
        sparse_len_sum = 0
        dense_len_sum = 0
        chose_sparse = 0
        for _ in range(n_per_bin):
            gt = _draw_realistic_gt_types(n_samples, af, rng)
            dense = vectorized_gt_pack(gt)
            blob = encode_gt_blob(dense, n_samples)
            dense_len = 1 + len(dense)  # dense tagged-blob equivalent
            blob_len = len(blob)
            sparse_len_sum += blob_len
            dense_len_sum += dense_len
            if blob[0] == 0x01:
                chose_sparse += 1
            # sanity: decode round-trip
            assert decode_gt_blob(blob, n_samples) == dense
        mean_blob = sparse_len_sum / n_per_bin
        mean_dense = dense_len_sum / n_per_bin
        rows.append(
            {
                "af": float(af),
                "mean_tagged_blob_bytes": mean_blob,
                "mean_dense_bytes": mean_dense,
                "compression_ratio": mean_dense / mean_blob,
                "sparse_chosen_frac": chose_sparse / n_per_bin,
            }
        )

    # Neutral-SFS weighted mean compression ratio.
    ratios = np.array([r["compression_ratio"] for r in rows])
    mean_ratio_neutral = float(np.sum(ratios * weights_neutral))

    # Effective ceiling extension: v1.0 at 50K samples saturated the per-property
    # comfort zone at ceil(50000/4)=12,500 bytes per variant. Under sparse
    # encoding the same dataset lands at ~12500/mean_ratio_neutral bytes, which
    # corresponds to an effective N of 4 * 12,500 * mean_ratio_neutral for
    # storage-limited scaling.
    effective_ceiling = int(4 * 12_500 * mean_ratio_neutral)

    return {
        "bench": "sparse_compression",
        "n_samples": n_samples,
        "per_af_rows": rows,
        "mean_compression_ratio_neutral_sfs": mean_ratio_neutral,
        "effective_storage_ceiling_samples": effective_ceiling,
    }


def main() -> int:
    results = []
    print("Running v1.1 micro-benchmarks at N =", N_1KGP)

    r1 = bench_parser_overhead(N_1KGP, iters=200)
    print(
        f"  parser overhead:          {r1['overhead_sec']*1e6:7.2f} µs/variant "
        f"({r1['overhead_pct']:+.1f}%)"
    )
    results.append(r1)

    r2 = bench_export_unpack_overhead(N_1KGP, iters=200)
    print(
        f"  export unpack overhead:   {r2['overhead_sec']*1e6:7.2f} µs/variant "
        f"({r2['overhead_pct']:+.1f}%)"
    )
    results.append(r2)

    r3 = bench_storage_delta(N_1KGP, N_CHR22_VARIANTS)
    print(
        f"  storage delta:            +{r3['added_bytes_per_variant']} bytes/variant "
        f"(+{r3['added_pct']:.1f}%)"
    )
    print(
        f"    chr22 total:            v1.0 {r3['chr22_total_v1_0_mb']:8.1f} MB -> "
        f"v1.1 {r3['chr22_total_v1_1_mb']:8.1f} MB"
    )
    results.append(r3)

    r4 = bench_sparse_compression(N_1KGP)
    print(
        f"  sparse compression ratio: {r4['mean_compression_ratio_neutral_sfs']:5.2f}x "
        f"(neutral-SFS weighted mean)"
    )
    print(
        f"  effective ceiling:        ~{r4['effective_storage_ceiling_samples']:,} "
        f"samples at same per-property byte budget"
    )
    results.append(r4)

    # Persist JSONL (machine readable)
    with open(JSONL_PATH, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")

    # Persist a small Markdown summary that the paper can cite.
    lines = [
        "# GraphMana v1.1 micro-benchmarks",
        "",
        f"Reference dataset: 1000 Genomes chromosome 22 scale "
        f"(N = {N_1KGP} samples, {N_CHR22_VARIANTS:,} variants).",
        "",
        "## 1. Parser overhead (called_packed bit packing on top of v1.0 packer)",
        "",
        f"- Mean v1.0 packer time per variant: {r1['v1_0_sec_per_variant']*1e6:.2f} µs",
        f"- Mean v1.1 packer time per variant: {r1['v1_1_sec_per_variant']*1e6:.2f} µs",
        f"- Added time: {r1['overhead_sec']*1e6:.2f} µs ({r1['overhead_pct']:+.1f}%)",
        "",
        "## 2. Exporter unpack overhead (called_packed → Missing coercion)",
        "",
        f"- Mean v1.0 unpack time per variant: {r2['v1_0_sec_per_variant']*1e6:.2f} µs",
        f"- Mean v1.1 unpack time per variant: {r2['v1_1_sec_per_variant']*1e6:.2f} µs",
        f"- Added time: {r2['overhead_sec']*1e6:.2f} µs ({r2['overhead_pct']:+.1f}%)",
        "",
        "## 3. Storage delta from called_packed",
        "",
        f"- gt_packed: {r3['gt_packed_bytes']} bytes/variant",
        f"- phase_packed: {r3['phase_packed_bytes']} bytes/variant",
        f"- called_packed: {r3['called_packed_bytes']} bytes/variant (new)",
        f"- Per-variant total: v1.0 {r3['v1_0_per_variant_bytes']} B -> "
        f"v1.1 {r3['v1_1_per_variant_bytes']} B (+{r3['added_pct']:.1f}%)",
        f"- chr22 total (dense, N=3,202): "
        f"v1.0 {r3['chr22_total_v1_0_mb']:.1f} MB -> v1.1 {r3['chr22_total_v1_1_mb']:.1f} MB",
        f"- Whole-genome projection (~85M variants, N=3,202): "
        f"v1.0 {r3['wgs_total_v1_0_gb']:.2f} GB -> v1.1 {r3['wgs_total_v1_1_gb']:.2f} GB",
        "",
        "## 4. Sparse gt_packed compression across a realistic allele frequency grid",
        "",
        "| allele freq | mean dense (B) | mean tagged blob (B) | compression | sparse chosen |",
        "|---|---|---|---|---|",
    ]
    for row in r4["per_af_rows"]:
        lines.append(
            f"| {row['af']:.3f} | {row['mean_dense_bytes']:.0f} | "
            f"{row['mean_tagged_blob_bytes']:.0f} | {row['compression_ratio']:.2f}x | "
            f"{row['sparse_chosen_frac']*100:.0f}% |"
        )
    lines += [
        "",
        f"Neutral-SFS weighted mean compression: "
        f"**{r4['mean_compression_ratio_neutral_sfs']:.2f}x** on gt_packed.",
        "",
        "Under this compression ratio, the effective per-variant byte budget at the "
        "Neo4j property-size comfort zone (~12.5 KB, corresponding to ~50,000 samples "
        "in v1.0 dense encoding) now accommodates approximately "
        f"**{r4['effective_storage_ceiling_samples']:,}** samples at equivalent storage "
        "pressure.",
        "",
    ]
    MD_PATH.write_text("\n".join(lines))
    print(f"\nWrote {MD_PATH} and {JSONL_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
