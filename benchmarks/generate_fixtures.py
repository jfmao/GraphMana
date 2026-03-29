"""Generate synthetic VCF and population map files for benchmarking.

Creates realistic-looking VCF data with controllable sample count,
variant count, and population structure. Output is bgzipped + tabix-indexed
when bcftools is available, plain VCF otherwise.

Usage::

    python benchmarks/generate_fixtures.py --scale medium --output-dir benchmarks/fixtures
    python benchmarks/generate_fixtures.py --samples 500 --variants 50000 --output-dir /tmp/bench
"""

from __future__ import annotations

import argparse
import gzip
import shutil
import subprocess
from pathlib import Path

import numpy as np

from conftest import DEFAULT_CHROMOSOME, DEFAULT_N_POPULATIONS, DEFAULT_SEED, SCALES

# VCF constants
_BASES = ["A", "C", "G", "T"]
_CHR22_LENGTH = 50_818_468


def generate_vcf(
    output_path: Path,
    n_samples: int,
    n_variants: int,
    *,
    chrom: str = DEFAULT_CHROMOSOME,
    seed: int = DEFAULT_SEED,
) -> list[str]:
    """Write a synthetic VCF to *output_path*.

    Returns the list of sample IDs.
    """
    rng = np.random.default_rng(seed)
    sample_ids = [f"SAMPLE_{i:05d}" for i in range(n_samples)]

    # Spread positions across chromosome range
    positions = np.sort(rng.integers(1, _CHR22_LENGTH, size=n_variants))
    # Ensure unique positions
    positions = np.unique(positions)
    while len(positions) < n_variants:
        extra = rng.integers(1, _CHR22_LENGTH, size=n_variants - len(positions))
        positions = np.unique(np.concatenate([positions, extra]))
    positions = np.sort(positions[:n_variants])

    # Genotype probabilities: ~60% HomRef, ~25% Het, ~10% HomAlt, ~5% Missing
    gt_probs = np.array([0.60, 0.25, 0.10, 0.05])
    gt_strings = ["0/0", "0/1", "1/1", "./."]

    opener = gzip.open if str(output_path).endswith(".gz") else open
    mode = "wt" if str(output_path).endswith(".gz") else "w"

    with opener(output_path, mode) as f:
        # Header
        f.write("##fileformat=VCFv4.2\n")
        f.write(f"##contig=<ID={chrom},length={_CHR22_LENGTH}>\n")
        f.write('##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n')
        f.write('##FILTER=<ID=PASS,Description="All filters passed">\n')
        f.write('##INFO=<ID=AC,Number=A,Type=Integer,Description="Allele count">\n')
        f.write('##INFO=<ID=AN,Number=1,Type=Integer,Description="Total alleles">\n')

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

        # Variants
        for i in range(n_variants):
            pos = int(positions[i])
            ref_idx = rng.integers(0, 4)
            alt_idx = (ref_idx + rng.integers(1, 4)) % 4
            ref = _BASES[ref_idx]
            alt = _BASES[alt_idx]

            qual = int(rng.integers(20, 1000))
            filt = "PASS"
            vid = f"chr{chrom}_{pos}_{ref}_{alt}"

            # Draw genotypes
            gt_indices = rng.choice(4, size=n_samples, p=gt_probs)
            gts = [gt_strings[g] for g in gt_indices]

            # Compute AC/AN from non-missing
            alt_count = int(np.sum(gt_indices == 1) + 2 * np.sum(gt_indices == 2))
            n_called = int(np.sum(gt_indices != 3))
            an = 2 * n_called

            info = f"AC={alt_count};AN={an}"
            fields = [chrom, str(pos), vid, ref, alt, str(qual), filt, info, "GT"]
            fields.extend(gts)
            f.write("\t".join(fields) + "\n")

    return sample_ids


def generate_population_map(
    output_path: Path,
    sample_ids: list[str],
    n_populations: int = DEFAULT_N_POPULATIONS,
    *,
    seed: int = DEFAULT_SEED,
) -> None:
    """Write a TSV population map assigning samples to balanced groups."""
    rng = np.random.default_rng(seed)
    pop_names = [f"POP_{i}" for i in range(n_populations)]

    # Round-robin assignment with slight randomization
    assignments = [pop_names[i % n_populations] for i in range(len(sample_ids))]
    rng.shuffle(assignments)

    with open(output_path, "w") as f:
        f.write("sample\tpopulation\n")
        for sid, pop in zip(sample_ids, assignments):
            f.write(f"{sid}\t{pop}\n")


def maybe_index_vcf(vcf_path: Path) -> Path:
    """If bcftools available and output is .gz, create tabix index. Return final path."""
    if not str(vcf_path).endswith(".gz"):
        return vcf_path
    if shutil.which("bcftools") is None:
        return vcf_path
    try:
        subprocess.run(
            ["bcftools", "index", "-t", str(vcf_path)],
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    return vcf_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic VCF + population map for benchmarking."
    )
    parser.add_argument(
        "--scale",
        choices=list(SCALES.keys()),
        help="Predefined scale tier (overrides --samples/--variants).",
    )
    parser.add_argument("--samples", type=int, help="Number of samples.")
    parser.add_argument("--variants", type=int, help="Number of variants.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory for output files.",
    )
    parser.add_argument(
        "--populations",
        type=int,
        default=DEFAULT_N_POPULATIONS,
        help="Number of populations.",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--chrom", default=DEFAULT_CHROMOSOME, help="Chromosome name.")
    args = parser.parse_args()

    if args.scale:
        n_samples, n_variants = SCALES[args.scale]
    elif args.samples and args.variants:
        n_samples, n_variants = args.samples, args.variants
    else:
        parser.error("Provide --scale or both --samples and --variants.")
        return  # unreachable

    args.output_dir.mkdir(parents=True, exist_ok=True)
    vcf_path = args.output_dir / f"bench_{n_samples}s_{n_variants}v.vcf.gz"
    pop_path = args.output_dir / f"bench_{n_samples}s_{n_variants}v_popmap.tsv"

    print(f"Generating {n_samples} samples × {n_variants} variants → {vcf_path}")
    sample_ids = generate_vcf(
        vcf_path,
        n_samples,
        n_variants,
        chrom=args.chrom,
        seed=args.seed,
    )
    maybe_index_vcf(vcf_path)

    print(f"Generating population map → {pop_path}")
    generate_population_map(pop_path, sample_ids, args.populations, seed=args.seed)
    print("Done.")


if __name__ == "__main__":
    main()
