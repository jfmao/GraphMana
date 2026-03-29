"""Shared configuration for the GraphMana benchmark suite.

All values can be overridden via environment variables.
"""

from __future__ import annotations

import os
from pathlib import Path

# -- Neo4j connection (overridable via env) ----------------------------------
NEO4J_URI = os.environ.get("GRAPHMANA_BENCH_NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("GRAPHMANA_BENCH_NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("GRAPHMANA_BENCH_NEO4J_PASSWORD", "graphmana")

# -- Paths -------------------------------------------------------------------
BENCH_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BENCH_DIR / "results"
FIXTURES_DIR = BENCH_DIR / "fixtures"

# -- Scale tiers: (n_samples, n_variants) ------------------------------------
SCALES: dict[str, tuple[int, int]] = {
    "small": (100, 10_000),
    "medium": (1_000, 100_000),
    "large": (5_000, 1_000_000),
}

# -- Export format groups ----------------------------------------------------
EXPORT_FORMATS_FAST = ["treemix", "sfs-dadi", "sfs-fsc", "bed"]
EXPORT_FORMATS_FULL = ["vcf", "plink", "eigenstrat", "tsv"]
EXPORT_FORMATS_ALL = EXPORT_FORMATS_FAST + EXPORT_FORMATS_FULL

# -- 1KGP benchmark parameters (overridable via env) -----------------------
ONEKG_VCF_DIR = os.environ.get("GRAPHMANA_1KGP_VCF_DIR", "")
ONEKG_POPMAP = os.environ.get("GRAPHMANA_1KGP_POPMAP", "")
ONEKG_N_SAMPLES = 3202
EXPORT_SUBSET_SIZES = [100, 200, 300, 400, 500]
EXPERIMENT_A_REPS = 10
EXPERIMENT_B_REPS = 100

# -- Default benchmark parameters -------------------------------------------
DEFAULT_WARM_RUNS = 3
DEFAULT_CHROMOSOME = "22"
DEFAULT_N_POPULATIONS = 5
DEFAULT_SEED = 42
