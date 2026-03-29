"""Load pre-generated CSV files into Neo4j via neo4j-admin database import.

This module handles the second step of the two-step split import:
1. prepare-csv: generate CSV files (no Neo4j needed)
2. load-csv: import CSVs into Neo4j (this module)

Uses ``neo4j-admin database import full`` for maximum performance (10-100x
faster than LOAD CSV Cypher). No sudo required for user-space Neo4j.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

from graphmana.config import DEFAULT_DATABASE

logger = logging.getLogger(__name__)

# The 7 core CSV files produced by CSVEmitter
REQUIRED_CSV_FILES = [
    "variant_nodes.csv",
    "sample_nodes.csv",
    "population_nodes.csv",
    "chromosome_nodes.csv",
    "next_edges.csv",
    "on_chromosome_edges.csv",
    "in_population_edges.csv",
]

# Optional CSV files produced by VEPParser or VCFHeader extraction
OPTIONAL_CSV_FILES = [
    "gene_nodes.csv",
    "has_consequence_edges.csv",
    "vcf_header_nodes.csv",
]


def validate_csv_dir(csv_dir: str | Path) -> list[str]:
    """Check that required CSV files exist in the directory.

    Returns a list of missing file names. Empty list means all present.
    """
    csv_dir = Path(csv_dir)
    missing = []
    for name in REQUIRED_CSV_FILES:
        if not (csv_dir / name).exists():
            missing.append(name)
    return missing


def find_neo4j_admin(neo4j_home: str | Path) -> Path:
    """Locate the neo4j-admin binary under neo4j_home/bin/.

    Raises FileNotFoundError if not found.
    """
    neo4j_home = Path(neo4j_home)
    candidate = neo4j_home / "bin" / "neo4j-admin"
    if candidate.exists():
        return candidate
    # Windows fallback
    candidate_bat = neo4j_home / "bin" / "neo4j-admin.bat"
    if candidate_bat.exists():
        return candidate_bat
    raise FileNotFoundError(
        f"neo4j-admin not found at {neo4j_home / 'bin' / 'neo4j-admin'}. "
        f"Check --neo4j-home path."
    )


def _build_import_command(
    neo4j_admin: Path,
    csv_dir: Path,
    *,
    database: str = DEFAULT_DATABASE,
    array_delimiter: str = ";",
    overwrite: bool = True,
) -> list[str]:
    """Construct the neo4j-admin database import full command."""
    csv_dir = Path(csv_dir)

    cmd = [
        str(neo4j_admin),
        "database",
        "import",
        "full",
        database,
        f"--array-delimiter={array_delimiter}",
        "--id-type=string",
        "--skip-bad-relationships=true",
        "--skip-duplicate-nodes=true",
    ]

    if overwrite:
        cmd.append("--overwrite-destination=true")

    # Node files
    cmd.append(f"--nodes=Variant={csv_dir / 'variant_nodes.csv'}")
    cmd.append(f"--nodes=Sample={csv_dir / 'sample_nodes.csv'}")
    cmd.append(f"--nodes=Population={csv_dir / 'population_nodes.csv'}")
    cmd.append(f"--nodes=Chromosome={csv_dir / 'chromosome_nodes.csv'}")

    # Optional gene nodes
    gene_csv = csv_dir / "gene_nodes.csv"
    if gene_csv.exists():
        cmd.append(f"--nodes=Gene={gene_csv}")

    # Optional VCFHeader node
    vcf_header_csv = csv_dir / "vcf_header_nodes.csv"
    if vcf_header_csv.exists():
        cmd.append(f"--nodes=VCFHeader={vcf_header_csv}")

    # Relationship files
    cmd.append(f"--relationships=NEXT={csv_dir / 'next_edges.csv'}")
    cmd.append(f"--relationships=ON_CHROMOSOME={csv_dir / 'on_chromosome_edges.csv'}")
    cmd.append(f"--relationships=IN_POPULATION={csv_dir / 'in_population_edges.csv'}")

    # Optional consequence edges
    consequence_csv = csv_dir / "has_consequence_edges.csv"
    if consequence_csv.exists():
        cmd.append(f"--relationships=HAS_CONSEQUENCE={consequence_csv}")

    return cmd


def run_load_csv(
    csv_dir: str | Path,
    *,
    neo4j_home: str | Path,
    database: str = DEFAULT_DATABASE,
    overwrite: bool = True,
) -> subprocess.CompletedProcess:
    """Run neo4j-admin database import full on a directory of CSVs.

    Args:
        csv_dir: directory containing the CSV files.
        neo4j_home: Neo4j installation directory.
        database: target database name.
        overwrite: if True, overwrite existing database.

    Returns:
        The CompletedProcess result.

    Raises:
        FileNotFoundError: if neo4j-admin binary or required CSVs are missing.
        subprocess.CalledProcessError: if the import command fails.
    """
    csv_dir = Path(csv_dir)
    missing = validate_csv_dir(csv_dir)
    if missing:
        raise FileNotFoundError(f"Missing required CSV files in {csv_dir}: {', '.join(missing)}")

    neo4j_admin = find_neo4j_admin(neo4j_home)
    cmd = _build_import_command(
        neo4j_admin,
        csv_dir,
        database=database,
        overwrite=overwrite,
    )

    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    if result.returncode != 0:
        logger.error("neo4j-admin import failed (exit %d)", result.returncode)
        if result.stderr:
            logger.error("stderr: %s", result.stderr[:2000])
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)

    logger.info("neo4j-admin import completed successfully")
    if result.stdout:
        logger.info("stdout: %s", result.stdout[:2000])

    return result


def apply_post_import_indexes(
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    *,
    database: str | None = None,
    reference_genome: str = "unknown",
    chr_naming_style: str = "auto",
) -> None:
    """Connect to Neo4j and create constraints, indexes, and schema metadata.

    Should be called after neo4j-admin import + database start.
    """
    from graphmana.db.connection import GraphManaConnection
    from graphmana.db.schema import ensure_schema

    with GraphManaConnection(neo4j_uri, neo4j_user, neo4j_password, database=database) as conn:
        ensure_schema(
            conn,
            reference_genome=reference_genome,
            chr_naming_style=chr_naming_style,
        )
    logger.info("Post-import indexes and schema metadata created")
