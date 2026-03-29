"""Fixtures for 1KGP integration tests.

Manages a temporary Neo4j instance with GraphMana-ingested data.
Requires: Neo4j installed at /usr/share/neo4j, port 7687 free.

Session-scoped fixtures run prepare-csv + neo4j-admin import ONCE,
then all test classes share the same running Neo4j instance.
"""

import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

NEO4J_HOME = Path("/usr/share/neo4j")
NEO4J_ADMIN = Path("/usr/bin/neo4j-admin")
BOLT_PORT = 7687
HTTP_PORT = 7474

CHR22_VCF = Path(
    "/mnt/data/GraphPop/data/raw/1000g/"
    "CCDG_14151_B01_GRM_WGS_2020-08-05_chr22.filtered.shapeit2-duohmm-phased.vcf.gz"
)
FULL_GENOME_VCF_DIR = Path("/mnt/data/GraphPop/data/raw/1000g/vcf")
POPULATION_PANEL = Path(
    "/mnt/data/GraphPop/data/raw/1000g/"
    "integrated_call_samples_v3.20130502.ALL.panel"
)

# Default parallelism for prepare-csv
DEFAULT_THREADS = 4


def _port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) != 0


def _wait_for_neo4j(timeout: int = 120) -> bool:
    """Poll until Neo4j responds on bolt port."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _port_is_free(BOLT_PORT):
            try:
                from neo4j import GraphDatabase

                d = GraphDatabase.driver(
                    f"bolt://localhost:{BOLT_PORT}", auth=None
                )
                d.verify_connectivity()
                d.close()
                return True
            except Exception:
                pass
        time.sleep(2)
    return False


def _write_neo4j_conf(conf_dir: Path, data_dir: Path, logs_dir: Path,
                      run_dir: Path, heap: str = "2g",
                      pagecache: str = "2g") -> Path:
    """Write a minimal neo4j.conf for the test instance."""
    conf_file = conf_dir / "neo4j.conf"
    conf_file.write_text(
        f"server.directories.data={data_dir}\n"
        f"server.directories.logs={logs_dir}\n"
        f"server.directories.run={run_dir}\n"
        f"server.bolt.enabled=true\n"
        f"server.bolt.listen_address=:{BOLT_PORT}\n"
        f"server.http.enabled=true\n"
        f"server.http.listen_address=:{HTTP_PORT}\n"
        f"server.memory.heap.initial_size={heap}\n"
        f"server.memory.heap.max_size={heap}\n"
        f"server.memory.pagecache.size={pagecache}\n"
        f"dbms.security.auth_enabled=false\n"
        f"server.directories.transaction.logs.root={data_dir}/transactions\n"
    )
    return conf_file


def _start_neo4j(conf_dir: Path) -> None:
    env = os.environ.copy()
    env["NEO4J_CONF"] = str(conf_dir)
    subprocess.run(
        [str(NEO4J_HOME / "bin" / "neo4j"), "start"],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


def _stop_neo4j(conf_dir: Path) -> None:
    env = os.environ.copy()
    env["NEO4J_CONF"] = str(conf_dir)
    subprocess.run(
        [str(NEO4J_HOME / "bin" / "neo4j"), "stop"],
        env=env,
        capture_output=True,
        text=True,
    )


def _neo4j_admin_import(csv_dir: Path, conf_file: Path) -> None:
    """Run neo4j-admin database import full."""
    cmd = [
        str(NEO4J_ADMIN),
        "database",
        "import",
        "full",
        "neo4j",
        f"--additional-config={conf_file}",
        "--array-delimiter=;",
        "--id-type=string",
        "--overwrite-destination=true",
        "--skip-bad-relationships=true",
        "--skip-duplicate-nodes=true",
        f"--nodes=Variant={csv_dir / 'variant_nodes.csv'}",
        f"--nodes=Sample={csv_dir / 'sample_nodes.csv'}",
        f"--nodes=Population={csv_dir / 'population_nodes.csv'}",
        f"--nodes=Chromosome={csv_dir / 'chromosome_nodes.csv'}",
        f"--relationships=NEXT={csv_dir / 'next_edges.csv'}",
        f"--relationships=ON_CHROMOSOME={csv_dir / 'on_chromosome_edges.csv'}",
        f"--relationships=IN_POPULATION={csv_dir / 'in_population_edges.csv'}",
    ]
    if (csv_dir / "vcf_header_nodes.csv").exists():
        cmd.append(f"--nodes=VCFHeader={csv_dir / 'vcf_header_nodes.csv'}")
    if (csv_dir / "gene_nodes.csv").exists():
        cmd.append(f"--nodes=Gene={csv_dir / 'gene_nodes.csv'}")
    if (csv_dir / "has_consequence_edges.csv").exists():
        cmd.append(
            f"--relationships=HAS_CONSEQUENCE={csv_dir / 'has_consequence_edges.csv'}"
        )

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"neo4j-admin import failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )


def _prepare_csv(vcf_files: list[Path], panel: Path, output_dir: Path,
                 threads: int = DEFAULT_THREADS) -> subprocess.CompletedProcess:
    """Run graphmana prepare-csv."""
    cmd = [
        "graphmana", "prepare-csv",
        "--population-map", str(panel),
        "--stratify-by", "population",
        "--output-dir", str(output_dir),
        "--reference", "GRCh38",
        "--threads", str(threads),
    ]
    for vcf in vcf_files:
        cmd.extend(["--input", str(vcf)])

    return subprocess.run(cmd, capture_output=True, text=True, timeout=14400)


class Neo4jTestInstance:
    """Manages a temporary Neo4j instance for integration testing."""

    def __init__(self, base_dir: Path, heap: str = "2g", pagecache: str = "2g"):
        self.base_dir = base_dir
        self.data_dir = base_dir / "neo4j_data"
        self.conf_dir = base_dir / "neo4j_conf"
        self.logs_dir = base_dir / "neo4j_logs"
        self.run_dir = base_dir / "neo4j_run"
        self.csv_dir = base_dir / "csv"
        self.export_dir = base_dir / "exports"
        self.heap = heap
        self.pagecache = pagecache
        self._started = False

        for d in [self.data_dir, self.conf_dir, self.logs_dir,
                  self.run_dir, self.csv_dir, self.export_dir]:
            d.mkdir(parents=True, exist_ok=True)

        self.conf_file = _write_neo4j_conf(
            self.conf_dir, self.data_dir, self.logs_dir, self.run_dir,
            heap, pagecache
        )

    def import_csv(self, csv_dir: Path) -> None:
        _neo4j_admin_import(csv_dir, self.conf_file)

    def start(self, timeout: int = 120) -> None:
        _start_neo4j(self.conf_dir)
        if not _wait_for_neo4j(timeout):
            raise RuntimeError(f"Neo4j did not start within {timeout}s")
        self._started = True

    def stop(self) -> None:
        if self._started:
            _stop_neo4j(self.conf_dir)
            self._started = False
            deadline = time.time() + 30
            while time.time() < deadline:
                if _port_is_free(BOLT_PORT):
                    break
                time.sleep(1)

    def cleanup(self) -> None:
        self.stop()
        if self.base_dir.exists():
            shutil.rmtree(self.base_dir, ignore_errors=True)

    @property
    def bolt_uri(self) -> str:
        return f"bolt://localhost:{BOLT_PORT}"


def _check_prerequisites(need_vcf: Path = CHR22_VCF):
    """Check prerequisites and skip if missing."""
    if not NEO4J_HOME.exists():
        pytest.skip("Neo4j not installed at /usr/share/neo4j")
    if not NEO4J_ADMIN.exists():
        pytest.skip("neo4j-admin not found")
    if not need_vcf.exists():
        pytest.skip(f"VCF not found: {need_vcf}")
    if not POPULATION_PANEL.exists():
        pytest.skip(f"Population panel not found: {POPULATION_PANEL}")
    if not _port_is_free(BOLT_PORT):
        pytest.skip(
            f"Port {BOLT_PORT} is in use — stop the existing Neo4j instance first"
        )


# ---------------------------------------------------------------------------
# Session-scoped fixtures: run prepare-csv + import ONCE, share across tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def chr22_csv_dir(tmp_path_factory):
    """Prepare CSV files from chr22 VCF (runs once per session)."""
    _check_prerequisites()
    csv_dir = tmp_path_factory.mktemp("chr22_csv")
    t0 = time.time()
    result = _prepare_csv([CHR22_VCF], POPULATION_PANEL, csv_dir,
                          threads=DEFAULT_THREADS)
    wall = time.time() - t0
    if result.returncode != 0:
        pytest.fail(
            f"prepare-csv failed (exit {result.returncode}):\n"
            f"STDERR: {result.stderr[-3000:]}"
        )
    print(f"\n  [session] prepare-csv chr22: {wall:.1f}s ({DEFAULT_THREADS} threads)")
    return csv_dir


@pytest.fixture(scope="session")
def chr22_neo4j(chr22_csv_dir, tmp_path_factory):
    """Neo4j instance loaded with chr22 data (runs once per session).

    Yields the Neo4jTestInstance with a running Neo4j ready for queries/exports.
    """
    base_dir = tmp_path_factory.mktemp("chr22_neo4j")
    instance = Neo4jTestInstance(base_dir, heap="2g", pagecache="2g")

    t0 = time.time()
    instance.import_csv(chr22_csv_dir)
    import_time = time.time() - t0
    print(f"\n  [session] neo4j-admin import: {import_time:.1f}s")

    instance.start(timeout=120)
    print(f"  [session] Neo4j started on bolt://localhost:{BOLT_PORT}")

    yield instance

    instance.stop()
    print(f"\n  [session] Neo4j stopped")


@pytest.fixture(scope="session")
def chr22_export_dir(tmp_path_factory):
    """Shared export output directory for chr22 tests."""
    return tmp_path_factory.mktemp("chr22_exports")
