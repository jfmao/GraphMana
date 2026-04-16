"""Neo4j user-space lifecycle management for HPC cluster deployments.

Academic cluster users typically cannot use package managers or systemd.
This module provides:

- ``setup_neo4j()``: Download and configure Neo4j Community for user-space
  operation (no root needed).
- ``start_neo4j()``: Start Neo4j in user space and wait for readiness.
- ``stop_neo4j()``: Gracefully stop a user-space Neo4j instance.
- ``auto_memory_config()``: Auto-set heap and page cache based on available RAM.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import tarfile
import time
from pathlib import Path

logger = logging.getLogger(__name__)

NEO4J_DEFAULT_BOLT_PORT = 7687
NEO4J_DEFAULT_HTTP_PORT = 7474
NEO4J_DOWNLOAD_URL_TEMPLATE = "https://dist.neo4j.org/neo4j-community-{version}-unix.tar.gz"
# Note: auto-download uses 5.26.2; the Zenodo deposit (DOI 10.5281/zenodo.19603203)
# hosts 5.26.0. Both are 5.26.x and compatible. The tarball validator accepts any
# 5.26.x version. Update the Zenodo tarball when a security patch warrants it.
NEO4J_DEFAULT_VERSION = "5.26.2"
_TARBALL_RE = re.compile(r"neo4j-community-(5\.26\.\d+)-unix\.tar\.gz$")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class PortConflictError(RuntimeError):
    """Raised when a required port is already in use."""

    def __init__(self, port: int, pid: str | None, instructions: str) -> None:
        self.port = port
        self.pid = pid
        self.instructions = instructions
        super().__init__(instructions)


# ---------------------------------------------------------------------------
# Port and process helpers
# ---------------------------------------------------------------------------


def probe_port(port: int) -> str | None:
    """Check whether ``port`` is in use on localhost.

    Returns the PID of the listener (as a string) if occupied, or ``None``
    if the port is free. Falls back to ``ss``/``lsof`` for PID lookup.
    """
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        result = sock.connect_ex(("127.0.0.1", port))
        if result != 0:
            return None
    finally:
        sock.close()

    # Port is occupied — try to find the PID.
    for cmd in (
        ["lsof", "-ti", f":{port}"],
        ["ss", "-tlnp", f"sport = :{port}"],
    ):
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            pid_str = out.stdout.strip().split("\n")[0].strip()
            if pid_str:
                # ss output may need parsing: extract pid=XXXX
                m = re.search(r"pid=(\d+)", pid_str)
                if m:
                    return m.group(1)
                if pid_str.isdigit():
                    return pid_str
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
    return "unknown"


def check_port_available(
    bolt_port: int = NEO4J_DEFAULT_BOLT_PORT,
    http_port: int = NEO4J_DEFAULT_HTTP_PORT,
) -> None:
    """Verify that the Bolt and HTTP ports are free.

    Raises :class:`PortConflictError` with an actionable message if either
    port is occupied.
    """
    for port, label in [(bolt_port, "Bolt"), (http_port, "HTTP")]:
        pid = probe_port(port)
        if pid is not None:
            alt_bolt = bolt_port + 1 if label == "Bolt" else bolt_port
            alt_http = http_port + 1 if label == "HTTP" else http_port
            instructions = (
                f"{label} port {port} is already in use"
                + (f" (PID {pid})" if pid != "unknown" else "")
                + ".\n\nOptions:\n"
                f"  1. Stop the existing process:  kill {pid}\n"
                f"  2. Install on different ports:\n"
                f"     graphmana setup-neo4j --bolt-port {alt_bolt} "
                f"--http-port {alt_http} ...\n"
                f"  3. Adopt the running instance:\n"
                f"     graphmana setup-neo4j --adopt --install-dir <existing-neo4j-home> ..."
            )
            raise PortConflictError(port, pid, instructions)


def detect_running_neo4j() -> dict | None:
    """Detect a running Neo4j process on this machine.

    Returns ``{pid, cmdline}`` if found, else ``None``.
    """
    try:
        result = subprocess.run(
            ["pgrep", "-fa", "neo4j"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        for line in result.stdout.strip().split("\n"):
            if line and "java" in line.lower() and "neo4j" in line.lower():
                parts = line.split(None, 1)
                return {"pid": parts[0], "cmdline": parts[1] if len(parts) > 1 else ""}
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def validate_tarball_filename(path: str | Path) -> str:
    """Validate that a tarball filename matches the expected Neo4j 5.26.x pattern.

    Args:
        path: path to the tarball file.

    Returns:
        The extracted version string (e.g. ``"5.26.0"``).

    Raises:
        ValueError: if the filename does not match ``neo4j-community-5.26.\\d+-unix.tar.gz``.
    """
    name = Path(path).name
    m = _TARBALL_RE.match(name)
    if not m:
        raise ValueError(
            f"Tarball filename '{name}' does not match the expected pattern "
            f"'neo4j-community-5.26.X-unix.tar.gz'. "
            f"GraphMana requires Neo4j Community 5.26.x."
        )
    return m.group(1)


def set_neo4j_password(neo4j_home: str | Path, password: str) -> None:
    """Set the initial Neo4j password via ``neo4j-admin``.

    Handles the "password already set" case gracefully.
    """
    admin_bin = Path(neo4j_home) / "bin" / "neo4j-admin"
    if not admin_bin.exists():
        logger.warning("neo4j-admin not found at %s; skipping password setup.", admin_bin)
        return

    try:
        result = subprocess.run(
            [str(admin_bin), "dbms", "set-initial-password", password],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            logger.info("Neo4j initial password set.")
        else:
            combined = result.stderr + result.stdout
            if "already" in combined.lower():
                logger.info("Neo4j password already set (use Neo4j browser to change).")
            else:
                logger.warning("neo4j-admin password setup: %s", combined.strip())
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        logger.warning("Could not set Neo4j password: %s", exc)


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def setup_neo4j(
    install_dir: str | Path,
    *,
    version: str = NEO4J_DEFAULT_VERSION,
    data_dir: str | Path | None = None,
    memory_auto: bool = False,
    skip_download: bool = False,
    neo4j_tarball: str | Path | None = None,
    deploy_plugin: str | Path | None = None,
    bolt_port: int = NEO4J_DEFAULT_BOLT_PORT,
    http_port: int = NEO4J_DEFAULT_HTTP_PORT,
    password: str | None = None,
    skip_port_check: bool = False,
) -> dict:
    """Download and configure Neo4j Community for user-space operation.

    Args:
        install_dir: Directory to install Neo4j into.
        version: Neo4j version to download (ignored when *neo4j_tarball* is set).
        data_dir: Custom data directory path.
        memory_auto: Auto-set heap and page cache from available RAM.
        skip_download: Use existing Neo4j at *install_dir* without downloading.
        neo4j_tarball: Path to a pre-downloaded ``neo4j-community-5.26.x-unix.tar.gz``
            for offline install. The filename is validated against a strict pattern.
        deploy_plugin: Path to a user-provided ``graphmana-procedures.jar``. When
            set, the bundled JAR is not deployed.
        bolt_port: Bolt protocol port (written to ``neo4j.conf``).
        http_port: HTTP browser port (written to ``neo4j.conf``).
        password: Initial Neo4j password. Set via ``neo4j-admin``.

    Returns:
        Dict with neo4j_home, version, data_dir, java_version, bolt_port, http_port.

    Raises:
        RuntimeError: If Java 21+ is not available.
        PortConflictError: If the requested ports are already in use.
        ValueError: If *neo4j_tarball* has an invalid filename.
    """
    install_dir = Path(install_dir)
    install_dir.mkdir(parents=True, exist_ok=True)

    # ---- Port check (fail early, skipped on adopt path) ----
    if not skip_port_check:
        check_port_available(bolt_port, http_port)

    # ---- Java check ----
    java_version = check_java()

    # ---- Determine version from tarball if provided ----
    if neo4j_tarball is not None:
        tarball_path = Path(neo4j_tarball)
        if not tarball_path.exists():
            raise FileNotFoundError(f"Tarball not found: {tarball_path}")
        version = validate_tarball_filename(tarball_path)

    neo4j_home = install_dir / f"neo4j-community-{version}"

    # ---- Download / extract / skip ----
    if skip_download:
        if not neo4j_home.exists() or not (neo4j_home / "bin" / "neo4j").exists():
            raise FileNotFoundError(
                f"--skip-download: Neo4j not found at {neo4j_home}. "
                f"Install it first, or remove --skip-download."
            )
        logger.info("Using existing Neo4j at %s (--skip-download)", neo4j_home)
    elif neo4j_tarball is not None:
        if neo4j_home.exists():
            logger.info("Neo4j already extracted at %s", neo4j_home)
        else:
            logger.info("Extracting %s to %s ...", tarball_path.name, install_dir)
            with tarfile.open(tarball_path, "r:gz") as tar:
                tar.extractall(path=install_dir)
    elif neo4j_home.exists():
        logger.info("Neo4j already installed at %s", neo4j_home)
    else:
        tarball_url = NEO4J_DOWNLOAD_URL_TEMPLATE.format(version=version)
        tarball_path_dl = install_dir / f"neo4j-community-{version}-unix.tar.gz"
        if not tarball_path_dl.exists():
            logger.info("Downloading Neo4j %s ...", version)
            _download_file(tarball_url, tarball_path_dl)
        logger.info("Extracting to %s ...", install_dir)
        with tarfile.open(tarball_path_dl, "r:gz") as tar:
            tar.extractall(path=install_dir)
        tarball_path_dl.unlink(missing_ok=True)

    # ---- Configure ----
    conf_path = neo4j_home / "conf" / "neo4j.conf"
    actual_data_dir = Path(data_dir) if data_dir else neo4j_home / "data"

    if data_dir:
        actual_data_dir.mkdir(parents=True, exist_ok=True)
        _set_conf_value(conf_path, "server.directories.data", str(actual_data_dir))

    if memory_auto:
        heap, pagecache = auto_memory_config()
        _set_conf_value(conf_path, "server.memory.heap.initial_size", heap)
        _set_conf_value(conf_path, "server.memory.heap.max_size", heap)
        _set_conf_value(conf_path, "server.memory.pagecache.size", pagecache)
        logger.info("Memory auto-configured: heap=%s, pagecache=%s", heap, pagecache)

    # Port configuration (non-default)
    if bolt_port != NEO4J_DEFAULT_BOLT_PORT:
        _set_conf_value(conf_path, "server.bolt.listen_address", f":{bolt_port}")
    if http_port != NEO4J_DEFAULT_HTTP_PORT:
        _set_conf_value(conf_path, "server.http.listen_address", f":{http_port}")

    # Allow GraphMana procedures
    _set_conf_value(conf_path, "dbms.security.procedures.unrestricted", "graphmana.*")

    # Make scripts executable
    bin_dir = neo4j_home / "bin"
    for script in bin_dir.glob("*"):
        if script.is_file() and not script.suffix:
            script.chmod(script.stat().st_mode | 0o755)

    # ---- Plugin deployment ----
    if deploy_plugin:
        dest = neo4j_home / "plugins" / "graphmana-procedures.jar"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(deploy_plugin, dest)
        logger.info("User-provided plugin deployed to %s", dest)
    else:
        _deploy_procedures_jar(neo4j_home)

    # ---- Password ----
    if password:
        set_neo4j_password(neo4j_home, password)

    logger.info("Neo4j %s ready at %s", version, neo4j_home)

    return {
        "neo4j_home": str(neo4j_home),
        "version": version,
        "data_dir": str(actual_data_dir),
        "java_version": java_version,
        "bolt_port": bolt_port,
        "http_port": http_port,
    }


def start_neo4j(
    neo4j_home: str | Path,
    *,
    data_dir: str | Path | None = None,
    wait: bool = True,
    timeout: int = 120,
) -> dict:
    """Start a Neo4j instance in user space.

    Args:
        neo4j_home: Neo4j installation directory.
        data_dir: Override data directory (sets env var before start).
        wait: If True, block until Neo4j is accepting connections.
        timeout: Maximum seconds to wait for readiness.

    Returns:
        Dict with neo4j_home, pid_file, status, bolt_port.

    Raises:
        RuntimeError: If Neo4j fails to start or times out.
    """
    neo4j_home = Path(neo4j_home)
    neo4j_bin = neo4j_home / "bin" / "neo4j"

    if not neo4j_bin.exists():
        raise FileNotFoundError(f"Neo4j binary not found: {neo4j_bin}")

    env = os.environ.copy()
    if data_dir:
        env["NEO4J_DATA"] = str(Path(data_dir).resolve())

    # Start Neo4j as a console process in background
    cmd = [str(neo4j_bin), "start"]
    logger.info("Starting Neo4j: %s", " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True, env=env)

    if result.returncode != 0:
        raise RuntimeError(
            f"Neo4j start failed (exit {result.returncode}): " f"{result.stderr.strip()}"
        )

    logger.info("Neo4j start command completed")

    # Detect PID file
    pid_file = neo4j_home / "run" / "neo4j.pid"
    pid = None
    if pid_file.exists():
        pid = pid_file.read_text().strip()

    status = "started"
    if wait:
        status = _wait_for_neo4j(neo4j_home, timeout=timeout)

    return {
        "neo4j_home": str(neo4j_home),
        "pid": pid,
        "status": status,
        "bolt_port": NEO4J_DEFAULT_BOLT_PORT,
    }


def stop_neo4j(neo4j_home: str | Path) -> dict:
    """Stop a running Neo4j instance.

    Args:
        neo4j_home: Neo4j installation directory.

    Returns:
        Dict with neo4j_home, status.

    Raises:
        RuntimeError: If stop command fails.
    """
    neo4j_home = Path(neo4j_home)
    neo4j_bin = neo4j_home / "bin" / "neo4j"

    if not neo4j_bin.exists():
        raise FileNotFoundError(f"Neo4j binary not found: {neo4j_bin}")

    cmd = [str(neo4j_bin), "stop"]
    logger.info("Stopping Neo4j: %s", " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(
            f"Neo4j stop failed (exit {result.returncode}): " f"{result.stderr.strip()}"
        )

    logger.info("Neo4j stopped")
    return {
        "neo4j_home": str(neo4j_home),
        "status": "stopped",
    }


def _detect_available_memory_gb() -> float:
    """Detect available memory in GB, respecting cgroup limits.

    Checks (in order):
    1. cgroup v2 memory.max
    2. cgroup v1 memory.limit_in_bytes
    3. /proc/meminfo (Linux)
    4. sysctl hw.memsize (macOS)
    5. Fallback: 16 GB
    """
    # cgroup v2
    cg2 = Path("/sys/fs/cgroup/memory.max")
    if cg2.exists():
        try:
            val = cg2.read_text().strip()
            if val != "max":
                return int(val) / (1024**3)
        except (ValueError, OSError):
            pass

    # cgroup v1
    cg1 = Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    if cg1.exists():
        try:
            limit_bytes = int(cg1.read_text().strip())
            # cgroup v1 uses a very large number to indicate "no limit"
            if limit_bytes < 2**60:
                return limit_bytes / (1024**3)
        except (ValueError, OSError):
            pass

    # /proc/meminfo (Linux)
    try:
        meminfo = Path("/proc/meminfo").read_text()
        for line in meminfo.split("\n"):
            if line.startswith("MemTotal:"):
                kb = int(line.split()[1])
                return kb / (1024 * 1024)
    except (FileNotFoundError, OSError):
        pass

    # macOS
    try:
        result = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return int(result.stdout.strip()) / (1024**3)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    return 16.0  # fallback


def auto_memory_config() -> tuple[str, str]:
    """Calculate heap and page cache sizes based on available RAM.

    Allocation strategy:
    - 50% of available RAM to page cache (for graph data)
    - 25% of available RAM to JVM heap (for query processing)
    - 25% reserved for OS and other processes

    Returns:
        Tuple of (heap_size, pagecache_size) as strings like "4g", "8g".
    """
    total_gb = _detect_available_memory_gb()

    heap_gb = max(1, int(total_gb * 0.25))
    pagecache_gb = max(1, int(total_gb * 0.50))

    return f"{heap_gb}g", f"{pagecache_gb}g"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def check_java() -> str:
    """Check that Java 21+ is available.

    Returns the java version string.

    Raises:
        RuntimeError: If java is not found or version is too old.
    """
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except FileNotFoundError:
        raise RuntimeError(
            "Java not found on PATH. Neo4j requires Java 21+. "
            "On HPC clusters, try: module load java/21"
        )

    # Java version can appear in stdout or stderr
    output = result.stderr + result.stdout
    match = re.search(r'"(\d+)[\.\d]*"', output)
    if not match:
        # Try alternate format: openjdk 21.0.1
        match = re.search(r"(?:openjdk|java)\s+(\d+)", output, re.IGNORECASE)

    if match:
        major = int(match.group(1))
        if major < 21:
            raise RuntimeError(
                f"Java {major} found, but Neo4j requires Java 21+. "
                f"On HPC clusters, try: module load java/21"
            )
        return output.strip().split("\n")[0]

    # Could not parse but java exists — warn and continue
    logger.warning("Could not parse Java version from: %s", output[:200])
    return "unknown"


def _download_file(url: str, dest: Path) -> None:
    """Download a file via curl or urllib."""
    # Try curl first (more likely on HPC)
    curl = shutil.which("curl")
    if curl:
        cmd = [curl, "-L", "-o", str(dest), url]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return
        logger.warning("curl failed, falling back to urllib")

    # Fallback to urllib
    import urllib.request

    urllib.request.urlretrieve(url, dest)


def _set_conf_value(conf_path: Path, key: str, value: str) -> None:
    """Set or update a configuration value in neo4j.conf."""
    if not conf_path.exists():
        conf_path.parent.mkdir(parents=True, exist_ok=True)
        conf_path.write_text(f"{key}={value}\n")
        return

    text = conf_path.read_text()
    # Match both commented and uncommented versions
    pattern = re.compile(rf"^#?\s*{re.escape(key)}\s*=.*$", re.MULTILINE)

    if pattern.search(text):
        text = pattern.sub(f"{key}={value}", text)
    else:
        text += f"\n{key}={value}\n"

    conf_path.write_text(text)


def _wait_for_neo4j(neo4j_home: Path, *, timeout: int = 120) -> str:
    """Wait for Neo4j to be ready by checking its status.

    Returns 'ready' if Neo4j becomes available, raises RuntimeError
    on timeout.
    """
    neo4j_bin = neo4j_home / "bin" / "neo4j"
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        result = subprocess.run(
            [str(neo4j_bin), "status"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and "running" in result.stdout.lower():
            logger.info("Neo4j is ready")
            return "ready"
        time.sleep(2)

    raise RuntimeError(
        f"Neo4j did not become ready within {timeout}s. " f"Check logs at {neo4j_home / 'logs'}"
    )


# ---------------------------------------------------------------------------
# JAR deployment
# ---------------------------------------------------------------------------


def _deploy_procedures_jar(neo4j_home: Path) -> None:
    """Copy the bundled GraphMana procedures JAR to Neo4j plugins directory."""
    try:
        from graphmana.data import get_procedures_jar

        jar_src = get_procedures_jar()
    except (ImportError, FileNotFoundError):
        logger.info("No bundled procedures JAR found; skipping plugin deployment")
        return

    plugins_dir = neo4j_home / "plugins"
    plugins_dir.mkdir(parents=True, exist_ok=True)
    jar_dest = plugins_dir / "graphmana-procedures.jar"

    if jar_dest.exists():
        # Check if the bundled version is newer
        if jar_src.stat().st_size == jar_dest.stat().st_size:
            logger.info("Procedures JAR already deployed (same size)")
            return

    shutil.copy2(jar_src, jar_dest)
    logger.info("Deployed procedures JAR to %s", jar_dest)


# ---------------------------------------------------------------------------
# Java download (user-space, no admin required)
# ---------------------------------------------------------------------------

# Eclipse Temurin JDK 21 download URLs
_TEMURIN_URLS = {
    "linux-x64": "https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jdk/hotspot/normal/eclipse",
    "linux-aarch64": "https://api.adoptium.net/v3/binary/latest/21/ga/linux/aarch64/jdk/hotspot/normal/eclipse",
    "mac-x64": "https://api.adoptium.net/v3/binary/latest/21/ga/mac/x64/jdk/hotspot/normal/eclipse",
    "mac-aarch64": "https://api.adoptium.net/v3/binary/latest/21/ga/mac/aarch64/jdk/hotspot/normal/eclipse",
}


def _detect_platform() -> str:
    """Detect platform as 'os-arch' for Temurin download."""
    import platform as plat

    system = plat.system().lower()
    machine = plat.machine().lower()

    if system == "darwin":
        system = "mac"

    if machine in ("x86_64", "amd64"):
        arch = "x64"
    elif machine in ("aarch64", "arm64"):
        arch = "aarch64"
    else:
        arch = machine

    return f"{system}-{arch}"


def download_java(install_dir: str | Path) -> Path:
    """Download Eclipse Temurin JDK 21 to user space (no admin required).

    Args:
        install_dir: Directory where the JDK will be extracted.

    Returns:
        Path to the java binary.

    Raises:
        RuntimeError: If platform is not supported or download fails.
    """
    install_dir = Path(install_dir)
    install_dir.mkdir(parents=True, exist_ok=True)

    platform_key = _detect_platform()
    if platform_key not in _TEMURIN_URLS:
        raise RuntimeError(
            f"Unsupported platform '{platform_key}'. "
            f"Supported: {', '.join(_TEMURIN_URLS.keys())}"
        )

    # Check if already downloaded
    existing_jdks = list(install_dir.glob("jdk-21*"))
    if existing_jdks:
        java_bin = existing_jdks[0] / "bin" / "java"
        if java_bin.exists():
            logger.info("JDK already present at %s", existing_jdks[0])
            return java_bin

    url = _TEMURIN_URLS[platform_key]
    tarball_path = install_dir / "temurin-jdk21.tar.gz"

    logger.info("Downloading Eclipse Temurin JDK 21 for %s ...", platform_key)
    _download_file(url, tarball_path)

    logger.info("Extracting JDK to %s ...", install_dir)
    with tarfile.open(tarball_path, "r:gz") as tar:
        tar.extractall(path=install_dir)

    tarball_path.unlink(missing_ok=True)

    # Find the extracted directory
    jdk_dirs = list(install_dir.glob("jdk-21*"))
    if not jdk_dirs:
        raise RuntimeError("JDK extraction failed — no jdk-21* directory found")

    java_bin = jdk_dirs[0] / "bin" / "java"
    if not java_bin.exists():
        raise RuntimeError(f"Java binary not found at {java_bin}")

    # Make executable
    java_bin.chmod(java_bin.stat().st_mode | 0o755)

    logger.info("JDK 21 installed at %s", jdk_dirs[0])
    return java_bin
