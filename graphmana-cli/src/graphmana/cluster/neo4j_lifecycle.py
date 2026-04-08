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
NEO4J_DEFAULT_VERSION = "5.26.2"


def setup_neo4j(
    install_dir: str | Path,
    *,
    version: str = NEO4J_DEFAULT_VERSION,
    data_dir: str | Path | None = None,
    memory_auto: bool = False,
) -> dict:
    """Download and configure Neo4j Community for user-space operation.

    Args:
        install_dir: Directory to install Neo4j into. A ``neo4j-community-*``
            subdirectory will be created.
        version: Neo4j version to download.
        data_dir: Custom data directory path. If provided, neo4j.conf is
            updated to use this path.
        memory_auto: If True, auto-set heap and page cache based on
            available RAM.

    Returns:
        Dict with neo4j_home, version, data_dir, java_version.

    Raises:
        RuntimeError: If Java 21+ is not available.
    """
    install_dir = Path(install_dir)
    install_dir.mkdir(parents=True, exist_ok=True)

    # Check Java
    java_version = _check_java()

    neo4j_home = install_dir / f"neo4j-community-{version}"

    if neo4j_home.exists():
        logger.info("Neo4j already installed at %s", neo4j_home)
    else:
        # Download and extract
        tarball_url = NEO4J_DOWNLOAD_URL_TEMPLATE.format(version=version)
        tarball_path = install_dir / f"neo4j-community-{version}-unix.tar.gz"

        if not tarball_path.exists():
            logger.info("Downloading Neo4j %s ...", version)
            _download_file(tarball_url, tarball_path)

        logger.info("Extracting to %s ...", install_dir)
        with tarfile.open(tarball_path, "r:gz") as tar:
            tar.extractall(path=install_dir)

        # Clean up tarball
        tarball_path.unlink(missing_ok=True)

    # Configure
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

    # Make scripts executable
    bin_dir = neo4j_home / "bin"
    for script in bin_dir.glob("*"):
        if script.is_file() and not script.suffix:
            script.chmod(script.stat().st_mode | 0o755)

    # Deploy bundled GraphMana procedures JAR to plugins/
    _deploy_procedures_jar(neo4j_home)

    logger.info("Neo4j %s ready at %s", version, neo4j_home)

    return {
        "neo4j_home": str(neo4j_home),
        "version": version,
        "data_dir": str(actual_data_dir),
        "java_version": java_version,
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


def _check_java() -> str:
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
