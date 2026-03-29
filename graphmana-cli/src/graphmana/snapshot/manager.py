"""Snapshot management — create, list, restore, and delete database snapshots.

Wraps ``neo4j-admin database dump`` and ``neo4j-admin database load`` to
provide named snapshots stored as ``.dump`` files in a snapshot directory.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from graphmana.config import DEFAULT_DATABASE

logger = logging.getLogger(__name__)

DEFAULT_SNAPSHOT_DIR = "graphmana_snapshots"


class SnapshotManager:
    """Manage named database snapshots via neo4j-admin dump/load.

    Snapshots are stored as ``<name>.dump`` files in the snapshot directory.

    Args:
        snapshot_dir: Directory where snapshot files are stored.
            Created automatically if it doesn't exist.
    """

    def __init__(self, snapshot_dir: str | Path) -> None:
        self._snapshot_dir = Path(snapshot_dir)
        self._snapshot_dir.mkdir(parents=True, exist_ok=True)

    @property
    def snapshot_dir(self) -> Path:
        return self._snapshot_dir

    def create(
        self,
        name: str,
        *,
        neo4j_home: str | Path,
        database: str = DEFAULT_DATABASE,
    ) -> dict:
        """Create a snapshot by dumping the database.

        Runs ``neo4j-admin database dump <database> --to-path=<snapshot_dir>``
        then renames the output to ``<name>.dump``.

        Args:
            name: Unique snapshot name.
            neo4j_home: Neo4j installation directory.
            database: Neo4j database name.

        Returns:
            Dict with name, path, size_bytes, created_date.

        Raises:
            ValueError: If snapshot name already exists.
            RuntimeError: If neo4j-admin dump fails.
        """
        _validate_name(name)
        dump_path = self._snapshot_dir / f"{name}.dump"
        if dump_path.exists():
            raise ValueError(f"Snapshot already exists: {name!r}")

        neo4j_admin = _find_neo4j_admin(neo4j_home)

        # neo4j-admin database dump writes to --to-path directory
        # as <database>.dump
        cmd = [
            str(neo4j_admin),
            "database",
            "dump",
            database,
            f"--to-path={self._snapshot_dir}",
            "--overwrite-destination=true",
        ]

        logger.info("Creating snapshot %r: %s", name, " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise RuntimeError(
                f"neo4j-admin dump failed (exit {result.returncode}): {result.stderr.strip()}"
            )

        # Rename default output to snapshot name
        default_dump = self._snapshot_dir / f"{database}.dump"
        if default_dump.exists() and default_dump != dump_path:
            default_dump.rename(dump_path)

        created = datetime.now(timezone.utc).isoformat()
        size = dump_path.stat().st_size if dump_path.exists() else 0

        logger.info("Snapshot created: %s (%d bytes)", name, size)

        return {
            "name": name,
            "path": str(dump_path),
            "size_bytes": size,
            "created_date": created,
        }

    def list(self) -> list[dict]:
        """List all snapshots in the snapshot directory.

        Returns:
            List of dicts with name, path, size_bytes, modified_date.
        """
        snapshots: list[dict] = []
        for p in sorted(self._snapshot_dir.glob("*.dump")):
            stat = p.stat()
            snapshots.append(
                {
                    "name": p.stem,
                    "path": str(p),
                    "size_bytes": stat.st_size,
                    "modified_date": datetime.fromtimestamp(
                        stat.st_mtime, tz=timezone.utc
                    ).isoformat(),
                }
            )
        return snapshots

    def get(self, name: str) -> dict | None:
        """Get info about a specific snapshot.

        Returns:
            Dict with name, path, size_bytes, modified_date, or None.
        """
        dump_path = self._snapshot_dir / f"{name}.dump"
        if not dump_path.exists():
            return None
        stat = dump_path.stat()
        return {
            "name": name,
            "path": str(dump_path),
            "size_bytes": stat.st_size,
            "modified_date": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        }

    def restore(
        self,
        name: str,
        *,
        neo4j_home: str | Path,
        database: str = DEFAULT_DATABASE,
    ) -> dict:
        """Restore a database from a named snapshot.

        Runs ``neo4j-admin database load <database> --from-path=<snapshot_dir>``
        after renaming the snapshot to the expected filename.

        **WARNING**: This overwrites the current database. Neo4j must be stopped.

        Args:
            name: Snapshot name to restore.
            neo4j_home: Neo4j installation directory.
            database: Neo4j database name.

        Returns:
            Dict with name, database, status.

        Raises:
            ValueError: If snapshot not found.
            RuntimeError: If neo4j-admin load fails.
        """
        dump_path = self._snapshot_dir / f"{name}.dump"
        if not dump_path.exists():
            raise ValueError(f"Snapshot not found: {name!r}")

        neo4j_admin = _find_neo4j_admin(neo4j_home)

        # Verify Neo4j is stopped — neo4j-admin load fails on a running instance
        if _is_neo4j_running(neo4j_home):
            raise RuntimeError(
                "Neo4j appears to be running. Stop Neo4j before restoring a snapshot "
                "(graphmana neo4j-stop or neo4j stop)."
            )

        # neo4j-admin load expects <database>.dump in the --from-path dir
        # Temporarily rename if needed
        expected_name = f"{database}.dump"
        expected_path = self._snapshot_dir / expected_name
        renamed = False
        if dump_path.name != expected_name:
            if expected_path.exists():
                # Move existing file out of the way temporarily
                backup_path = expected_path.with_suffix(".dump.bak")
                expected_path.rename(backup_path)
            shutil.copy2(dump_path, expected_path)
            renamed = True

        try:
            cmd = [
                str(neo4j_admin),
                "database",
                "load",
                database,
                f"--from-path={self._snapshot_dir}",
                "--overwrite-destination=true",
            ]

            logger.info("Restoring snapshot %r: %s", name, " ".join(cmd))
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode != 0:
                raise RuntimeError(
                    f"neo4j-admin load failed (exit {result.returncode}): "
                    f"{result.stderr.strip()}"
                )
        finally:
            # Clean up temporary copy
            if renamed and expected_path.exists():
                expected_path.unlink()
                backup_path = expected_path.with_suffix(".dump.bak")
                if backup_path.exists():
                    backup_path.rename(expected_path)

        logger.info("Snapshot restored: %s -> database %s", name, database)

        return {
            "name": name,
            "database": database,
            "status": "restored",
        }

    def delete(self, name: str) -> bool:
        """Delete a named snapshot.

        Args:
            name: Snapshot name.

        Returns:
            True if deleted, False if not found.
        """
        dump_path = self._snapshot_dir / f"{name}.dump"
        if not dump_path.exists():
            return False
        dump_path.unlink()
        logger.info("Snapshot deleted: %s", name)
        return True


def _find_neo4j_admin(neo4j_home: str | Path) -> Path:
    """Locate the neo4j-admin binary.

    Args:
        neo4j_home: Neo4j installation directory.

    Returns:
        Path to neo4j-admin binary.

    Raises:
        FileNotFoundError: If neo4j-admin not found.
    """
    neo4j_home = Path(neo4j_home)
    candidates = [
        neo4j_home / "bin" / "neo4j-admin",
        neo4j_home / "bin" / "neo4j-admin.bat",
    ]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError(
        f"neo4j-admin not found in {neo4j_home / 'bin'}. "
        f"Checked: {', '.join(str(c) for c in candidates)}"
    )


def _is_neo4j_running(neo4j_home: str | Path) -> bool:
    """Check if Neo4j is currently running by inspecting the PID file.

    Returns True if a PID file exists and the process is alive.
    """
    neo4j_home = Path(neo4j_home)
    pid_file = neo4j_home / "run" / "neo4j.pid"
    if not pid_file.exists():
        return False
    try:
        pid = int(pid_file.read_text().strip())
        # Check if process is alive (signal 0 doesn't kill)
        import os

        os.kill(pid, 0)
        return True
    except (ValueError, ProcessLookupError, PermissionError, OSError):
        return False


def _validate_name(name: str) -> None:
    """Validate snapshot name for filesystem safety."""
    if not name:
        raise ValueError("Snapshot name cannot be empty.")
    if "/" in name or "\\" in name or ".." in name:
        raise ValueError(f"Invalid snapshot name: {name!r}")
    if name.startswith("."):
        raise ValueError(f"Snapshot name cannot start with '.': {name!r}")
