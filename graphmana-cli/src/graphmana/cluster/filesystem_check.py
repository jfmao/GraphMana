"""Filesystem type detection — warn users about network filesystems.

Neo4j performs extremely poorly on NFS/Lustre/GPFS due to random I/O
patterns. This module detects the filesystem type of a given path and
warns if it is not local storage.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

# Filesystem types known to cause poor Neo4j performance
NETWORK_FS_TYPES = frozenset(
    {
        "nfs",
        "nfs4",
        "cifs",
        "smb",
        "smbfs",
        "lustre",
        "gpfs",
        "beegfs",
        "afs",
        "fuse.sshfs",
        "glusterfs",
        "ceph",
        "fuse.ceph",
        "panfs",
    }
)

# Filesystem types known to be fine for Neo4j
LOCAL_FS_TYPES = frozenset(
    {
        "ext4",
        "ext3",
        "xfs",
        "btrfs",
        "zfs",
        "ntfs",
        "tmpfs",
        "overlay",
        "apfs",
    }
)


def detect_filesystem_type(path: str | Path) -> str | None:
    """Detect the filesystem type of the given path.

    Uses ``df -T`` (Linux) or ``mount`` as fallback. Returns the
    filesystem type string (e.g. "ext4", "nfs4") or None if detection
    fails.
    """
    path = Path(path).resolve()

    # Walk up to find an existing ancestor if path doesn't exist yet
    check_path = path
    while not check_path.exists() and check_path.parent != check_path:
        check_path = check_path.parent

    # Try df -T (Linux, most reliable)
    try:
        result = subprocess.run(
            ["df", "-T", str(check_path)],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            lines = result.stdout.strip().split("\n")
            if len(lines) >= 2:
                # Header: Filesystem Type ...
                # Data:   /dev/sda1   ext4 ...
                parts = lines[-1].split()
                if len(parts) >= 2:
                    return parts[1].lower()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # Fallback: parse /proc/mounts (Linux only)
    try:
        mounts_text = Path("/proc/mounts").read_text()
        best_match = ""
        best_type = None
        str_path = str(check_path)
        for line in mounts_text.strip().split("\n"):
            parts = line.split()
            if len(parts) >= 3:
                mount_point = parts[1]
                fs_type = parts[2]
                if str_path.startswith(mount_point) and len(mount_point) > len(best_match):
                    best_match = mount_point
                    best_type = fs_type
        if best_type:
            return best_type.lower()
    except (FileNotFoundError, PermissionError):
        pass

    return None


def is_network_filesystem(fs_type: str | None) -> bool:
    """Return True if the filesystem type is a known network filesystem."""
    if fs_type is None:
        return False
    return fs_type.lower() in NETWORK_FS_TYPES


def check_neo4j_data_dir(path: str | Path) -> dict:
    """Check if a Neo4j data directory is on a suitable filesystem.

    Returns a dict with:
        path: resolved path
        fs_type: detected filesystem type (or "unknown")
        is_network: True if on a network filesystem
        warning: warning message if problematic, else None
    """
    path = Path(path).resolve()
    fs_type = detect_filesystem_type(path)

    is_network = is_network_filesystem(fs_type)
    warning = None

    if is_network:
        warning = (
            f"Neo4j data directory {path} is on a network filesystem ({fs_type}). "
            f"Neo4j performs extremely poorly on NFS/Lustre/GPFS due to random I/O "
            f"patterns. Move the data directory to local SSD or node-local scratch "
            f"(e.g. /scratch, /tmp/local, /local) for acceptable performance."
        )
        logger.warning(warning)
    elif fs_type is None:
        logger.info(
            "Could not detect filesystem type for %s. "
            "Ensure it is on local storage (not NFS/Lustre/GPFS).",
            path,
        )

    return {
        "path": str(path),
        "fs_type": fs_type or "unknown",
        "is_network": is_network,
        "warning": warning,
    }
