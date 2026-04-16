"""Tests for Neo4j setup/lifecycle Phase 1+2 improvements.

Covers: port probing, tarball validation, config file management,
PortConflictError, and doctor diagnostic stubs.
"""

from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from graphmana.cluster.neo4j_lifecycle import (
    PortConflictError,
    check_port_available,
    probe_port,
    validate_tarball_filename,
)
from graphmana.config_file import (
    get_config_value,
)

# ---------------------------------------------------------------------------
# Port probing
# ---------------------------------------------------------------------------


class TestProbePort:
    def test_free_port_returns_none(self):
        with mock.patch("socket.socket") as mock_sock:
            inst = mock_sock.return_value.__enter__.return_value
            inst.connect_ex.return_value = 1  # connection refused → port is free
            # probe_port creates its own socket, so mock differently
            pass
        # Use a high ephemeral port that is almost certainly free
        result = probe_port(59123)
        assert result is None

    def test_check_port_available_raises_on_occupied(self):
        with mock.patch(
            "graphmana.cluster.neo4j_lifecycle.probe_port", return_value="12345"
        ):
            with pytest.raises(PortConflictError) as exc_info:
                check_port_available(7687, 7474)
            assert exc_info.value.port == 7687
            assert "12345" in exc_info.value.instructions
            assert "--bolt-port" in exc_info.value.instructions

    def test_check_port_available_passes_when_free(self):
        with mock.patch(
            "graphmana.cluster.neo4j_lifecycle.probe_port", return_value=None
        ):
            check_port_available(7687, 7474)


# ---------------------------------------------------------------------------
# Tarball filename validation
# ---------------------------------------------------------------------------


class TestValidateTarball:
    def test_valid_5_26_0(self):
        assert validate_tarball_filename("neo4j-community-5.26.0-unix.tar.gz") == "5.26.0"

    def test_valid_5_26_2(self):
        assert validate_tarball_filename("neo4j-community-5.26.2-unix.tar.gz") == "5.26.2"

    def test_valid_path_object(self):
        assert (
            validate_tarball_filename(Path("/tmp/neo4j-community-5.26.0-unix.tar.gz"))
            == "5.26.0"
        )

    def test_wrong_major_version_raises(self):
        with pytest.raises(ValueError, match="does not match"):
            validate_tarball_filename("neo4j-community-4.4.0-unix.tar.gz")

    def test_enterprise_raises(self):
        with pytest.raises(ValueError, match="does not match"):
            validate_tarball_filename("neo4j-enterprise-5.26.0-unix.tar.gz")

    def test_zip_raises(self):
        with pytest.raises(ValueError, match="does not match"):
            validate_tarball_filename("neo4j-community-5.26.0-unix.zip")

    def test_windows_raises(self):
        with pytest.raises(ValueError, match="does not match"):
            validate_tarball_filename("neo4j-community-5.26.0-windows.zip")


# ---------------------------------------------------------------------------
# Config file management
# ---------------------------------------------------------------------------


class TestConfigFile:
    def _with_temp_home(self):
        """Patch config paths to a temp directory."""
        import graphmana.config_file as cf

        tmpdir = tempfile.mkdtemp()
        cf.CONFIG_DIR = Path(tmpdir) / ".graphmana"
        cf.CONFIG_PATH = cf.CONFIG_DIR / "config.yaml"
        return cf

    def test_save_load_roundtrip(self):
        cf = self._with_temp_home()
        original = {
            "neo4j_home": "/tmp/neo4j/neo4j-community-5.26.2",
            "uri": "bolt://localhost:7688",
            "user": "neo4j",
            "password": "s3cret",
            "database": "neo4j",
            "bolt_port": 7688,
            "http_port": 7475,
        }
        cf.save_config(original)
        loaded = cf.load_config()
        assert loaded is not None
        for k, v in original.items():
            assert loaded[k] == v

    def test_file_permissions_0600(self):
        cf = self._with_temp_home()
        cf.save_config({"password": "test"})
        mode = cf.CONFIG_PATH.stat().st_mode
        assert stat.S_IMODE(mode) == 0o600

    def test_load_missing_returns_none(self):
        cf = self._with_temp_home()
        assert cf.load_config() is None

    def test_none_values_excluded(self):
        cf = self._with_temp_home()
        cf.save_config({"neo4j_home": "/tmp", "data_dir": None})
        loaded = cf.load_config()
        assert "data_dir" not in loaded
        assert loaded["neo4j_home"] == "/tmp"


class TestGetConfigValue:
    def _with_temp_config(self, cfg: dict):
        import graphmana.config_file as cf

        tmpdir = tempfile.mkdtemp()
        cf.CONFIG_DIR = Path(tmpdir) / ".graphmana"
        cf.CONFIG_PATH = cf.CONFIG_DIR / "config.yaml"
        cf.save_config(cfg)
        return cf

    def test_cli_value_wins(self):
        self._with_temp_config({"neo4j_home": "/from/config"})
        assert get_config_value("neo4j_home", cli_value="/from/cli") == "/from/cli"

    def test_config_file_wins_over_env(self):
        self._with_temp_config({"neo4j_home": "/from/config"})
        with mock.patch.dict(os.environ, {"GRAPHMANA_NEO4J_HOME": "/from/env"}):
            assert (
                get_config_value("neo4j_home", env_var="GRAPHMANA_NEO4J_HOME")
                == "/from/config"
            )

    def test_env_wins_over_default(self):
        import graphmana.config_file as cf

        tmpdir = tempfile.mkdtemp()
        cf.CONFIG_DIR = Path(tmpdir) / ".graphmana"
        cf.CONFIG_PATH = cf.CONFIG_DIR / "config.yaml"
        with mock.patch.dict(os.environ, {"GRAPHMANA_NEO4J_HOME": "/from/env"}):
            assert (
                get_config_value(
                    "neo4j_home",
                    env_var="GRAPHMANA_NEO4J_HOME",
                    default="/default",
                )
                == "/from/env"
            )

    def test_default_when_nothing_set(self):
        import graphmana.config_file as cf

        tmpdir = tempfile.mkdtemp()
        cf.CONFIG_DIR = Path(tmpdir) / ".graphmana"
        cf.CONFIG_PATH = cf.CONFIG_DIR / "config.yaml"
        assert get_config_value("missing", default="fallback") == "fallback"
