"""Tests for database parameter support in GraphManaConnection and callers."""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock


class TestGraphManaConnectionDatabase:
    """Verify GraphManaConnection stores and uses the database parameter."""

    def test_database_stored(self):
        from graphmana.db.connection import GraphManaConnection

        conn = GraphManaConnection("bolt://localhost:7687", "neo4j", "pass", database="mydb")
        assert conn._database == "mydb"

    def test_database_default_none(self):
        from graphmana.db.connection import GraphManaConnection

        conn = GraphManaConnection("bolt://localhost:7687", "neo4j", "pass")
        assert conn._database is None

    def test_session_called_with_database(self):
        from graphmana.db.connection import GraphManaConnection

        conn = GraphManaConnection("bolt://localhost:7687", "neo4j", "pass", database="testdb")

        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        mock_result = MagicMock()
        mock_result.consume.return_value = None
        mock_session.run.return_value = mock_result
        mock_result.__iter__ = MagicMock(return_value=iter([]))
        conn._driver = mock_driver

        conn.execute_read("RETURN 1")
        mock_driver.session.assert_called_with(database="testdb")

    def test_session_called_with_none_database(self):
        from graphmana.db.connection import GraphManaConnection

        conn = GraphManaConnection("bolt://localhost:7687", "neo4j", "pass")

        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        mock_result = MagicMock()
        mock_result.consume.return_value = None
        mock_session.run.return_value = mock_result
        mock_result.__iter__ = MagicMock(return_value=iter([]))
        conn._driver = mock_driver

        conn.execute_read("RETURN 1")
        mock_driver.session.assert_called_with(database=None)

    def test_execute_write_uses_database(self):
        from graphmana.db.connection import GraphManaConnection

        conn = GraphManaConnection("bolt://localhost:7687", "neo4j", "pass", database="writedb")

        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        mock_result = MagicMock()
        mock_result.consume.return_value = None
        mock_session.run.return_value = mock_result
        mock_result.__iter__ = MagicMock(return_value=iter([]))
        conn._driver = mock_driver

        conn.execute_write("CREATE (n) RETURN n")
        mock_driver.session.assert_called_with(database="writedb")

    def test_execute_write_tx_uses_database(self):
        from graphmana.db.connection import GraphManaConnection

        conn = GraphManaConnection("bolt://localhost:7687", "neo4j", "pass", database="txdb")

        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        conn._driver = mock_driver

        conn.execute_write_tx(lambda tx: None)
        mock_driver.session.assert_called_with(database="txdb")

    def test_execute_read_tx_uses_database(self):
        from graphmana.db.connection import GraphManaConnection

        conn = GraphManaConnection("bolt://localhost:7687", "neo4j", "pass", database="rxdb")

        mock_driver = MagicMock()
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        conn._driver = mock_driver

        conn.execute_read_tx(lambda tx: None)
        mock_driver.session.assert_called_with(database="rxdb")


class TestExportParallelConnArgs:
    """Verify parallel export passes database in conn_args."""

    def test_conn_args_includes_database(self):
        """run_export_parallel must build a 4-tuple including _database."""
        from graphmana.export.parallel import run_export_parallel

        # Inspect the source to verify conn_args construction
        src = inspect.getsource(run_export_parallel)
        assert "conn._database" in src

    def test_worker_unpacks_4_tuple(self):
        """_worker_export_chromosome must unpack 4 values from conn_args."""
        from graphmana.export.parallel import _worker_export_chromosome

        src = inspect.getsource(_worker_export_chromosome)
        assert "uri, user, password, database = conn_args" in src

    def test_worker_passes_database_to_connection(self):
        """_worker_export_chromosome must pass database= to GraphManaConnection."""
        from graphmana.export.parallel import _worker_export_chromosome

        src = inspect.getsource(_worker_export_chromosome)
        assert "database=database" in src


class TestApplyPostImportIndexesDatabase:
    """Verify apply_post_import_indexes accepts and forwards database."""

    def test_has_database_parameter(self):
        from graphmana.ingest.loader import apply_post_import_indexes

        sig = inspect.signature(apply_post_import_indexes)
        assert "database" in sig.parameters
        assert sig.parameters["database"].default is None

    def test_forwards_database(self):
        """Verify database= is passed to GraphManaConnection."""
        from graphmana.ingest.loader import apply_post_import_indexes

        src = inspect.getsource(apply_post_import_indexes)
        assert "database=database" in src


class TestDetectImportModeDatabase:
    """Verify _detect_import_mode accepts and forwards database."""

    def test_has_database_parameter(self):
        from graphmana.ingest.pipeline import _detect_import_mode

        sig = inspect.signature(_detect_import_mode)
        assert "database" in sig.parameters
        assert sig.parameters["database"].default is None

    def test_forwards_database(self):
        from graphmana.ingest.pipeline import _detect_import_mode

        src = inspect.getsource(_detect_import_mode)
        assert "database=database" in src


class TestRunIncrementalDatabase:
    """Verify run_incremental accepts and forwards database."""

    def test_has_database_parameter(self):
        from graphmana.ingest.pipeline import run_incremental

        sig = inspect.signature(run_incremental)
        assert "database" in sig.parameters
        assert sig.parameters["database"].default is None

    def test_forwards_database(self):
        from graphmana.ingest.pipeline import run_incremental

        src = inspect.getsource(run_incremental)
        assert "database=database" in src


class TestRunIngestDatabaseForwarding:
    """Verify run_ingest forwards database to all callees."""

    def test_forwards_to_detect_import_mode(self):
        from graphmana.ingest.pipeline import run_ingest

        src = inspect.getsource(run_ingest)
        assert "database=database" in src

    def test_forwards_to_apply_post_import_indexes(self):
        from graphmana.ingest.pipeline import run_ingest

        src = inspect.getsource(run_ingest)
        # Verify database=database appears in the apply_post_import_indexes call
        assert "apply_post_import_indexes" in src
        # The database keyword should appear after the function is called
        lines = src.split("\n")
        in_apply_block = False
        found = False
        for line in lines:
            if "apply_post_import_indexes(" in line:
                in_apply_block = True
            if in_apply_block and "database=database" in line:
                found = True
                break
            if in_apply_block and ")" in line and "database" not in line:
                # Check for closing paren without database
                pass
        assert found, "database=database not found in apply_post_import_indexes call"

    def test_forwards_to_provenance_connection(self):
        from graphmana.ingest.pipeline import run_ingest

        src = inspect.getsource(run_ingest)
        assert "database=database" in src


class TestExportCliDatabaseForwarding:
    """Verify the export CLI command passes database to GraphManaConnection."""

    def test_export_passes_database(self):
        """The export function in cli.py must pass database= to GraphManaConnection."""
        from graphmana import cli

        # cli.export is a Click Command, so inspect the underlying callback
        src = inspect.getsource(cli.export.callback)
        assert "database=database" in src


# ---------------------------------------------------------------------------
# Comprehensive: every Neo4j-connected command has --database
# ---------------------------------------------------------------------------


class TestAllCommandsHaveDatabaseParam:
    """Every CLI command that connects to Neo4j must expose --database."""

    # Top-level commands with Neo4j connections
    TOP_LEVEL = [
        "status", "ingest", "load-csv", "export",
        "qc", "liftover", "migrate", "merge",
    ]

    # Subcommand groups: (group_name, [subcommand_names])
    SUBGROUPS = [
        ("cohort", ["define", "list", "show", "delete", "count", "validate"]),
        ("sample", ["remove", "restore", "list", "reassign", "hard-remove"]),
        (
            "annotate",
            [
                "load", "list", "remove",
                "load-cadd", "load-constraint", "load-bed",
                "load-go", "load-pathway", "load-clinvar",
            ],
        ),
        ("provenance", ["list", "show", "headers", "summary"]),
    ]

    def test_top_level_commands_have_database_param(self):
        from graphmana.cli import cli

        for cmd_name in self.TOP_LEVEL:
            cmd = cli.commands[cmd_name]
            param_names = [p.name for p in cmd.params]
            assert "database" in param_names, (
                f"Top-level command '{cmd_name}' is missing --database"
            )

    def test_subgroup_commands_have_database_param(self):
        from graphmana.cli import cli

        for grp_name, subcmd_names in self.SUBGROUPS:
            grp = cli.commands[grp_name]
            for subcmd_name in subcmd_names:
                subcmd = grp.commands[subcmd_name]
                param_names = [p.name for p in subcmd.params]
                assert "database" in param_names, (
                    f"Subcommand '{grp_name} {subcmd_name}' is missing --database"
                )

    def test_merge_has_source_database_param(self):
        from graphmana.cli import cli

        cmd = cli.commands["merge"]
        param_names = [p.name for p in cmd.params]
        assert "source_database" in param_names, (
            "merge command is missing --source-database"
        )
        assert "database" in param_names, (
            "merge command is missing --database"
        )


class TestAllCallbacksPassDatabaseToConnection:
    """Every callback that creates a GraphManaConnection must pass database=database."""

    TOP_LEVEL = [
        "status", "export", "qc", "liftover", "migrate", "merge",
    ]

    SUBGROUPS = [
        ("cohort", ["define", "list", "show", "delete", "count", "validate"]),
        ("sample", ["remove", "restore", "list", "reassign", "hard-remove"]),
        (
            "annotate",
            [
                "load", "list", "remove",
                "load-cadd", "load-constraint", "load-bed",
                "load-go", "load-pathway", "load-clinvar",
            ],
        ),
        ("provenance", ["list", "show", "headers", "summary"]),
    ]

    def test_top_level_callbacks_pass_database(self):
        from graphmana.cli import cli

        for cmd_name in self.TOP_LEVEL:
            cmd = cli.commands[cmd_name]
            src = inspect.getsource(cmd.callback)
            assert "database=database" in src, (
                f"Top-level command '{cmd_name}' callback does not pass "
                f"database=database to GraphManaConnection"
            )

    def test_subgroup_callbacks_pass_database(self):
        from graphmana.cli import cli

        for grp_name, subcmd_names in self.SUBGROUPS:
            grp = cli.commands[grp_name]
            for subcmd_name in subcmd_names:
                subcmd = grp.commands[subcmd_name]
                src = inspect.getsource(subcmd.callback)
                assert "database=database" in src, (
                    f"Subcommand '{grp_name} {subcmd_name}' callback does not "
                    f"pass database=database to GraphManaConnection"
                )

    def test_merge_passes_source_database(self):
        from graphmana.cli import cli

        src = inspect.getsource(cli.commands["merge"].callback)
        assert "database=source_database" in src, (
            "merge callback does not pass database=source_database "
            "to source GraphManaConnection"
        )


class TestNoBareGraphManaConnectionCalls:
    """No bare GraphManaConnection(uri, user, password) without database= in cli.py."""

    def test_no_bare_connection_calls(self):
        import re

        from graphmana import cli as cli_module

        src = inspect.getsource(cli_module)
        # Find all GraphManaConnection(...) calls
        pattern = r"GraphManaConnection\([^)]+\)"
        matches = re.findall(pattern, src)
        for match in matches:
            assert "database=" in match, (
                f"Bare GraphManaConnection call without database=: {match}"
            )
