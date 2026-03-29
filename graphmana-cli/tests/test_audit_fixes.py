"""Tests for systematic audit bug fixes.

Verifies:
1. `--threads` removed from qc command (no consumer)
2. `--threads` removed from liftover command (no consumer)
3. `--verbose` added to load-csv; database forwarded to apply_post_import_indexes
4. LIST_ALL_SAMPLES uses ACTIVE_SAMPLE_FILTER; _WITH_EXCLUDED variants exist
5. All --database CLI defaults use DEFAULT_DATABASE constant
6. All --snapshot-dir CLI defaults use DEFAULT_SNAPSHOT_DIR constant
"""

import inspect

from graphmana.cli import cli
from graphmana.config import DEFAULT_DATABASE
from graphmana.db import queries
from graphmana.sample.manager import SampleManager
from graphmana.snapshot.manager import DEFAULT_SNAPSHOT_DIR

# ---------------------------------------------------------------------------
# Bug 1: qc --threads removed
# ---------------------------------------------------------------------------


class TestQCThreadsRemoved:
    def test_threads_not_in_qc_params(self):
        """qc command must not accept --threads (no consumer)."""
        qc_cmd = cli.commands["qc"]
        param_names = [p.name for p in qc_cmd.params]
        assert "threads" not in param_names

    def test_threads_not_in_qc_callback_sig(self):
        """qc callback must not have 'threads' parameter."""
        qc_cmd = cli.commands["qc"]
        sig = inspect.signature(qc_cmd.callback)
        assert "threads" not in sig.parameters


# ---------------------------------------------------------------------------
# Bug 2: liftover --threads removed
# ---------------------------------------------------------------------------


class TestLiftoverThreadsRemoved:
    def test_threads_not_in_liftover_params(self):
        """liftover command must not accept --threads (no consumer)."""
        liftover_cmd = cli.commands["liftover"]
        param_names = [p.name for p in liftover_cmd.params]
        assert "threads" not in param_names

    def test_threads_not_in_liftover_callback_sig(self):
        """liftover callback must not have 'threads' parameter."""
        liftover_cmd = cli.commands["liftover"]
        sig = inspect.signature(liftover_cmd.callback)
        assert "threads" not in sig.parameters


# ---------------------------------------------------------------------------
# Bug 3: load-csv --verbose + database forwarding
# ---------------------------------------------------------------------------


class TestLoadCSVFixes:
    def test_verbose_in_load_csv_params(self):
        """load-csv must accept --verbose/--quiet."""
        load_csv_cmd = cli.commands["load-csv"]
        param_names = [p.name for p in load_csv_cmd.params]
        assert "verbose" in param_names

    def test_verbose_in_load_csv_callback_sig(self):
        """load-csv callback must have 'verbose' parameter."""
        load_csv_cmd = cli.commands["load-csv"]
        sig = inspect.signature(load_csv_cmd.callback)
        assert "verbose" in sig.parameters

    def test_apply_post_import_indexes_gets_database(self):
        """apply_post_import_indexes call must pass database=database."""
        source = inspect.getsource(cli.commands["load-csv"].callback)
        assert "database=database" in source


# ---------------------------------------------------------------------------
# Bug 4: Soft-delete queries
# ---------------------------------------------------------------------------


class TestSoftDeleteQueries:
    def test_list_all_samples_has_active_filter(self):
        """LIST_ALL_SAMPLES must include ACTIVE_SAMPLE_FILTER."""
        assert queries.ACTIVE_SAMPLE_FILTER in queries.LIST_ALL_SAMPLES

    def test_list_all_samples_with_excluded_no_active_filter(self):
        """LIST_ALL_SAMPLES_WITH_EXCLUDED must NOT include ACTIVE_SAMPLE_FILTER."""
        assert queries.ACTIVE_SAMPLE_FILTER not in queries.LIST_ALL_SAMPLES_WITH_EXCLUDED

    def test_list_by_pop_has_active_filter(self):
        """LIST_SAMPLES_BY_POPULATION must include ACTIVE_SAMPLE_FILTER."""
        assert queries.ACTIVE_SAMPLE_FILTER in queries.LIST_SAMPLES_BY_POPULATION

    def test_list_by_pop_with_excluded_no_active_filter(self):
        """LIST_SAMPLES_BY_POPULATION_WITH_EXCLUDED must NOT include ACTIVE_SAMPLE_FILTER."""
        assert (
            queries.ACTIVE_SAMPLE_FILTER
            not in queries.LIST_SAMPLES_BY_POPULATION_WITH_EXCLUDED
        )

    def test_sample_manager_list_no_python_postfilter(self):
        """SampleManager.list() must not have the old Python post-filter."""
        source = inspect.getsource(SampleManager.list)
        # Old code had: samples = [s for s in samples if not s.get("excluded")]
        assert 's.get("excluded")' not in source
        assert "s.get('excluded')" not in source


# ---------------------------------------------------------------------------
# Bug 5: DEFAULT_DATABASE used everywhere
# ---------------------------------------------------------------------------


class TestDefaultDatabaseConstant:
    def test_cli_database_defaults_use_constant(self):
        """All --database options in top-level CLI commands must use DEFAULT_DATABASE."""
        top_level_with_db = [
            "load-csv", "ingest", "export", "status", "qc",
            "liftover", "migrate", "merge",
        ]
        for cmd_name in top_level_with_db:
            cmd = cli.commands[cmd_name]
            for param in cmd.params:
                if param.name == "database":
                    assert param.default == DEFAULT_DATABASE, (
                        f"{cmd_name} --database default is {param.default!r}, "
                        f"expected DEFAULT_DATABASE={DEFAULT_DATABASE!r}"
                    )

    def test_snapshot_commands_database_defaults(self):
        """Snapshot subcommands --database defaults must use DEFAULT_DATABASE."""
        snapshot_grp = cli.commands["snapshot"]
        for subcmd_name in ["create", "restore"]:
            subcmd = snapshot_grp.commands[subcmd_name]
            for param in subcmd.params:
                if param.name == "database":
                    assert param.default == DEFAULT_DATABASE, (
                        f"snapshot {subcmd_name} --database default is {param.default!r}"
                    )

    def test_loader_uses_default_database(self):
        """loader.run_load_csv and _build_import_command must use DEFAULT_DATABASE."""
        from graphmana.ingest.loader import _build_import_command, run_load_csv

        sig_run = inspect.signature(run_load_csv)
        assert sig_run.parameters["database"].default == DEFAULT_DATABASE

        sig_build = inspect.signature(_build_import_command)
        assert sig_build.parameters["database"].default == DEFAULT_DATABASE

    def test_pipeline_uses_default_database(self):
        """pipeline.run_ingest must use DEFAULT_DATABASE."""
        from graphmana.ingest.pipeline import run_ingest

        sig = inspect.signature(run_ingest)
        assert sig.parameters["database"].default == DEFAULT_DATABASE

    def test_snapshot_manager_uses_default_database(self):
        """SnapshotManager.create/restore must use DEFAULT_DATABASE."""
        from graphmana.snapshot.manager import SnapshotManager

        sig_create = inspect.signature(SnapshotManager.create)
        assert sig_create.parameters["database"].default == DEFAULT_DATABASE

        sig_restore = inspect.signature(SnapshotManager.restore)
        assert sig_restore.parameters["database"].default == DEFAULT_DATABASE


# ---------------------------------------------------------------------------
# Bug 6: DEFAULT_SNAPSHOT_DIR used everywhere
# ---------------------------------------------------------------------------


class TestDefaultSnapshotDirConstant:
    def test_snapshot_commands_use_constant(self):
        """All --snapshot-dir options must use DEFAULT_SNAPSHOT_DIR."""
        snapshot_grp = cli.commands["snapshot"]
        for subcmd_name in ["create", "list", "restore", "delete"]:
            subcmd = snapshot_grp.commands[subcmd_name]
            for param in subcmd.params:
                if param.name == "snapshot_dir":
                    assert param.default == DEFAULT_SNAPSHOT_DIR, (
                        f"snapshot {subcmd_name} --snapshot-dir default is "
                        f"{param.default!r}, expected {DEFAULT_SNAPSHOT_DIR!r}"
                    )

    def test_liftover_snapshot_dir_uses_constant(self):
        """liftover --snapshot-dir must use DEFAULT_SNAPSHOT_DIR."""
        liftover_cmd = cli.commands["liftover"]
        for param in liftover_cmd.params:
            if param.name == "snapshot_dir":
                assert param.default == DEFAULT_SNAPSHOT_DIR, (
                    f"liftover --snapshot-dir default is {param.default!r}"
                )


# ---------------------------------------------------------------------------
# Bug 7: recalculate_af conditional default
# ---------------------------------------------------------------------------


class TestRecalculateAfConditionalDefault:
    def test_recalculate_af_resolved_before_exporters(self):
        """recalculate_af must be resolved to a bool before exporter dispatch."""
        src = inspect.getsource(cli.commands["export"].callback)
        # Must resolve None→bool BEFORE the exporter dispatch block
        assert "if recalculate_af is None:" in src
        assert "recalculate_af = bool(populations)" in src

    def test_no_bool_recalculate_af_in_exporter_calls(self):
        """Exporter constructors must receive recalculate_af directly, not bool()."""
        src = inspect.getsource(cli.commands["export"].callback)
        assert "bool(recalculate_af)" not in src


# ---------------------------------------------------------------------------
# Bug 8: Migration version update uses execute_write_tx
# ---------------------------------------------------------------------------


class TestMigrationTransactionSafety:
    def test_version_update_uses_write_tx(self):
        """MigrationManager.run() must use execute_write_tx for version update."""
        from graphmana.migration.manager import MigrationManager

        src = inspect.getsource(MigrationManager.run)
        assert "execute_write_tx" in src

    def test_version_update_not_bare_execute_write(self):
        """Version update must NOT use bare execute_write with UPDATE_SCHEMA_VERSION."""
        from graphmana.migration.manager import MigrationManager

        src = inspect.getsource(MigrationManager.run)
        # The old pattern was: self._conn.execute_write(UPDATE_SCHEMA_VERSION, {...})
        assert "execute_write(UPDATE_SCHEMA_VERSION" not in src
        assert "execute_write(\n" not in src or "UPDATE_SCHEMA_VERSION" not in src


# ---------------------------------------------------------------------------
# Bug 9: SchemaMetadata n_populations decremented on hard_remove
# ---------------------------------------------------------------------------


class TestHardRemovePopulationDecrement:
    def test_hard_remove_decrements_n_populations(self):
        """hard_remove() must decrement n_populations when populations are deleted."""
        src = inspect.getsource(SampleManager.hard_remove)
        assert "n_populations_deleted" in src
        assert "n_populations" in src


# ---------------------------------------------------------------------------
# Bug 10: --include-filtered wired to CLI
# ---------------------------------------------------------------------------


class TestIncludeFilteredCLI:
    def test_prepare_csv_has_include_filtered(self):
        """prepare-csv must accept --include-filtered."""
        cmd = cli.commands["prepare-csv"]
        param_names = [p.name for p in cmd.params]
        assert "include_filtered" in param_names

    def test_ingest_has_include_filtered(self):
        """ingest must accept --include-filtered."""
        cmd = cli.commands["ingest"]
        param_names = [p.name for p in cmd.params]
        assert "include_filtered" in param_names

    def test_prepare_csv_forwards_include_filtered(self):
        """prepare-csv callback must pass include_filtered= to run_prepare_csv."""
        src = inspect.getsource(cli.commands["prepare-csv"].callback)
        assert "include_filtered=include_filtered" in src

    def test_ingest_forwards_include_filtered(self):
        """ingest callback must pass include_filtered= to run_ingest."""
        src = inspect.getsource(cli.commands["ingest"].callback)
        assert "include_filtered=include_filtered" in src


# ---------------------------------------------------------------------------
# Bug 11: --verbose/--quiet consistency
# ---------------------------------------------------------------------------


class TestVerboseQuietConsistency:
    ALL_VERBOSE_COMMANDS = [
        "prepare-csv", "ingest", "load-csv", "export",
        "liftover", "migrate", "setup-neo4j", "neo4j-start", "neo4j-stop",
    ]

    def test_no_bare_verbose_flags(self):
        """No CLI command should use bare --verbose (is_flag). All must use --verbose/--quiet."""
        from graphmana import cli as cli_module

        src = inspect.getsource(cli_module)
        assert '"--verbose", is_flag=True' not in src

    def test_all_verbose_commands_have_quiet(self):
        """Every command with --verbose must also accept --quiet."""
        for cmd_name in self.ALL_VERBOSE_COMMANDS:
            cmd = cli.commands[cmd_name]
            for p in cmd.params:
                if p.name == "verbose":
                    # Boolean flags have secondary_opts (e.g. ['--quiet'])
                    assert p.secondary_opts, (
                        f"'{cmd_name}' --verbose is not paired with --quiet"
                    )


# ---------------------------------------------------------------------------
# Bug 12: --filter-liftover-status export filter
# ---------------------------------------------------------------------------


class TestFilterLiftoverStatus:
    def test_export_has_filter_liftover_status(self):
        """export must accept --filter-liftover-status."""
        cmd = cli.commands["export"]
        param_names = [p.name for p in cmd.params]
        assert "filter_liftover_status" in param_names

    def test_export_filter_config_has_liftover_status(self):
        """ExportFilterConfig must have liftover_status field."""
        from graphmana.filtering.export_filters import ExportFilterConfig

        fc = ExportFilterConfig(liftover_status="mapped")
        assert fc.liftover_status == "mapped"

    def test_parallel_serialization_has_liftover_status(self):
        """Parallel serialization must include liftover_status."""
        from graphmana.export.parallel import _get_filter_config_dict
        from graphmana.filtering.export_filters import ExportFilterConfig

        fc = ExportFilterConfig(liftover_status="unmapped")
        d = _get_filter_config_dict(fc)
        assert d["liftover_status"] == "unmapped"

    def test_export_filter_checks_liftover_status(self):
        """ExportFilter.variant_passes must check liftover_status."""
        from graphmana.filtering.export_filters import ExportFilter, ExportFilterConfig

        f = ExportFilter(ExportFilterConfig(liftover_status="mapped"))
        assert f.variant_passes({"liftover_status": "mapped"})
        assert not f.variant_passes({"liftover_status": "unmapped"})
        assert not f.variant_passes({})


# ---------------------------------------------------------------------------
# Bug 13: recalculate_af in all FULL PATH exporters
# ---------------------------------------------------------------------------


class TestRecalculateAfAllExporters:
    FULL_PATH_EXPORTERS = [
        "plink2_export",
        "eigenstrat_export",
        "beagle_export",
        "structure_export",
        "genepop_export",
        "hap_export",
        "json_export",
        "zarr_export",
        "gds_export",
        "bgen_export",
    ]

    def test_all_full_path_exporters_call_maybe_recalculate_af(self):
        """All FULL PATH exporters must call _maybe_recalculate_af."""
        import importlib

        for name in self.FULL_PATH_EXPORTERS:
            mod = importlib.import_module(f"graphmana.export.{name}")
            src = inspect.getsource(mod)
            assert "_maybe_recalculate_af" in src, (
                f"{name} does not call _maybe_recalculate_af"
            )

    def test_base_exporter_has_maybe_recalculate_af(self):
        """BaseExporter must have the _maybe_recalculate_af helper."""
        from graphmana.export.base import BaseExporter

        assert hasattr(BaseExporter, "_maybe_recalculate_af")


# ---------------------------------------------------------------------------
# Bug 14: Zarr/GDS/BGEN no longer buffer all variants
# ---------------------------------------------------------------------------


class TestStreamingExporters:
    """Verify Zarr, GDS, BGEN use count-first-then-stream pattern."""

    def test_zarr_no_all_variants_list(self):
        """ZarrExporter.export must not buffer all_variants list."""
        from graphmana.export.zarr_export import ZarrExporter

        src = inspect.getsource(ZarrExporter.export)
        assert "all_variants" not in src

    def test_gds_no_all_variants_list(self):
        """GDSExporter.export must not buffer all_variants list."""
        from graphmana.export.gds_export import GDSExporter

        src = inspect.getsource(GDSExporter.export)
        assert "all_variants" not in src

    def test_bgen_no_all_variants_list(self):
        """BGENExporter.export must not buffer all_variants list."""
        from graphmana.export.bgen_export import BGENExporter

        src = inspect.getsource(BGENExporter.export)
        assert "all_variants" not in src


# ---------------------------------------------------------------------------
# Bug 15: Snapshot restore checks if Neo4j is running
# ---------------------------------------------------------------------------


class TestSnapshotRestorePreCheck:
    def test_restore_checks_neo4j_running(self):
        """SnapshotManager.restore must check if Neo4j is running."""
        from graphmana.snapshot.manager import SnapshotManager

        src = inspect.getsource(SnapshotManager.restore)
        assert "_is_neo4j_running" in src

    def test_is_neo4j_running_exists(self):
        """_is_neo4j_running helper must exist."""
        from graphmana.snapshot.manager import _is_neo4j_running

        assert callable(_is_neo4j_running)
