"""Tests for ProvenanceManager and provenance CLI commands."""

from unittest.mock import MagicMock

from click.testing import CliRunner

from graphmana.cli import cli
from graphmana.provenance.manager import ProvenanceManager

# ---------------------------------------------------------------------------
# Interface tests
# ---------------------------------------------------------------------------


class TestProvenanceManagerInterface:
    """Verify ProvenanceManager has the expected methods."""

    def test_class_exists(self):
        assert ProvenanceManager is not None

    def test_has_record_ingestion(self):
        assert callable(getattr(ProvenanceManager, "record_ingestion", None))

    def test_has_list_ingestions(self):
        assert callable(getattr(ProvenanceManager, "list_ingestions", None))

    def test_has_get_ingestion(self):
        assert callable(getattr(ProvenanceManager, "get_ingestion", None))

    def test_has_list_vcf_headers(self):
        assert callable(getattr(ProvenanceManager, "list_vcf_headers", None))

    def test_has_get_vcf_header(self):
        assert callable(getattr(ProvenanceManager, "get_vcf_header", None))

    def test_has_summary(self):
        assert callable(getattr(ProvenanceManager, "summary", None))


# ---------------------------------------------------------------------------
# Helper to create a mock connection
# ---------------------------------------------------------------------------


def _make_mock_conn():
    """Create a mock GraphManaConnection with a session context manager."""
    conn = MagicMock()
    session = MagicMock()
    conn.driver.session.return_value.__enter__ = MagicMock(return_value=session)
    conn.driver.session.return_value.__exit__ = MagicMock(return_value=False)
    return conn, session


# ---------------------------------------------------------------------------
# record_ingestion
# ---------------------------------------------------------------------------


class TestRecordIngestion:
    def test_creates_ingestion_log(self):
        conn, session = _make_mock_conn()
        record = MagicMock()
        record.__getitem__ = lambda self, k: {
            "log_id": "ds1_2024-01-01T00:00:00+00:00",
            "source_file": "/data/test.vcf.gz",
            "dataset_id": "ds1",
            "mode": "initial",
            "n_samples": 100,
            "n_variants": 5000,
        }
        result = MagicMock()
        result.single.return_value = record
        session.run.return_value = result

        mgr = ProvenanceManager(conn)
        mgr.record_ingestion(
            source_file="/data/test.vcf.gz",
            dataset_id="ds1",
            mode="initial",
            n_samples=100,
            n_variants=5000,
        )

        session.run.assert_called_once()
        call_args = session.run.call_args
        assert "CREATE" in call_args[0][0]
        params = call_args[0][1]
        assert params["dataset_id"] == "ds1"
        assert params["mode"] == "initial"
        assert params["n_samples"] == 100
        assert params["n_variants"] == 5000
        assert params["source_file"] == "/data/test.vcf.gz"

    def test_log_id_format(self):
        conn, session = _make_mock_conn()
        record = MagicMock()
        record.__getitem__ = lambda self, k: {}
        result = MagicMock()
        result.single.return_value = record
        session.run.return_value = result

        mgr = ProvenanceManager(conn)
        mgr.record_ingestion(
            source_file="test.vcf",
            dataset_id="mydata",
            mode="initial",
            n_samples=10,
            n_variants=100,
        )

        params = session.run.call_args[0][1]
        assert params["log_id"].startswith("mydata_")


# ---------------------------------------------------------------------------
# list_ingestions
# ---------------------------------------------------------------------------


class TestListIngestions:
    def test_returns_ordered_list(self):
        conn, session = _make_mock_conn()
        rec1 = MagicMock()
        rec1.__getitem__ = lambda self, k: {
            "log_id": "ds1_2024-01-02",
            "import_date": "2024-01-02",
        }
        rec2 = MagicMock()
        rec2.__getitem__ = lambda self, k: {
            "log_id": "ds1_2024-01-01",
            "import_date": "2024-01-01",
        }
        result = MagicMock()
        result.__iter__ = MagicMock(return_value=iter([rec1, rec2]))
        session.run.return_value = result

        mgr = ProvenanceManager(conn)
        logs = mgr.list_ingestions()

        assert len(logs) == 2
        session.run.assert_called_once()

    def test_empty_list(self):
        conn, session = _make_mock_conn()
        result = MagicMock()
        result.__iter__ = MagicMock(return_value=iter([]))
        session.run.return_value = result

        mgr = ProvenanceManager(conn)
        logs = mgr.list_ingestions()
        assert logs == []


# ---------------------------------------------------------------------------
# get_ingestion
# ---------------------------------------------------------------------------


class TestGetIngestion:
    def test_found(self):
        conn, session = _make_mock_conn()
        record = MagicMock()
        record.__getitem__ = lambda self, k: {"log_id": "ds1_2024", "mode": "initial"}
        result = MagicMock()
        result.single.return_value = record
        session.run.return_value = result

        mgr = ProvenanceManager(conn)
        log = mgr.get_ingestion("ds1_2024")
        assert log is not None

    def test_not_found(self):
        conn, session = _make_mock_conn()
        result = MagicMock()
        result.single.return_value = None
        session.run.return_value = result

        mgr = ProvenanceManager(conn)
        log = mgr.get_ingestion("nonexistent")
        assert log is None


# ---------------------------------------------------------------------------
# list_vcf_headers
# ---------------------------------------------------------------------------


class TestListVCFHeaders:
    def test_returns_list(self):
        conn, session = _make_mock_conn()
        rec1 = MagicMock()
        rec1.__getitem__ = lambda self, k: {"dataset_id": "ds1"}
        result = MagicMock()
        result.__iter__ = MagicMock(return_value=iter([rec1]))
        session.run.return_value = result

        mgr = ProvenanceManager(conn)
        headers = mgr.list_vcf_headers()
        assert len(headers) == 1


# ---------------------------------------------------------------------------
# get_vcf_header
# ---------------------------------------------------------------------------


class TestGetVCFHeader:
    def test_found(self):
        conn, session = _make_mock_conn()
        record = MagicMock()
        record.__getitem__ = lambda self, k: {"dataset_id": "ds1"}
        result = MagicMock()
        result.single.return_value = record
        session.run.return_value = result

        mgr = ProvenanceManager(conn)
        header = mgr.get_vcf_header("ds1")
        assert header is not None

    def test_not_found(self):
        conn, session = _make_mock_conn()
        result = MagicMock()
        result.single.return_value = None
        session.run.return_value = result

        mgr = ProvenanceManager(conn)
        header = mgr.get_vcf_header("nonexistent")
        assert header is None


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------


class TestSummary:
    def test_returns_aggregate_dict(self):
        conn, session = _make_mock_conn()
        record = MagicMock()
        record.__getitem__ = lambda self, k: {
            "n_ingestions": 3,
            "total_samples_imported": 500,
            "total_variants_imported": 10000,
            "first_import": "2024-01-01",
            "last_import": "2024-06-15",
            "source_files": ["/a.vcf", "/b.vcf"],
        }.get(k, None)
        result = MagicMock()
        result.single.return_value = record
        session.run.return_value = result

        mgr = ProvenanceManager(conn)
        s = mgr.summary()
        assert s["n_ingestions"] == 3
        assert s["total_samples_imported"] == 500
        assert s["total_variants_imported"] == 10000
        assert s["first_import"] == "2024-01-01"
        assert s["last_import"] == "2024-06-15"
        assert len(s["source_files"]) == 2

    def test_empty_summary(self):
        conn, session = _make_mock_conn()
        result = MagicMock()
        result.single.return_value = None
        session.run.return_value = result

        mgr = ProvenanceManager(conn)
        s = mgr.summary()
        assert s["n_ingestions"] == 0
        assert s["source_files"] == []


# ---------------------------------------------------------------------------
# CLI tests
# ---------------------------------------------------------------------------


class TestProvenanceCLI:
    def test_provenance_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["provenance", "--help"])
        assert result.exit_code == 0
        assert "provenance" in result.output.lower()

    def test_provenance_list_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["provenance", "list", "--help"])
        assert result.exit_code == 0
        assert "--neo4j-uri" in result.output

    def test_provenance_show_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["provenance", "show", "--help"])
        assert result.exit_code == 0
        assert "LOG_ID" in result.output

    def test_provenance_headers_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["provenance", "headers", "--help"])
        assert result.exit_code == 0
        assert "--neo4j-uri" in result.output

    def test_provenance_summary_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["provenance", "summary", "--help"])
        assert result.exit_code == 0
        assert "--neo4j-uri" in result.output
