"""Tests for provenance search CLI command, query, and ProvenanceManager.search()."""

from unittest.mock import MagicMock

from click.testing import CliRunner

from graphmana.cli import cli
from graphmana.db.queries import SEARCH_INGESTION_LOGS
from graphmana.provenance.manager import ProvenanceManager


class TestProvenanceSearchHelp:
    """Test that provenance search command is registered."""

    def test_search_in_provenance_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["provenance", "--help"])
        assert "search" in result.output

    def test_search_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["provenance", "search", "--help"])
        assert result.exit_code == 0
        assert "--since" in result.output
        assert "--until" in result.output
        assert "--dataset-id" in result.output
        assert "--json" in result.output


class TestSearchQuery:
    """Test the SEARCH_INGESTION_LOGS query structure."""

    def test_query_has_since_parameter(self):
        assert "$since" in SEARCH_INGESTION_LOGS

    def test_query_has_until_parameter(self):
        assert "$until" in SEARCH_INGESTION_LOGS

    def test_query_has_dataset_id_parameter(self):
        assert "$dataset_id" in SEARCH_INGESTION_LOGS

    def test_query_handles_null_parameters(self):
        """Query should use IS NULL checks so None parameters mean 'no filter'."""
        assert "IS NULL" in SEARCH_INGESTION_LOGS

    def test_query_returns_ordered(self):
        assert "ORDER BY" in SEARCH_INGESTION_LOGS


class TestProvenanceManagerSearch:
    """Test ProvenanceManager.search() with mocked connection."""

    def _make_mock_conn(self, records):
        conn = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_results = []
        for rec in records:
            mock_rec = MagicMock()
            mock_rec.__getitem__ = lambda s, k, r=rec: r
            mock_results.append(mock_rec)

        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter(mock_results))
        mock_session.run.return_value = mock_result
        conn.driver.session.return_value = mock_session
        return conn

    def test_search_returns_list(self):
        log = {"log_id": "test_1", "import_date": "2026-03-15", "mode": "initial",
               "source_file": "data.vcf.gz", "n_samples": 100, "n_variants": 50000}
        conn = self._make_mock_conn([log])
        mgr = ProvenanceManager(conn)
        results = mgr.search(since="2026-03-01", until="2026-03-31")
        assert isinstance(results, list)
        assert len(results) == 1

    def test_search_passes_parameters(self):
        conn = self._make_mock_conn([])
        mgr = ProvenanceManager(conn)
        mgr.search(since="2026-01-01", until="2026-12-31", dataset_id="batch_1")

        session = conn.driver.session.return_value.__enter__.return_value
        call_args = session.run.call_args
        params = call_args[0][1] if len(call_args[0]) > 1 else call_args[1].get("parameters", {})
        assert params["since"] == "2026-01-01"
        assert params["until"] == "2026-12-31"
        assert params["dataset_id"] == "batch_1"

    def test_search_with_no_filters(self):
        conn = self._make_mock_conn([])
        mgr = ProvenanceManager(conn)
        results = mgr.search()
        assert results == []

        session = conn.driver.session.return_value.__enter__.return_value
        call_args = session.run.call_args
        params = call_args[0][1] if len(call_args[0]) > 1 else {}
        assert params.get("since") is None
        assert params.get("until") is None
        assert params.get("dataset_id") is None

    def test_has_search_method(self):
        assert callable(getattr(ProvenanceManager, "search", None))
