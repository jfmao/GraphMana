"""Tests for GraphManaConnection and _EagerResult."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from graphmana.db.connection import GraphManaConnection, _EagerResult


# ---------------------------------------------------------------------------
# _EagerResult
# ---------------------------------------------------------------------------


class TestEagerResult:
    """Test the _EagerResult wrapper."""

    def test_single_with_one_record(self):
        result = _EagerResult([{"a": 1}])
        assert result.single() == {"a": 1}

    def test_single_with_no_records_returns_none(self):
        result = _EagerResult([])
        assert result.single() is None

    def test_single_with_multiple_records_raises(self):
        result = _EagerResult([{"a": 1}, {"a": 2}])
        with pytest.raises(ValueError, match="Expected exactly one record"):
            result.single()

    def test_len(self):
        result = _EagerResult([1, 2, 3])
        assert len(result) == 3

    def test_len_empty(self):
        result = _EagerResult([])
        assert len(result) == 0

    def test_iter(self):
        records = [{"a": 1}, {"b": 2}]
        result = _EagerResult(records)
        assert list(result) == records

    def test_data_returns_dicts(self):
        """data() converts records to plain dicts."""
        mock_rec = MagicMock()
        mock_rec.__iter__ = MagicMock(return_value=iter([("key", "val")]))
        result = _EagerResult([mock_rec])
        data = result.data()
        assert isinstance(data, list)
        assert len(data) == 1

    def test_summary_stored(self):
        summary = MagicMock()
        result = _EagerResult([], summary=summary)
        assert result._summary is summary


# ---------------------------------------------------------------------------
# GraphManaConnection
# ---------------------------------------------------------------------------


class TestGraphManaConnectionInit:
    """Test connection initialization (no live Neo4j)."""

    def test_stores_credentials(self):
        conn = GraphManaConnection("bolt://localhost:7687", "neo4j", "pass")
        assert conn._uri == "bolt://localhost:7687"
        assert conn._user == "neo4j"
        assert conn._password == "pass"
        assert conn._database is None
        assert conn._driver is None

    def test_custom_database(self):
        conn = GraphManaConnection("bolt://host", "u", "p", database="mydb")
        assert conn._database == "mydb"


class TestGraphManaConnectionContextManager:
    """Test context manager protocol with mocked driver."""

    def _make_mock_driver(self):
        driver = MagicMock()
        driver.verify_connectivity = MagicMock()
        driver.close = MagicMock()
        return driver

    @patch("graphmana.db.connection.GraphDatabase")
    def test_enter_creates_driver_and_verifies(self, mock_gdb):
        mock_driver = self._make_mock_driver()
        mock_gdb.driver.return_value = mock_driver

        conn = GraphManaConnection("bolt://localhost", "neo4j", "pass")
        result = conn.__enter__()

        assert result is conn
        mock_gdb.driver.assert_called_once()
        mock_driver.verify_connectivity.assert_called_once()

    @patch("graphmana.db.connection.GraphDatabase")
    def test_exit_closes_driver(self, mock_gdb):
        mock_driver = self._make_mock_driver()
        mock_gdb.driver.return_value = mock_driver

        conn = GraphManaConnection("bolt://localhost", "neo4j", "pass")
        conn.__enter__()
        conn.__exit__(None, None, None)

        mock_driver.close.assert_called_once()

    @patch("graphmana.db.connection.GraphDatabase")
    def test_exit_without_enter_does_not_crash(self, mock_gdb):
        """Exiting without entering (driver is None) should not raise."""
        conn = GraphManaConnection("bolt://localhost", "neo4j", "pass")
        # _driver is None, should not crash
        assert conn.__exit__(None, None, None) is False

    @patch("graphmana.db.connection.GraphDatabase")
    def test_context_manager_protocol(self, mock_gdb):
        mock_driver = self._make_mock_driver()
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_driver.session.return_value = mock_session
        mock_gdb.driver.return_value = mock_driver

        with GraphManaConnection("bolt://localhost", "neo4j", "pass") as conn:
            assert conn.driver is mock_driver

        mock_driver.close.assert_called_once()


class TestGraphManaConnectionExecute:
    """Test execute_read/write with mocked sessions."""

    def _setup_conn(self, mock_gdb):
        mock_driver = MagicMock()
        mock_driver.verify_connectivity = MagicMock()
        mock_driver.close = MagicMock()
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_result = MagicMock()
        mock_result.__iter__ = MagicMock(return_value=iter([{"c": 42}]))
        mock_result.consume.return_value = MagicMock()
        mock_session.run.return_value = mock_result
        mock_driver.session.return_value = mock_session
        mock_gdb.driver.return_value = mock_driver
        return mock_driver, mock_session

    @patch("graphmana.db.connection.GraphDatabase")
    def test_execute_read_returns_eager_result(self, mock_gdb):
        _, mock_session = self._setup_conn(mock_gdb)

        with GraphManaConnection("bolt://localhost", "neo4j", "pass") as conn:
            result = conn.execute_read("RETURN 1 AS c")

        assert isinstance(result, _EagerResult)
        assert len(result) == 1
        mock_session.run.assert_called_once_with("RETURN 1 AS c", {})

    @patch("graphmana.db.connection.GraphDatabase")
    def test_execute_read_passes_parameters(self, mock_gdb):
        _, mock_session = self._setup_conn(mock_gdb)

        with GraphManaConnection("bolt://localhost", "neo4j", "pass") as conn:
            conn.execute_read("MATCH (n) WHERE n.id = $id RETURN n", {"id": "abc"})

        mock_session.run.assert_called_once_with(
            "MATCH (n) WHERE n.id = $id RETURN n", {"id": "abc"}
        )

    @patch("graphmana.db.connection.GraphDatabase")
    def test_execute_write_returns_eager_result(self, mock_gdb):
        _, mock_session = self._setup_conn(mock_gdb)

        with GraphManaConnection("bolt://localhost", "neo4j", "pass") as conn:
            result = conn.execute_write("CREATE (n) RETURN n")

        assert isinstance(result, _EagerResult)

    @patch("graphmana.db.connection.GraphDatabase")
    def test_execute_write_tx_delegates(self, mock_gdb):
        mock_driver, _ = self._setup_conn(mock_gdb)
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.execute_write.return_value = "tx_result"
        mock_driver.session.return_value = mock_session

        with GraphManaConnection("bolt://localhost", "neo4j", "pass") as conn:
            tx_func = MagicMock()
            result = conn.execute_write_tx(tx_func, key="val")

        mock_session.execute_write.assert_called_once_with(tx_func, key="val")
        assert result == "tx_result"

    @patch("graphmana.db.connection.GraphDatabase")
    def test_execute_read_tx_delegates(self, mock_gdb):
        mock_driver, _ = self._setup_conn(mock_gdb)
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.execute_read.return_value = "read_result"
        mock_driver.session.return_value = mock_session

        with GraphManaConnection("bolt://localhost", "neo4j", "pass") as conn:
            tx_func = MagicMock()
            result = conn.execute_read_tx(tx_func, key="val")

        mock_session.execute_read.assert_called_once_with(tx_func, key="val")
        assert result == "read_result"
