"""Neo4j driver wrapper with context manager support."""

from neo4j import GraphDatabase


class _EagerResult:
    """Lightweight wrapper holding materialized records from a closed session.

    Supports .single(), iteration, and len() — the subset of the neo4j
    Result API used by GraphMana.
    """

    def __init__(self, records: list, summary=None):
        self._records = records
        self._summary = summary

    def single(self):
        """Return the single record, or None if empty."""
        if len(self._records) == 1:
            return self._records[0]
        if len(self._records) == 0:
            return None
        raise ValueError(
            f"Expected exactly one record, got {len(self._records)}"
        )

    def __iter__(self):
        return iter(self._records)

    def __len__(self):
        return len(self._records)

    def data(self):
        """Return records as list of dicts."""
        return [dict(r) for r in self._records]


class GraphManaConnection:
    """Thin wrapper around neo4j.GraphDatabase.driver().

    Usage::

        with GraphManaConnection(uri, user, password) as conn:
            result = conn.execute_read("MATCH (n) RETURN count(n) AS c")
    """

    def __init__(self, uri: str, user: str, password: str, database: str | None = None):
        self._uri = uri
        self._user = user
        self._password = password
        self._database = database
        self._driver = None

    def __enter__(self):
        self._driver = GraphDatabase.driver(
            self._uri,
            auth=(self._user, self._password),
            # Keep TCP connections alive during long streaming exports (10+ minutes).
            keep_alive=True,
            # Allow connections to live for 2 hours so a single chromosome export
            # session is never closed mid-stream by the pool reaper.
            max_connection_lifetime=7200,
            # Generous acquisition timeout so slow-starting cluster nodes don't fail.
            connection_acquisition_timeout=120,
        )
        self._driver.verify_connectivity()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._driver:
            self._driver.close()
        return False

    def execute_read(self, query: str, parameters: dict | None = None):
        """Execute a read transaction and return an EagerResult.

        Uses Result.fetch() to materialize all records before the session
        closes, returning an EagerResult-like wrapper that supports
        .single(), iteration, and len().
        """
        with self._driver.session(database=self._database) as session:
            result = session.run(query, parameters or {})
            return _EagerResult(list(result), result.consume())

    def execute_write(self, query: str, parameters: dict | None = None):
        """Execute a write transaction and return an EagerResult."""
        with self._driver.session(database=self._database) as session:
            result = session.run(query, parameters or {})
            return _EagerResult(list(result), result.consume())

    def execute_write_tx(self, tx_func, **kwargs):
        """Execute a managed write transaction with auto-retry.

        Args:
            tx_func: callable(tx, **kwargs) that runs Cypher inside a transaction.
            **kwargs: forwarded to tx_func.

        Returns:
            The return value of tx_func.
        """
        with self._driver.session(database=self._database) as session:
            return session.execute_write(tx_func, **kwargs)

    def execute_read_tx(self, tx_func, **kwargs):
        """Execute a managed read transaction with auto-retry.

        Args:
            tx_func: callable(tx, **kwargs) that runs Cypher inside a transaction.
            **kwargs: forwarded to tx_func.

        Returns:
            The return value of tx_func.
        """
        with self._driver.session(database=self._database) as session:
            return session.execute_read(tx_func, **kwargs)

    @property
    def driver(self):
        """Access the underlying neo4j Driver."""
        return self._driver
