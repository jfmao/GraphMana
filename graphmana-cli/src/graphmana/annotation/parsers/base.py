"""Base class for annotation parsers that load external data into the graph."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from graphmana.db.connection import GraphManaConnection
from graphmana.db.queries import CREATE_ANNOTATION_VERSION

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 10_000


class BaseAnnotationParser(ABC):
    """Abstract base for annotation parsers.

    Subclasses implement ``source_name``, ``_parse_file()``, and
    ``_load_batch()``.  The base class handles batch accumulation,
    AnnotationVersion tracking, and progress logging.
    """

    def __init__(self, conn: GraphManaConnection) -> None:
        self._conn = conn

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Short identifier for the annotation source (e.g. 'CADD', 'gnomAD_constraint')."""

    @abstractmethod
    def _parse_file(self, input_path: Path, **kwargs) -> Iterator[dict]:
        """Yield one dict per record from the input file.

        Must handle gzipped files, comment lines, malformed rows, etc.
        """

    @abstractmethod
    def _load_batch(self, batch: list[dict]) -> int:
        """Load a batch of parsed records into Neo4j.

        Returns:
            Number of records matched/updated in the graph.
        """

    def load(
        self,
        input_path: str | Path,
        version: str,
        *,
        description: str = "",
        batch_size: int = DEFAULT_BATCH_SIZE,
        **parse_kwargs,
    ) -> dict:
        """Parse an annotation file and load records into the graph.

        Args:
            input_path: Path to the annotation file.
            version: Version label for the AnnotationVersion node.
            description: Human-readable description.
            batch_size: Records per Cypher UNWIND batch.
            **parse_kwargs: Extra kwargs forwarded to ``_parse_file()``.

        Returns:
            Dict with version, source, n_parsed, n_matched.
        """
        input_path = Path(input_path)
        n_parsed = 0
        n_matched = 0
        batch: list[dict] = []

        for record in self._parse_file(input_path, **parse_kwargs):
            batch.append(record)
            n_parsed += 1
            if len(batch) >= batch_size:
                n_matched += self._load_batch(batch)
                batch.clear()

        if batch:
            n_matched += self._load_batch(batch)

        # Upsert AnnotationVersion tracking node
        now = datetime.now(timezone.utc).isoformat()
        with self._conn.driver.session() as session:
            session.run(
                CREATE_ANNOTATION_VERSION,
                {
                    "version_id": version,
                    "source": self.source_name,
                    "version": version,
                    "loaded_date": now,
                    "n_annotations": n_parsed,
                    "description": description,
                },
            )

        logger.info(
            "%s load complete: version=%r, %d parsed, %d matched",
            self.source_name,
            version,
            n_parsed,
            n_matched,
        )

        return {
            "version": version,
            "source": self.source_name,
            "n_parsed": n_parsed,
            "n_matched": n_matched,
        }
