"""Schema migration manager for GraphMana databases."""

from __future__ import annotations

import datetime
from dataclasses import dataclass

from graphmana.config import GRAPHMANA_VERSION, SCHEMA_VERSION
from graphmana.db.queries import GET_SCHEMA_METADATA, UPDATE_SCHEMA_VERSION


def _parse_version(v: str) -> tuple[int, ...]:
    """Parse a version string like '0.5.0' into a comparable tuple (0, 5, 0)."""
    return tuple(int(x) for x in v.split("."))


@dataclass(frozen=True)
class Migration:
    """A single schema migration step."""

    from_version: str
    to_version: str
    description: str
    steps: list[str]


MIGRATIONS: list[Migration] = [
    Migration(
        from_version="0.1.0",
        to_version="0.5.0",
        description="Add liftover and ClinVar indexes",
        steps=[
            "CREATE INDEX variant_liftover_status IF NOT EXISTS "
            "FOR (v:Variant) ON (v.liftover_status)",
            "CREATE INDEX variant_original_id IF NOT EXISTS "
            "FOR (v:Variant) ON (v.original_variantId)",
            "CREATE INDEX variant_clinvar IF NOT EXISTS "
            "FOR (v:Variant) ON (v.clinvar_sig)",
        ],
    ),
    Migration(
        from_version="0.5.0",
        to_version="0.9.0",
        description="Add gene constraint and sample QC indexes",
        steps=[
            "CREATE INDEX gene_pli IF NOT EXISTS FOR (g:Gene) ON (g.pli)",
            "CREATE INDEX sample_call_rate IF NOT EXISTS FOR (s:Sample) ON (s.call_rate)",
        ],
    ),
]


class MigrationManager:
    """Detects schema version mismatches and applies incremental migrations."""

    def __init__(self, conn):
        self._conn = conn

    def get_current_version(self) -> str:
        """Read schema_version from SchemaMetadata node. Returns '0.0.0' if absent."""
        result = self._conn.execute_read(GET_SCHEMA_METADATA)
        record = result.single()
        if record is None:
            return "0.0.0"
        node = record["m"]
        return node.get("schema_version", "0.0.0")

    def get_target_version(self) -> str:
        """Return the schema version this software expects."""
        return SCHEMA_VERSION

    def get_pending_migrations(self) -> list[Migration]:
        """Return ordered list of migrations needed to reach target version."""
        current = _parse_version(self.get_current_version())
        target = _parse_version(self.get_target_version())
        if current >= target:
            return []
        pending = []
        for m in MIGRATIONS:
            if _parse_version(m.from_version) >= current and _parse_version(m.to_version) <= target:
                pending.append(m)
        return pending

    def run(self, *, dry_run: bool = False) -> dict:
        """Execute pending migrations sequentially.

        Returns a summary dict with from_version, to_version,
        migrations_applied, and dry_run flag.
        """
        from_version = self.get_current_version()
        pending = self.get_pending_migrations()

        if not pending:
            return {
                "from_version": from_version,
                "to_version": from_version,
                "migrations_applied": 0,
                "dry_run": dry_run,
                "migrations": [],
            }

        migration_summaries = []
        for m in pending:
            migration_summaries.append(
                {
                    "from": m.from_version,
                    "to": m.to_version,
                    "description": m.description,
                    "steps": list(m.steps),
                }
            )

        if dry_run:
            return {
                "from_version": from_version,
                "to_version": pending[-1].to_version,
                "migrations_applied": len(pending),
                "dry_run": True,
                "migrations": migration_summaries,
            }

        for m in pending:
            # DDL statements (CREATE INDEX) are auto-committed and cannot
            # be grouped with DML in a single Neo4j transaction.  We run
            # all steps first, then atomically update the schema version
            # inside execute_write_tx so the version bump cannot be lost.
            for step in m.steps:
                self._conn.execute_write(step)

            version_params = {
                "schema_version": m.to_version,
                "graphmana_version": GRAPHMANA_VERSION,
                "last_modified": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }

            def _update_version(tx, params=version_params):
                tx.run(UPDATE_SCHEMA_VERSION, params)

            self._conn.execute_write_tx(_update_version)

        return {
            "from_version": from_version,
            "to_version": pending[-1].to_version,
            "migrations_applied": len(pending),
            "dry_run": False,
            "migrations": migration_summaries,
        }
