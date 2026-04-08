"""Bundled data files for GraphMana.

Contains the pre-built graphmana-procedures.jar for Neo4j plugin deployment.
"""

from pathlib import Path

DATA_DIR = Path(__file__).parent

PROCEDURES_JAR = DATA_DIR / "graphmana-procedures.jar"


def get_procedures_jar() -> Path:
    """Return the path to the bundled procedures JAR.

    Raises FileNotFoundError if the JAR is not present (e.g., in a
    development install without building the Java procedures first).
    """
    if not PROCEDURES_JAR.exists():
        raise FileNotFoundError(
            f"Bundled JAR not found at {PROCEDURES_JAR}. "
            "Build it with: cd graphmana-procedures && mvn clean package -DskipTests"
        )
    return PROCEDURES_JAR
