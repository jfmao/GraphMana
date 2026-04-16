"""Persistent configuration for GraphMana (``~/.graphmana/config.yaml``).

Created by ``graphmana setup-neo4j`` and read by every subsequent command
so that ``--neo4j-home``, ``--neo4j-uri``, ``--neo4j-password`` etc. do not
need to be repeated on every invocation. The file is chmod 0o600 because it
stores a plaintext Neo4j password (matching GraphPop's convention).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

CONFIG_DIR = Path.home() / ".graphmana"
CONFIG_PATH = CONFIG_DIR / "config.yaml"


def save_config(cfg: dict) -> Path:
    """Write the configuration dict to ``~/.graphmana/config.yaml``.

    Creates the directory if it does not exist. Sets file permissions to
    0o600 (owner read/write only) because the dict typically contains the
    Neo4j password in plaintext.

    Args:
        cfg: dict with keys like ``neo4j_home``, ``uri``, ``user``,
            ``password``, ``database``, ``bolt_port``, ``http_port``,
            ``data_dir``.

    Returns:
        Path to the written config file.
    """
    try:
        import yaml
    except ImportError:
        logger.warning(
            "PyYAML not installed; config file not written. "
            "Install with: pip install pyyaml"
        )
        return CONFIG_PATH

    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    filtered = {k: v for k, v in cfg.items() if v is not None}

    with open(CONFIG_PATH, "w") as f:
        yaml.dump(filtered, f, default_flow_style=False, sort_keys=False)

    try:
        CONFIG_PATH.chmod(0o600)
    except OSError:
        pass

    # Invalidate the read cache so the next get_config_value sees the new data.
    global _cached_config, _cache_loaded
    _cached_config = None
    _cache_loaded = False

    logger.info("Config written to %s", CONFIG_PATH)
    return CONFIG_PATH


_cached_config: dict | None = None
_cache_loaded: bool = False


def load_config() -> dict | None:
    """Load ``~/.graphmana/config.yaml`` and return it as a dict.

    The result is cached after the first successful (or unsuccessful) read
    so that multiple ``get_config_value`` calls within the same process do
    not re-read the file from disk.

    Returns ``None`` if the file does not exist or cannot be parsed.
    """
    global _cached_config, _cache_loaded
    if _cache_loaded:
        return _cached_config

    if not CONFIG_PATH.exists():
        _cache_loaded = True
        _cached_config = None
        return None

    try:
        import yaml
    except ImportError:
        logger.debug("PyYAML not installed; cannot load config file.")
        _cache_loaded = True
        _cached_config = None
        return None

    try:
        with open(CONFIG_PATH) as f:
            data = yaml.safe_load(f)
        _cached_config = data if isinstance(data, dict) else None
    except Exception as exc:
        logger.warning("Failed to parse %s: %s", CONFIG_PATH, exc)
        _cached_config = None
    _cache_loaded = True
    return _cached_config


def get_config_value(
    key: str,
    *,
    cli_value: object = None,
    env_var: str | None = None,
    default: object = None,
) -> object:
    """Resolve a configuration value with a clear precedence order.

    Resolution (first non-None wins):
        1. ``cli_value`` — explicit CLI flag passed by the user.
        2. Config file — ``~/.graphmana/config.yaml[key]``.
        3. Environment variable — ``os.environ[env_var]``.
        4. ``default`` — hardcoded fallback.

    Args:
        key: config-file dict key (e.g. ``"neo4j_home"``).
        cli_value: value from a Click option (None if not passed).
        env_var: name of the environment variable to check.
        default: final fallback.

    Returns:
        The resolved value, or ``default`` if nothing matched.
    """
    if cli_value is not None:
        return cli_value

    cfg = load_config()
    if cfg is not None:
        val = cfg.get(key)
        if val is not None:
            return val

    if env_var is not None:
        val = os.environ.get(env_var)
        if val is not None:
            return val

    return default
