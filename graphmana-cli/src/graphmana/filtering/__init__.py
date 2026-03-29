"""GraphMana variant filtering."""

__all__ = [
    "ExportFilter",
    "ExportFilterConfig",
    "ImportFilterChain",
    "ImportFilterConfig",
]


def __getattr__(name):  # noqa: F401
    if name in ("ImportFilterChain", "ImportFilterConfig"):
        from graphmana.filtering import import_filters

        return getattr(import_filters, name)
    if name in ("ExportFilter", "ExportFilterConfig"):
        from graphmana.filtering import export_filters

        return getattr(export_filters, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
