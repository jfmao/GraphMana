"""QC report output formatters — TSV, JSON, HTML."""

from __future__ import annotations

import csv
import json
from io import StringIO
from pathlib import Path


def write_qc_report(data: dict, output: Path, fmt: str = "tsv") -> None:
    """Write QC report to file in the requested format.

    Args:
        data: QC results dict from QCManager.run().
        output: Output file path.
        fmt: One of 'tsv', 'json', 'html'.
    """
    output.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "json":
        _write_json(data, output)
    elif fmt == "html":
        _write_html(data, output)
    else:
        _write_tsv(data, output)


def _write_json(data: dict, output: Path) -> None:
    """Write QC data as pretty-printed JSON."""
    with open(output, "w") as f:
        json.dump(data, f, indent=2, default=str)


def _write_tsv(data: dict, output: Path) -> None:
    """Write QC data as multi-section TSV.

    Each section is separated by a blank line and starts with a header
    comment line (## Section Name).
    """
    with open(output, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")

        if "variant" in data:
            _write_variant_tsv(w, data["variant"])
            f.write("\n")

        if "sample" in data:
            _write_sample_tsv(w, data["sample"])
            f.write("\n")

        if "batch" in data:
            _write_batch_tsv(w, data["batch"])


def _write_variant_tsv(w, variant_data: dict) -> None:
    """Write variant QC section to TSV writer."""
    summary = variant_data.get("summary", {})
    w.writerow(["## Variant QC Summary"])
    w.writerow(["metric", "value"])
    for key in sorted(summary.keys()):
        val = summary[key]
        if isinstance(val, float):
            val = f"{val:.6f}"
        w.writerow([key, val])

    type_counts = variant_data.get("type_counts", [])
    if type_counts:
        w.writerow([])
        w.writerow(["## Variant Type Distribution"])
        w.writerow(["variant_type", "count"])
        for row in type_counts:
            w.writerow([row["variant_type"], row["count"]])

    chr_counts = variant_data.get("chr_counts", [])
    if chr_counts:
        w.writerow([])
        w.writerow(["## Variants per Chromosome"])
        w.writerow(["chr", "count"])
        for row in chr_counts:
            w.writerow([row["chr"], row["count"]])


def _write_sample_tsv(w, sample_data: dict) -> None:
    """Write sample QC section to TSV writer."""
    w.writerow(["## Sample QC"])
    w.writerow(["n_samples", sample_data.get("n_samples", 0)])
    w.writerow(["n_variants_scanned", sample_data.get("n_variants_scanned", 0)])

    stats = sample_data.get("stats", [])
    if stats:
        w.writerow([])
        w.writerow(["sampleId", "n_het", "n_hom_alt", "heterozygosity", "call_rate"])
        for s in stats:
            w.writerow(
                [
                    s["sampleId"],
                    s["n_het"],
                    s["n_hom_alt"],
                    f"{s['heterozygosity']:.6f}",
                    f"{s['call_rate']:.6f}",
                ]
            )


def _write_batch_tsv(w, batch_data: dict) -> None:
    """Write batch QC section to TSV writer."""
    pop_summary = batch_data.get("population_summary", [])
    w.writerow(["## Population Summary"])
    w.writerow(["population", "n_samples_total", "n_samples_active"])
    for row in pop_summary:
        w.writerow([row["population"], row["n_samples_total"], row["n_samples_active"]])


def _write_html(data: dict, output: Path) -> None:
    """Write a simple HTML QC report."""
    parts: list[str] = [
        "<!DOCTYPE html>",
        "<html><head><title>GraphMana QC Report</title>",
        "<style>table{border-collapse:collapse;margin:1em 0}",
        "th,td{border:1px solid #ccc;padding:4px 8px;text-align:left}",
        "th{background:#f5f5f5}h2{margin-top:2em}</style></head><body>",
        "<h1>GraphMana QC Report</h1>",
    ]

    if "variant" in data:
        parts.append("<h2>Variant QC Summary</h2>")
        parts.append(_dict_to_html_table(data["variant"].get("summary", {})))

        type_counts = data["variant"].get("type_counts", [])
        if type_counts:
            parts.append("<h2>Variant Type Distribution</h2>")
            parts.append(_list_to_html_table(type_counts))

        chr_counts = data["variant"].get("chr_counts", [])
        if chr_counts:
            parts.append("<h2>Variants per Chromosome</h2>")
            parts.append(_list_to_html_table(chr_counts))

    if "sample" in data:
        parts.append("<h2>Sample QC</h2>")
        parts.append(f"<p>Samples: {data['sample'].get('n_samples', 0)}, ")
        parts.append(f"Variants scanned: {data['sample'].get('n_variants_scanned', 0)}</p>")
        stats = data["sample"].get("stats", [])
        if stats:
            parts.append(_list_to_html_table(stats))

    if "batch" in data:
        parts.append("<h2>Population Summary</h2>")
        pop_summary = data["batch"].get("population_summary", [])
        if pop_summary:
            parts.append(_list_to_html_table(pop_summary))

    parts.append("</body></html>")
    output.write_text("\n".join(parts))


def _dict_to_html_table(d: dict) -> str:
    """Convert a flat dict to a 2-column HTML table."""
    buf = StringIO()
    buf.write("<table><tr><th>Metric</th><th>Value</th></tr>")
    for k, v in sorted(d.items()):
        if isinstance(v, float):
            v = f"{v:.6f}"
        buf.write(f"<tr><td>{k}</td><td>{v}</td></tr>")
    buf.write("</table>")
    return buf.getvalue()


def _list_to_html_table(rows: list[dict]) -> str:
    """Convert a list of dicts to an HTML table."""
    if not rows:
        return "<p>No data.</p>"
    buf = StringIO()
    headers = list(rows[0].keys())
    buf.write("<table><tr>")
    for h in headers:
        buf.write(f"<th>{h}</th>")
    buf.write("</tr>")
    for row in rows:
        buf.write("<tr>")
        for h in headers:
            val = row.get(h, "")
            if isinstance(val, float):
                val = f"{val:.6f}"
            buf.write(f"<td>{val}</td>")
        buf.write("</tr>")
    buf.write("</table>")
    return buf.getvalue()
