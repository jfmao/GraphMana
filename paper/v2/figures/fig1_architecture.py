#!/usr/bin/env python3
"""Generate Figure 1: GraphMana platform overview and data architecture.

Four panels:
(a) Functional overview — user-facing workflow schematic
(b) Graph schema — node types and relationships
(c) Two access paths — FAST PATH vs FULL PATH
(d) Incremental sample addition — packed array extension

Usage:
    python paper/figures/fig1_architecture.py
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np
from pathlib import Path

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"],
    "font.size": 7,
    "axes.labelsize": 8,
    "axes.titlesize": 9,
    "figure.dpi": 300,
    "savefig.dpi": 300,
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
})

OUT_DIR = Path(__file__).resolve().parent

# Colors
VARIANT_COL = "#2166AC"
SAMPLE_COL = "#4DAF4A"
POP_COL = "#E08214"
CHROM_COL = "#984EA3"
GENE_COL = "#E41A1C"
FAST_COL = "#4DAF4A"
FULL_COL = "#984EA3"
PACKED_COL = "#2166AC"
NEW_COL = "#FC8D62"
GREY = "#B0B0B0"
DB_COL = "#2166AC"
IMPORT_COL = "#66C2A5"
EXPORT_COL = "#FC8D62"
MANAGE_COL = "#8DA0CB"
QC_COL = "#E78AC3"


def _box(ax, x, y, w, h, label, color, fontsize=6.5, sub=None, alpha=0.9):
    """Draw a rounded box with label."""
    rect = FancyBboxPatch((x - w/2, y - h/2), w, h,
                           boxstyle="round,pad=0.06",
                           facecolor=color, edgecolor="white",
                           linewidth=1, alpha=alpha)
    ax.add_patch(rect)
    if sub:
        ax.text(x, y + h*0.15, label, ha="center", va="center",
                fontsize=fontsize, fontweight="bold", color="white")
        ax.text(x, y - h*0.2, sub, ha="center", va="center",
                fontsize=fontsize - 1.5, color="white", alpha=0.9)
    else:
        ax.text(x, y, label, ha="center", va="center",
                fontsize=fontsize, fontweight="bold", color="white")


def _arrow(ax, x1, y1, x2, y2, color="#666666", lw=0.8):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=color, lw=lw))


# ---------------------------------------------------------------------------
# Panel a: Functional overview
# ---------------------------------------------------------------------------

def plot_overview(ax):
    ax.set_xlim(-1, 15)
    ax.set_ylim(-1, 7)
    ax.set_aspect("equal")
    ax.axis("off")
    ax.set_title("a  Platform overview", loc="left", fontweight="bold", fontsize=9)

    # Input
    _box(ax, 1.2, 5.5, 2.2, 1.0, "VCF/GVCF", IMPORT_COL, sub="Input data")

    # Import pipeline
    _box(ax, 4.5, 5.5, 2.2, 1.0, "Import", IMPORT_COL, sub="prepare-csv\nload-csv")

    # Central database
    _box(ax, 7.8, 3.5, 2.6, 2.0, "Neo4j\nGraph DB", DB_COL, fontsize=8)
    ax.text(7.8, 2.3, "Variant + Sample + Population\n+ Chromosome + Gene nodes",
            ha="center", va="top", fontsize=4.5, color="#333", fontstyle="italic")

    # Incremental loop
    _box(ax, 4.5, 3.5, 2.2, 0.9, "Incremental\nAdd", NEW_COL, fontsize=6)

    # Annotations
    _box(ax, 11.5, 5.5, 2.2, 1.0, "Annotations", MANAGE_COL,
         sub="VEP, ClinVar, CADD\nGO, Pathways, BED")

    # Export
    _box(ax, 11.5, 3.5, 2.2, 1.0, "Export", EXPORT_COL,
         sub="17 formats\n+ manifest")

    # Management tools
    _box(ax, 4.5, 1.2, 2.2, 0.9, "Provenance\nSearch", MANAGE_COL, fontsize=5.5)
    _box(ax, 7.8, 0.8, 2.2, 0.9, "QC / Status\nSummary", QC_COL, fontsize=5.5)
    _box(ax, 11.5, 1.2, 2.2, 0.9, "Cohort\nManagement", MANAGE_COL, fontsize=5.5)
    _box(ax, 1.2, 3.5, 2.2, 0.9, "Snapshot\nDiff / Validate", MANAGE_COL, fontsize=5.5)
    _box(ax, 1.2, 1.2, 2.2, 0.9, "Liftover\nRef-check", QC_COL, fontsize=5.5)

    # Arrows: data flow
    _arrow(ax, 2.3, 5.5, 3.4, 5.5, IMPORT_COL, 1.2)   # VCF -> Import
    _arrow(ax, 5.6, 5.5, 6.5, 4.3, IMPORT_COL, 1.2)    # Import -> DB
    _arrow(ax, 5.6, 3.5, 6.5, 3.5, NEW_COL, 1.2)       # Incremental -> DB
    _arrow(ax, 9.1, 4.0, 10.4, 5.2, MANAGE_COL, 0.8)   # DB -> Annotations
    _arrow(ax, 10.4, 5.0, 9.1, 3.8, MANAGE_COL, 0.8)   # Annotations -> DB
    _arrow(ax, 9.1, 3.5, 10.4, 3.5, EXPORT_COL, 1.2)   # DB -> Export
    _arrow(ax, 7.8, 2.5, 7.8, 1.4, QC_COL, 0.8)        # DB -> QC
    _arrow(ax, 6.5, 3.0, 5.6, 1.5, MANAGE_COL, 0.6)    # DB -> Provenance
    _arrow(ax, 9.1, 2.8, 10.4, 1.5, MANAGE_COL, 0.6)   # DB -> Cohort
    _arrow(ax, 6.5, 3.2, 2.3, 3.5, MANAGE_COL, 0.6)    # DB -> Snapshot
    _arrow(ax, 6.5, 2.8, 2.3, 1.5, QC_COL, 0.6)        # DB -> Liftover

    # Incremental loop arrow (VCF -> Incremental)
    _arrow(ax, 1.2, 5.0, 3.4, 3.9, NEW_COL, 0.8)

    # "CLI" label
    ax.text(7.8, -0.3, "All operations via graphmana CLI (58 commands, no programming required)",
            ha="center", fontsize=5.5, fontstyle="italic", color="#555")


# ---------------------------------------------------------------------------
# Panel b: Graph schema (compact)
# ---------------------------------------------------------------------------

def plot_schema(ax):
    ax.set_xlim(-0.5, 10)
    ax.set_ylim(-0.5, 4.5)
    ax.set_aspect("equal")
    ax.axis("off")
    ax.set_title("b  Graph schema", loc="left", fontweight="bold", fontsize=9)

    _box(ax, 5, 3.5, 2.8, 1.2, "Variant", VARIANT_COL, fontsize=7,
         sub="gt_packed, ac[], an[], af[]")
    _box(ax, 1.2, 1.5, 1.6, 0.8, "Sample", SAMPLE_COL, sub="packed_index")
    _box(ax, 1.2, 0.0, 1.6, 0.7, "Population", POP_COL, sub="n_samples")
    _box(ax, 8.5, 1.5, 1.6, 0.7, "Chromosome", CHROM_COL, sub="length")
    _box(ax, 5, 0.5, 1.4, 0.7, "Gene", GENE_COL, sub="symbol")

    _arrow(ax, 1.2, 0.4, 1.2, 1.1, POP_COL)
    ax.text(0.4, 0.75, "IN_POP", fontsize=4, color=POP_COL, fontstyle="italic")
    _arrow(ax, 6.4, 3.0, 7.8, 1.8, CHROM_COL)
    ax.text(7.5, 2.6, "ON_CHR", fontsize=4, color=CHROM_COL, fontstyle="italic")
    _arrow(ax, 5, 2.9, 5, 0.9, GENE_COL)
    ax.text(5.3, 1.9, "HAS_CSQ", fontsize=4, color=GENE_COL, fontstyle="italic")

    # NEXT chain
    ax.annotate("", xy=(6.5, 4.0), xytext=(3.5, 4.0),
                arrowprops=dict(arrowstyle="-|>", color=VARIANT_COL, lw=0.8,
                                connectionstyle="arc3,rad=-0.35"))
    ax.text(5, 4.5, "NEXT", fontsize=4, color=VARIANT_COL, fontstyle="italic")


# ---------------------------------------------------------------------------
# Panel c: Two access paths (compact)
# ---------------------------------------------------------------------------

def plot_encoding(ax):
    ax.set_xlim(-0.5, 10.5)
    ax.set_ylim(-0.5, 4.0)
    ax.set_aspect("equal")
    ax.axis("off")
    ax.set_title("c  Two access paths", loc="left", fontweight="bold", fontsize=9)

    # gt_packed byte
    byte_x, byte_y = 0.5, 2.8
    bit_w, bit_h = 0.55, 0.55
    genotypes = ["00", "01", "10", "11"]
    gt_labels = ["Ref", "Het", "Alt", "Miss"]
    colors = ["#66C2A5", "#FC8D62", "#E78AC3", GREY]

    ax.text(byte_x + 2 * bit_w, byte_y + bit_h + 0.1,
            "gt_packed: 2 bits/sample", ha="center", fontsize=6, fontweight="bold")

    for i, (bits, label, col) in enumerate(zip(genotypes, gt_labels, colors)):
        x = byte_x + i * bit_w
        rect = FancyBboxPatch((x, byte_y), bit_w - 0.04, bit_h,
                               boxstyle="round,pad=0.02",
                               facecolor=col, edgecolor="white", linewidth=0.8)
        ax.add_patch(rect)
        ax.text(x + bit_w/2 - 0.02, byte_y + bit_h*0.65, bits,
                ha="center", va="center", fontsize=5.5, fontweight="bold", color="white")
        ax.text(x + bit_w/2 - 0.02, byte_y + bit_h*0.25, label,
                ha="center", va="center", fontsize=4, color="white")

    # FAST PATH box
    fast_box = FancyBboxPatch((4.5, 2.0), 5.5, 1.5,
                               boxstyle="round,pad=0.1",
                               facecolor=FAST_COL, edgecolor="white",
                               linewidth=1, alpha=0.15)
    ax.add_patch(fast_box)
    ax.text(4.7, 3.2, "FAST PATH  O(K)", fontsize=6.5, fontweight="bold", color=FAST_COL)
    ax.text(4.7, 2.75, "Reads ac[], an[], af[] directly",
            fontsize=5, color="#333")
    ax.text(4.7, 2.3, "TreeMix  SFS  BED  TSV  JSON",
            fontsize=5.5, fontweight="bold", color=FAST_COL)

    # FULL PATH box
    full_box = FancyBboxPatch((4.5, 0.0), 5.5, 1.5,
                               boxstyle="round,pad=0.1",
                               facecolor=FULL_COL, edgecolor="white",
                               linewidth=1, alpha=0.15)
    ax.add_patch(full_box)
    ax.text(4.7, 1.2, "FULL PATH  O(N)", fontsize=6.5, fontweight="bold", color=FULL_COL)
    ax.text(4.7, 0.75, "Unpacks per-sample genotypes",
            fontsize=5, color="#333")
    ax.text(4.7, 0.3, "VCF  PLINK  EIGENSTRAT  +11 more",
            fontsize=5.5, fontweight="bold", color=FULL_COL)

    # Arrows
    _arrow(ax, 2.7, 2.9, 4.4, 2.9, FAST_COL, 1)
    _arrow(ax, 2.7, 2.7, 4.4, 0.8, FULL_COL, 1)

    # 125x callout
    ax.text(3.0, 1.5, "125x\nsmaller",
            ha="center", va="center", fontsize=5.5,
            bbox=dict(boxstyle="round,pad=0.25", facecolor="#FFF9C4",
                      edgecolor="#F9A825", linewidth=0.5),
            color="#333", fontweight="bold")


# ---------------------------------------------------------------------------
# Panel d: Incremental addition (compact)
# ---------------------------------------------------------------------------

def plot_incremental(ax):
    ax.set_xlim(-0.5, 10.5)
    ax.set_ylim(-0.3, 3.0)
    ax.set_aspect("equal")
    ax.axis("off")
    ax.set_title("d  Incremental sample addition", loc="left", fontweight="bold", fontsize=9)

    cell_w, cell_h = 0.7, 0.5

    def draw_array(x0, y0, n_exist, n_new, label, sub=""):
        for i in range(n_exist):
            rect = FancyBboxPatch((x0 + i*cell_w, y0), cell_w-0.04, cell_h,
                                   boxstyle="round,pad=0.02",
                                   facecolor=PACKED_COL, edgecolor="white", linewidth=0.5)
            ax.add_patch(rect)
            ax.text(x0 + i*cell_w + cell_w/2, y0 + cell_h/2,
                    f"S{i+1}", ha="center", va="center", fontsize=5, color="white")
        for j in range(n_new):
            i = n_exist + j
            rect = FancyBboxPatch((x0 + i*cell_w, y0), cell_w-0.04, cell_h,
                                   boxstyle="round,pad=0.02",
                                   facecolor=NEW_COL, edgecolor="white", linewidth=0.5)
            ax.add_patch(rect)
            ax.text(x0 + i*cell_w + cell_w/2, y0 + cell_h/2,
                    f"S{i+1}", ha="center", va="center", fontsize=5, color="white")
        ax.text(x0 - 0.15, y0 + cell_h/2, label,
                ha="right", va="center", fontsize=6.5, fontweight="bold")
        if sub:
            total_w = (n_exist + n_new) * cell_w
            ax.text(x0 + total_w/2, y0 - 0.15, sub,
                    ha="center", fontsize=4.5, color="#666")

    draw_array(1.5, 2.0, 4, 0, "Before:", "packed_index: 0  1  2  3")
    draw_array(1.5, 0.6, 4, 2, "After:", "packed_index: 0  1  2  3  4  5")

    _arrow(ax, 4.5, 1.7, 4.5, 1.2, "#333", 1)
    ax.text(5.5, 1.4, "+ new batch", fontsize=6, color=NEW_COL, fontweight="bold")

    # Legend
    ax.add_patch(FancyBboxPatch((7.2, 2.2), 0.35, 0.25, boxstyle="round,pad=0.02",
                                 facecolor=PACKED_COL, edgecolor="white", linewidth=0.5))
    ax.text(7.7, 2.32, "Existing (unchanged)", fontsize=5, va="center")
    ax.add_patch(FancyBboxPatch((7.2, 1.8), 0.35, 0.25, boxstyle="round,pad=0.02",
                                 facecolor=NEW_COL, edgecolor="white", linewidth=0.5))
    ax.text(7.7, 1.92, "New (appended)", fontsize=5, va="center")

    ax.text(7.5, 1.0, "packed_index\nis immutable",
            ha="center", va="center", fontsize=5,
            bbox=dict(boxstyle="round,pad=0.25", facecolor="#E3F2FD",
                      edgecolor=PACKED_COL, linewidth=0.5),
            color=PACKED_COL, fontweight="bold")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    fig = plt.figure(figsize=(7.08, 11.0))  # 180mm wide, tall for 4 panels

    ax_a = fig.add_axes([0.02, 0.74, 0.96, 0.24])
    plot_overview(ax_a)

    ax_b = fig.add_axes([0.02, 0.51, 0.96, 0.21])
    plot_schema(ax_b)

    ax_c = fig.add_axes([0.02, 0.27, 0.96, 0.21])
    plot_encoding(ax_c)

    ax_d = fig.add_axes([0.02, 0.02, 0.96, 0.22])
    plot_incremental(ax_d)

    out_pdf = OUT_DIR / "fig1_architecture.pdf"
    fig.savefig(out_pdf, bbox_inches="tight", pad_inches=0.1)
    out_png = OUT_DIR / "fig1_architecture.png"
    fig.savefig(out_png, bbox_inches="tight", pad_inches=0.1)
    print(f"Saved {out_pdf} and {out_png}")
    plt.close(fig)


if __name__ == "__main__":
    main()
