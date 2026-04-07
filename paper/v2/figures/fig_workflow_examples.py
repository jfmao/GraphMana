#!/usr/bin/env python3
"""Design examples: Conventional workflow (without GraphMana) vs GraphMana workflow.

Creates several icon/schematic styles for comparison. These are exploratory
designs — not yet integrated into the paper.

Usage:
    python paper/figures/fig_workflow_examples.py
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Circle
import numpy as np
from pathlib import Path

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"],
    "font.size": 7,
    "figure.dpi": 300,
    "savefig.dpi": 300,
    "pdf.fonttype": 42,
})

OUT_DIR = Path(__file__).resolve().parent

# Color palette
VCF_COL = "#1B9E77"       # teal green — VCF files
PLINK_COL = "#D95F02"     # orange — PLINK files
TREEMIX_COL = "#7570B3"   # purple — TreeMix/SFS files
ANNOT_COL = "#E7298A"     # pink — annotations
SEQ_COL = "#66A61E"       # green — sequencing center
PERSON_COL = "#E6AB02"    # gold — data manager / PI
SCRIPT_COL = "#A6761D"    # brown — scripts
GREY = "#B0B0B0"
RED = "#E41A1C"
BLUE = "#2166AC"
DB_COL = "#2166AC"        # GraphMana blue


def _icon_file(ax, x, y, label, color, size=0.6, sublabel=None):
    """Draw a file icon (rectangle with folded corner)."""
    w, h = size, size * 1.3
    fold = size * 0.25

    # Main rectangle
    verts = [
        (x - w/2, y - h/2),
        (x - w/2, y + h/2),
        (x + w/2 - fold, y + h/2),
        (x + w/2, y + h/2 - fold),
        (x + w/2, y - h/2),
        (x - w/2, y - h/2),
    ]
    from matplotlib.patches import Polygon
    poly = Polygon(verts, closed=True, facecolor=color, edgecolor="white",
                   linewidth=0.8, alpha=0.9)
    ax.add_patch(poly)

    # Fold triangle
    fold_verts = [
        (x + w/2 - fold, y + h/2),
        (x + w/2 - fold, y + h/2 - fold),
        (x + w/2, y + h/2 - fold),
    ]
    fold_poly = Polygon(fold_verts, closed=True, facecolor="white",
                        edgecolor=color, linewidth=0.5, alpha=0.5)
    ax.add_patch(fold_poly)

    ax.text(x, y - 0.05, label, ha="center", va="center",
            fontsize=5.5, fontweight="bold", color="white")
    if sublabel:
        ax.text(x, y - h/2 - 0.15, sublabel, ha="center", va="top",
                fontsize=4, color="#555")


def _icon_person(ax, x, y, label, color=PERSON_COL, size=0.5):
    """Draw a person icon (circle head + body)."""
    # Head
    head = Circle((x, y + size*0.6), size*0.25, facecolor=color,
                  edgecolor="white", linewidth=0.8)
    ax.add_patch(head)
    # Body (trapezoid)
    from matplotlib.patches import Polygon
    bw = size * 0.5
    bh = size * 0.5
    body = Polygon([
        (x - bw*0.3, y + size*0.3),
        (x + bw*0.3, y + size*0.3),
        (x + bw*0.6, y - bh*0.3),
        (x - bw*0.6, y - bh*0.3),
    ], closed=True, facecolor=color, edgecolor="white", linewidth=0.5)
    ax.add_patch(body)
    ax.text(x, y - size*0.6, label, ha="center", va="top",
            fontsize=5, color="#333")


def _icon_server(ax, x, y, label, color=SEQ_COL, size=0.6):
    """Draw a server/sequencer icon (stacked cylinders)."""
    w, h = size * 1.2, size * 0.3
    for i in range(3):
        yy = y + i * h * 0.8
        rect = FancyBboxPatch((x - w/2, yy - h/2), w, h,
                               boxstyle="round,pad=0.04",
                               facecolor=color, edgecolor="white",
                               linewidth=0.5, alpha=0.8 + i*0.05)
        ax.add_patch(rect)
    ax.text(x, y - h, label, ha="center", va="top",
            fontsize=5, color="#333")


def _icon_database(ax, x, y, label, color=DB_COL, size=0.7):
    """Draw a database icon (cylinder)."""
    from matplotlib.patches import Ellipse
    w, h = size * 1.4, size * 1.6
    ew = w
    eh = h * 0.2

    # Body
    rect = FancyBboxPatch((x - w/2, y - h/2 + eh/2), w, h - eh,
                           boxstyle="round,pad=0.02",
                           facecolor=color, edgecolor="white", linewidth=1)
    ax.add_patch(rect)
    # Top ellipse
    top = Ellipse((x, y + h/2 - eh/2), ew, eh, facecolor=color,
                  edgecolor="white", linewidth=1, alpha=0.95)
    ax.add_patch(top)
    # Bottom ellipse (just the visible arc)
    bot = Ellipse((x, y - h/2 + eh/2), ew, eh, facecolor=color,
                  edgecolor="white", linewidth=0.5, alpha=0.7)
    ax.add_patch(bot)

    ax.text(x, y, label, ha="center", va="center",
            fontsize=7, fontweight="bold", color="white")


def _icon_script(ax, x, y, label="script.sh", color=SCRIPT_COL, size=0.5):
    """Draw a script/code icon (terminal-like box)."""
    w, h = size * 1.5, size * 1.0
    rect = FancyBboxPatch((x - w/2, y - h/2), w, h,
                           boxstyle="round,pad=0.04",
                           facecolor=color, edgecolor="white", linewidth=0.8)
    ax.add_patch(rect)
    # Terminal prompt lines
    ax.text(x - w*0.3, y + h*0.15, "> _", fontsize=4, color="white",
            fontfamily="monospace")
    ax.text(x, y - h*0.2, label, ha="center", fontsize=4, color="white")


def _arrow(ax, x1, y1, x2, y2, color="#666", lw=0.8, style="-|>"):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle=style, color=color, lw=lw))


def _label_arrow(ax, x1, y1, x2, y2, label, color="#666", lw=0.8):
    _arrow(ax, x1, y1, x2, y2, color, lw)
    mx, my = (x1+x2)/2, (y1+y2)/2
    ax.text(mx, my + 0.15, label, ha="center", va="bottom",
            fontsize=4, color=color, fontstyle="italic")


# ===========================================================================
# EXAMPLE 1: Conventional workflow (file-centric, chaotic)
# ===========================================================================

def plot_conventional(ax):
    ax.set_xlim(-1, 14)
    ax.set_ylim(-2, 8)
    ax.set_aspect("equal")
    ax.axis("off")
    ax.set_title("Conventional workflow (without GraphMana)",
                 loc="left", fontweight="bold", fontsize=10)

    # Sequencing center
    _icon_server(ax, 0.5, 6.5, "Sequencing\nCenter", SEQ_COL)

    # VCF files arriving over time
    _icon_file(ax, 3, 7, "VCF", VCF_COL, sublabel="Batch 1\n(Jan)")
    _icon_file(ax, 4.2, 7, "VCF", VCF_COL, sublabel="Batch 2\n(Mar)")
    _icon_file(ax, 5.4, 7, "VCF", VCF_COL, sublabel="Batch 3\n(Jun)")

    _arrow(ax, 1.2, 6.5, 2.5, 7, SEQ_COL)
    _arrow(ax, 1.2, 6.5, 3.7, 7, SEQ_COL)
    _arrow(ax, 1.2, 6.5, 4.9, 7, SEQ_COL)

    # Data manager
    _icon_person(ax, 7, 6.5, "Data\nManager", PERSON_COL)

    # The "merge everything" step
    _icon_script(ax, 7, 4.8, "bcftools\nmerge", SCRIPT_COL)
    _arrow(ax, 5.8, 6.8, 6.5, 5.2, SCRIPT_COL, 0.6)
    _arrow(ax, 7, 6.0, 7, 5.3, PERSON_COL, 0.6)

    # Merged VCF
    _icon_file(ax, 7, 3.5, "Merged\nVCF", VCF_COL, size=0.7, sublabel="(regenerated\neach time)")

    _arrow(ax, 7, 4.3, 7, 4.0, SCRIPT_COL, 0.8)

    # Conversion scripts fanning out
    _icon_script(ax, 3, 3.5, "vcf2plink\n.sh", SCRIPT_COL)
    _icon_script(ax, 5, 2.0, "vcf2treemix\n.sh", SCRIPT_COL)
    _icon_script(ax, 9, 2.0, "vcf2eigen\n.sh", SCRIPT_COL)
    _icon_script(ax, 11, 3.5, "vcf2sfs\n.sh", SCRIPT_COL)

    _arrow(ax, 6.5, 3.2, 3.6, 3.5, SCRIPT_COL, 0.6)
    _arrow(ax, 6.8, 3.0, 5.4, 2.3, SCRIPT_COL, 0.6)
    _arrow(ax, 7.2, 3.0, 8.6, 2.3, SCRIPT_COL, 0.6)
    _arrow(ax, 7.5, 3.2, 10.4, 3.5, SCRIPT_COL, 0.6)

    # Output files
    _icon_file(ax, 1.5, 2.0, "PLINK\n.bed", PLINK_COL, sublabel="v3? v5?")
    _icon_file(ax, 3.5, 0.5, "TreeMix\n.gz", TREEMIX_COL, sublabel="which\nsamples?")
    _icon_file(ax, 7, 0.5, "EIGEN-\nSTRAT", TREEMIX_COL, sublabel="what\nfilters?")
    _icon_file(ax, 10, 0.5, "SFS\n.fs", TREEMIX_COL, sublabel="polarized?\nfolded?")
    _icon_file(ax, 12.5, 2.0, "VCF\nsubset", VCF_COL, sublabel="for\ncollaborator")

    _arrow(ax, 3, 3.0, 1.8, 2.6, SCRIPT_COL, 0.6)
    _arrow(ax, 5, 1.5, 3.8, 1.0, SCRIPT_COL, 0.6)
    _arrow(ax, 9, 1.5, 7.3, 1.0, SCRIPT_COL, 0.6)
    _arrow(ax, 11, 3.0, 10.3, 1.0, SCRIPT_COL, 0.6)
    _arrow(ax, 11.5, 3.5, 12.2, 2.5, SCRIPT_COL, 0.6)

    # Annotation (another VCF rewrite)
    _icon_file(ax, 10.5, 6.5, "ClinVar\nv2024", ANNOT_COL, size=0.5)
    _arrow(ax, 10.5, 6.0, 7.5, 4.0, ANNOT_COL, 0.6)
    ax.text(9.2, 5.2, "rewrite\nentire VCF", ha="center", fontsize=4.5,
            color=RED, fontweight="bold", rotation=30)

    # Pain point callouts
    pain_style = dict(boxstyle="round,pad=0.2", facecolor="#FFEBEE",
                      edgecolor=RED, linewidth=0.5)
    ax.text(1.5, 5.5, "New batch =\nregenerate\nEVERYTHING",
            ha="center", fontsize=5, color=RED, fontweight="bold",
            bbox=pain_style)
    ax.text(7, -1.2, "No provenance: which samples? what parameters? what version?",
            ha="center", fontsize=5.5, color=RED, fontweight="bold",
            bbox=pain_style)
    ax.text(12.5, 5.5, "Annotation =\nVCF rewrite\n(96 sec)",
            ha="center", fontsize=5, color=RED, fontweight="bold",
            bbox=pain_style)

    # Scattered files label
    ax.text(7, -0.3, "Scattered output files across directories",
            ha="center", fontsize=5, color="#888", fontstyle="italic")


# ===========================================================================
# EXAMPLE 2: GraphMana workflow (database-centric, clean)
# ===========================================================================

def plot_graphmana(ax):
    ax.set_xlim(-1, 14)
    ax.set_ylim(-2, 8)
    ax.set_aspect("equal")
    ax.axis("off")
    ax.set_title("GraphMana workflow", loc="left", fontweight="bold", fontsize=10)

    # Sequencing center
    _icon_server(ax, 0.5, 6.5, "Sequencing\nCenter", SEQ_COL)

    # VCF files
    _icon_file(ax, 3, 7, "VCF", VCF_COL, sublabel="Batch 1")
    _icon_file(ax, 4.2, 7, "VCF", VCF_COL, sublabel="Batch 2")
    _icon_file(ax, 5.4, 7, "VCF", VCF_COL, sublabel="Batch 3")

    _arrow(ax, 1.2, 6.5, 2.5, 7, SEQ_COL)
    _arrow(ax, 1.2, 6.5, 3.7, 7, SEQ_COL)
    _arrow(ax, 1.2, 6.5, 4.9, 7, SEQ_COL)

    # Central database
    _icon_database(ax, 7, 4.0, "GraphMana\nNeo4j DB", DB_COL, size=0.9)

    # Import arrows
    _arrow(ax, 3.3, 6.5, 6.3, 4.8, BLUE, 1.2)
    _arrow(ax, 4.5, 6.5, 6.5, 4.8, BLUE, 1.2)
    _arrow(ax, 5.7, 6.5, 6.8, 4.8, BLUE, 1.2)

    ax.text(4.2, 5.8, "graphmana ingest\n(incremental)", ha="center",
            fontsize=5, color=BLUE, fontweight="bold")

    # Annotation (in-place)
    _icon_file(ax, 11, 6.5, "ClinVar\nv2024", ANNOT_COL, size=0.5)
    _arrow(ax, 10.5, 6.0, 7.8, 4.8, ANNOT_COL, 1)
    ax.text(10, 5.5, "in-place\n3.5 sec", ha="center", fontsize=5,
            color=ANNOT_COL, fontweight="bold")

    # Export fan-out (single command each)
    export_y = 1.0
    formats = [
        (1.5, "VCF", VCF_COL),
        (3.5, "PLINK", PLINK_COL),
        (5.5, "TreeMix", TREEMIX_COL),
        (7.5, "SFS", TREEMIX_COL),
        (9.5, "EIGEN-\nSTRAT", TREEMIX_COL),
        (11.5, "BED", TREEMIX_COL),
    ]

    for fx, flabel, fcol in formats:
        _icon_file(ax, fx, export_y, flabel, fcol, size=0.5)
        _arrow(ax, 7, 3.2, fx, export_y + 0.5, fcol, 0.6)

    ax.text(7, 2.2, "graphmana export --format <fmt>",
            ha="center", fontsize=5.5, color=BLUE, fontweight="bold")
    ax.text(7, 1.8, "17 formats, one command each, manifest auto-generated",
            ha="center", fontsize=4.5, color="#555", fontstyle="italic")

    # Provenance
    benefit_style = dict(boxstyle="round,pad=0.2", facecolor="#E8F5E9",
                         edgecolor="#4CAF50", linewidth=0.5)
    ax.text(1, 4.0, "Provenance\nautomatic",
            ha="center", fontsize=5, color="#2E7D32", fontweight="bold",
            bbox=benefit_style)
    ax.text(12.5, 4.0, "Cohorts as\ngraph queries",
            ha="center", fontsize=5, color="#2E7D32", fontweight="bold",
            bbox=benefit_style)
    ax.text(7, -0.8, "Single source of truth: all data, metadata, and provenance in one database",
            ha="center", fontsize=5.5, color="#2E7D32", fontweight="bold",
            bbox=benefit_style)

    # Collaborator gets subset
    _icon_person(ax, 12.5, 1.5, "Collaborator", PERSON_COL)
    _arrow(ax, 11.8, export_y + 0.3, 12.2, 1.2, PERSON_COL, 0.6)


# ===========================================================================
# EXAMPLE 3: Side-by-side comparison (compact)
# ===========================================================================

def plot_comparison(ax_left, ax_right):
    """Two-panel comparison."""
    plot_conventional(ax_left)
    plot_graphmana(ax_right)


# ===========================================================================
# Main: generate all examples
# ===========================================================================

def main():
    # Example 1: Conventional workflow alone
    fig1, ax1 = plt.subplots(1, 1, figsize=(7.08, 5))
    plot_conventional(ax1)
    fig1.savefig(OUT_DIR / "example_conventional_workflow.png",
                 bbox_inches="tight", pad_inches=0.1)
    fig1.savefig(OUT_DIR / "example_conventional_workflow.pdf",
                 bbox_inches="tight", pad_inches=0.1)
    print(f"Saved example_conventional_workflow.png/pdf")
    plt.close(fig1)

    # Example 2: GraphMana workflow alone
    fig2, ax2 = plt.subplots(1, 1, figsize=(7.08, 5))
    plot_graphmana(ax2)
    fig2.savefig(OUT_DIR / "example_graphmana_workflow.png",
                 bbox_inches="tight", pad_inches=0.1)
    fig2.savefig(OUT_DIR / "example_graphmana_workflow.pdf",
                 bbox_inches="tight", pad_inches=0.1)
    print(f"Saved example_graphmana_workflow.png/pdf")
    plt.close(fig2)

    # Example 3: Side-by-side comparison
    fig3, (ax3l, ax3r) = plt.subplots(1, 2, figsize=(14.16, 5))
    plot_conventional(ax3l)
    plot_graphmana(ax3r)
    fig3.savefig(OUT_DIR / "example_comparison.png",
                 bbox_inches="tight", pad_inches=0.1)
    fig3.savefig(OUT_DIR / "example_comparison.pdf",
                 bbox_inches="tight", pad_inches=0.1)
    print(f"Saved example_comparison.png/pdf")
    plt.close(fig3)

    # Example 4: Stacked (top = conventional, bottom = GraphMana)
    fig4, (ax4t, ax4b) = plt.subplots(2, 1, figsize=(7.08, 10))
    plot_conventional(ax4t)
    plot_graphmana(ax4b)
    fig4.savefig(OUT_DIR / "example_stacked.png",
                 bbox_inches="tight", pad_inches=0.1)
    fig4.savefig(OUT_DIR / "example_stacked.pdf",
                 bbox_inches="tight", pad_inches=0.1)
    print(f"Saved example_stacked.png/pdf")
    plt.close(fig4)


if __name__ == "__main__":
    main()
