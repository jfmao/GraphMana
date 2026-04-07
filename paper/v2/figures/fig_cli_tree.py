#!/usr/bin/env python3
"""Visualize GraphMana CLI as a compact tree with command descriptions.

Usage:
    python paper/figures/fig_cli_tree.py
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch
from pathlib import Path

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"],
    "font.size": 6,
    "figure.dpi": 300,
    "savefig.dpi": 300,
    "pdf.fonttype": 42,
})

OUT_DIR = Path(__file__).resolve().parent

DOMAINS = [
    {
        "name": "Data Import\n& Integration",
        "color": "#388E3C", "bg": "#C8E6C9",
        "functions": [
            ("Data Import", [
                ("ingest", "Import VCF data: generate CSVs and load into Neo4j"),
                ("prepare-csv", "Generate CSV files from VCF (no Neo4j needed)"),
                ("load-csv", "Load pre-generated CSVs into Neo4j"),
            ]),
            ("Merging", [
                ("merge", "Merge a source database into the target"),
            ]),
            ("Liftover", [
                ("liftover", "Convert coordinates between reference assemblies"),
            ]),
        ],
    },
    {
        "name": "Annotation\nManagement",
        "color": "#C2185B", "bg": "#F8BBD0",
        "functions": [
            ("Functional", [
                ("annotate load", "Load annotations from VEP/SnpEff VCF"),
                ("annotate load-clinvar", "Load ClinVar annotations"),
                ("annotate load-cadd", "Load CADD scores from TSV"),
            ]),
            ("Ontology &\nPathway", [
                ("annotate load-go", "Load GO terms from GAF file"),
                ("annotate load-pathway", "Load pathway annotations"),
                ("annotate load-constraint", "Load gene constraint scores"),
            ]),
            ("Region", [
                ("annotate load-bed", "Load BED regions, link variants"),
            ]),
            ("Versioning", [
                ("annotate list", "List annotation versions"),
                ("annotate remove", "Remove an annotation version"),
            ]),
        ],
    },
    {
        "name": "Data\nExport",
        "color": "#E65100", "bg": "#FFE0B2",
        "functions": [
            ("Export", [
                ("export", "Export to 17 formats (VCF, PLINK, TreeMix, SFS, ...)"),
                ("list-formats", "List all formats with FAST/FULL path info"),
            ]),
        ],
    },
    {
        "name": "Sample &\nCohort Mgmt",
        "color": "#0277BD", "bg": "#B3E5FC",
        "functions": [
            ("Sample\nLifecycle", [
                ("sample list", "List samples with status"),
                ("sample remove", "Soft-delete samples"),
                ("sample restore", "Restore soft-deleted samples"),
                ("sample reassign", "Move samples between populations"),
                ("sample hard-remove", "Permanently remove samples"),
            ]),
            ("Cohort\nDefinitions", [
                ("cohort define", "Define or update a named cohort"),
                ("cohort list", "List all cohort definitions"),
                ("cohort show", "Show cohort details"),
                ("cohort count", "Count samples in a cohort"),
                ("cohort validate", "Validate a cohort Cypher query"),
                ("cohort delete", "Delete a cohort"),
            ]),
        ],
    },
    {
        "name": "QC &\nVerification",
        "color": "#558B2F", "bg": "#DCEDC8",
        "functions": [
            ("Quality", [
                ("qc", "Run QC checks, generate HTML/TSV/JSON report"),
                ("ref-check", "Verify REF alleles against FASTA genome"),
                ("db validate", "Check packed arrays, pop arrays, NEXT chains"),
            ]),
        ],
    },
    {
        "name": "Provenance &\nState Tracking",
        "color": "#7B1FA2", "bg": "#E1BEE7",
        "functions": [
            ("Ingestion\nLogs", [
                ("provenance list", "List all ingestion logs"),
                ("provenance show", "Show details of a single log"),
                ("provenance search", "Search by date range or dataset ID"),
                ("provenance summary", "Aggregate provenance statistics"),
                ("provenance headers", "List VCF header records"),
            ]),
            ("State\nComparison", [
                ("save-state", "Save database state summary to JSON"),
                ("diff", "Compare current state against saved summary"),
            ]),
        ],
    },
    {
        "name": "Database\nAdmin",
        "color": "#5D4037", "bg": "#D7CCC8",
        "functions": [
            ("Snapshots", [
                ("snapshot create", "Create snapshot via neo4j-admin dump"),
                ("snapshot restore", "Restore from snapshot"),
                ("snapshot list", "List all snapshots"),
                ("snapshot delete", "Delete a snapshot"),
            ]),
            ("DB Ops", [
                ("db info", "Show database size, location, version"),
                ("db check", "Run Neo4j consistency check"),
                ("db copy", "Copy database to new location"),
                ("db password", "Change Neo4j password"),
            ]),
            ("Other", [
                ("migrate", "Apply pending schema migrations"),
                ("query", "Run Cypher query from command line"),
            ]),
        ],
    },
    {
        "name": "Status &\nReporting",
        "color": "#F57F17", "bg": "#FFF9C4",
        "functions": [
            ("Info", [
                ("status", "Show database status and node counts"),
                ("summary", "Generate human-readable dataset report"),
                ("version", "Show versions of all dependencies"),
                ("config-show", "Display configuration and env variables"),
                ("init", "Initialize a new project directory"),
            ]),
        ],
    },
    {
        "name": "Infrastructure\n& Deployment",
        "color": "#00695C", "bg": "#B2DFDB",
        "functions": [
            ("Neo4j", [
                ("setup-neo4j", "Download and configure Neo4j (user-space)"),
                ("neo4j-start", "Start Neo4j"),
                ("neo4j-stop", "Stop Neo4j"),
            ]),
            ("Cluster", [
                ("cluster check-env", "Verify Java, Neo4j, ports, filesystem"),
                ("cluster generate-job", "Generate SLURM or PBS job script"),
                ("check-filesystem", "Check storage suitability for Neo4j"),
            ]),
        ],
    },
]


def main():
    line_h = 0.20
    func_gap = 0.04
    domain_gap = 0.12
    root_x = 0.0
    domain_x = 1.5
    func_x = 3.3
    cmd_x = 5.2
    desc_x = 7.5
    fig_w = 11.5

    # Scale to fill A4 (aspect ratio 8.27:11.69)
    # Use fixed total_h matching the A4 proportions
    total_h = 14.5

    # A4 page: 210mm x 297mm = 8.27 x 11.69 inches
    fig, ax = plt.subplots(1, 1, figsize=(8.27, 11.69))
    ax.set_xlim(-0.3, fig_w)
    ax.set_ylim(-0.3, total_h + 0.8)
    ax.axis("off")

    # Title
    ax.text(fig_w / 2, total_h + 0.55, "GraphMana CLI Command Hierarchy",
            ha="center", fontsize=13, fontweight="bold", color="#222")
    ax.text(fig_w / 2, total_h + 0.2, "9 domains  |  27 functions  |  58 commands",
            ha="center", fontsize=8, color="#666")

    # Root
    root_y = total_h / 2
    rb = FancyBboxPatch((root_x, root_y - 0.25), 1.3, 0.5,
                         boxstyle="round,pad=0.06", facecolor="#2166AC",
                         edgecolor="white", linewidth=1.2)
    ax.add_patch(rb)
    ax.text(root_x + 0.65, root_y, "graphmana", ha="center", va="center",
            fontsize=7.5, fontweight="bold", color="white", fontfamily="monospace")

    y_cursor = total_h - 0.2

    for di, d in enumerate(DOMAINS):
        n_lines = sum(len(f[1]) for f in d["functions"])
        n_fg = len(d["functions"]) - 1
        domain_h = n_lines * line_h + n_fg * func_gap + 0.12
        domain_top = y_cursor
        domain_bot = y_cursor - domain_h
        domain_mid = (domain_top + domain_bot) / 2

        # Background stripe
        bg = FancyBboxPatch(
            (domain_x - 0.2, domain_bot - 0.02), fig_w - domain_x + 0.4, domain_h + 0.04,
            boxstyle="round,pad=0.04", facecolor=d["bg"], edgecolor="none", alpha=0.35)
        ax.add_patch(bg)

        # Domain box
        dbox_h = min(0.45, domain_h * 0.6)
        dbox = FancyBboxPatch((domain_x - 0.15, domain_mid - dbox_h/2), 1.5, dbox_h,
                               boxstyle="round,pad=0.04", facecolor=d["color"],
                               edgecolor="white", linewidth=0.8, alpha=0.9)
        ax.add_patch(dbox)
        ax.text(domain_x + 0.6, domain_mid, d["name"], ha="center", va="center",
                fontsize=5.5, fontweight="bold", color="white")

        # Root branch
        ax.plot([root_x + 1.3, domain_x - 0.15], [root_y, domain_mid],
                color=d["color"], linewidth=0.9, alpha=0.5)

        fy_cursor = domain_top - 0.06

        for fi, (func_name, commands) in enumerate(d["functions"]):
            func_block_h = len(commands) * line_h
            func_mid = fy_cursor - func_block_h / 2

            # Function box
            fb_h = min(0.34, func_block_h * 0.8)
            fbox = FancyBboxPatch((func_x - 0.15, func_mid - fb_h/2), 1.5, fb_h,
                                   boxstyle="round,pad=0.03", facecolor=d["bg"],
                                   edgecolor=d["color"], linewidth=0.5)
            ax.add_patch(fbox)
            ax.text(func_x + 0.6, func_mid, func_name, ha="center", va="center",
                    fontsize=5, fontweight="bold", color=d["color"])

            # Domain-to-function branch
            ax.plot([domain_x + 1.35, func_x - 0.15], [domain_mid, func_mid],
                    color=d["color"], linewidth=0.5, alpha=0.4)

            # Vertical tree line for commands
            if len(commands) > 1:
                first_cy = fy_cursor - line_h / 2
                last_cy = fy_cursor - (len(commands) - 1) * line_h - line_h / 2
                tree_x = cmd_x - 0.35
                ax.plot([tree_x, tree_x], [first_cy, last_cy],
                        color=d["color"], linewidth=0.4, alpha=0.4)
                ax.plot([func_x + 1.35, tree_x], [func_mid, func_mid],
                        color=d["color"], linewidth=0.4, alpha=0.4)

            for ci, (cmd, desc) in enumerate(commands):
                cy = fy_cursor - ci * line_h - line_h / 2
                tree_x = cmd_x - 0.35

                if len(commands) == 1:
                    ax.plot([func_x + 1.35, tree_x + 0.15], [func_mid, cy],
                            color=d["color"], linewidth=0.4, alpha=0.4)
                else:
                    ax.plot([tree_x, tree_x + 0.15], [cy, cy],
                            color=d["color"], linewidth=0.4, alpha=0.4)

                # Command name (monospace, bold)
                ax.text(cmd_x, cy, cmd, va="center", fontsize=5.5,
                        fontfamily="monospace", fontweight="bold", color="#333")

                # Description (right-hand, lighter)
                ax.text(desc_x, cy, desc, va="center", fontsize=5, color="#666")

            fy_cursor -= func_block_h + func_gap

        y_cursor = domain_bot - domain_gap

    out_png = OUT_DIR / "fig_cli_tree.png"
    out_pdf = OUT_DIR / "fig_cli_tree.pdf"
    fig.savefig(out_png, bbox_inches="tight", pad_inches=0.15)
    fig.savefig(out_pdf, bbox_inches="tight", pad_inches=0.15)
    print(f"Saved {out_png} and {out_pdf}")
    plt.close(fig)


if __name__ == "__main__":
    main()
