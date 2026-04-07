#!/usr/bin/env Rscript
# ==========================================================================
# Figure 2: 1000 Genomes Project Benchmark
# GraphMana Brief Communication — Nature Methods
#
# Generates a 3-panel figure using ggplot2 + ggpubr:
#   (a) Lifecycle simulation — horizontal stacked bars
#   (b) Head-to-head task comparison — grouped bars
#   (c) Export format breadth — tile grid
#
# Usage:
#   Rscript paper/figures/fig2_benchmark.R
# ==========================================================================

suppressPackageStartupMessages({
  library(ggplot2)
  library(ggpubr)
  library(dplyr)
  library(tidyr)
  library(scales)
  library(jsonlite)
})

# --- Global theme ---
theme_set(theme_bw())

# --- Paths ---
# Detect script location; fall back to working directory
args <- commandArgs(trailingOnly = FALSE)
script_arg <- grep("--file=", args, value = TRUE)
if (length(script_arg) > 0) {
  script_dir <- dirname(normalizePath(sub("--file=", "", script_arg[1])))
} else {
  script_dir <- "paper/figures"
}
data_file  <- normalizePath(file.path(script_dir, "../../benchmarks/results/comprehensive_comprehensive.jsonl"))
out_dir    <- script_dir

# --- Colors ---
GRAPHMANA_BLUE <- "#2166AC"
BCFTOOLS_ORANGE <- "#E08214"
FAST_GREEN <- "#4DAF4A"
FULL_PURPLE <- "#984EA3"

CAT_COLORS <- c(
  "Import"           = "#66C2A5",
  "Incremental"      = "#FC8D62",
  "Annotation"       = "#E78AC3",
  "FAST export"      = "#4DAF4A",
  "VCF/cohort export"= "#984EA3",
  "Not supported"    = "#B0B0B0"
)

# --- Load data ---
cat("Reading benchmark data from:", data_file, "\n")
raw <- stream_in(file(data_file), verbose = FALSE)

# ==========================================================================
# Panel (a): Lifecycle simulation — horizontal stacked bars
# ==========================================================================

lifecycle <- raw %>% filter(benchmark == "lifecycle")

# Categorize operations
categorize_op <- function(op) {
  case_when(
    grepl("prepare_csv|load_csv|copy_base", op) ~ "Import",
    grepl("incremental|merge", op)               ~ "Incremental",
    grepl("annotate", op)                        ~ "Annotation",
    grepl("treemix|sfs|bed", op)                 ~ "FAST export",
    grepl("vcf|plink|eigenstrat", op)            ~ "VCF/cohort export",
    TRUE                                         ~ "Other"
  )
}

lifecycle <- lifecycle %>%
  mutate(
    category = categorize_op(operation),
    time_min = ifelse(is.na(elapsed_s), 0, elapsed_s) / 60,
    tool_label = ifelse(tool == "graphmana", "GraphMana", "bcftools")
  )

# For not_supported operations, assign a small token time for visibility
lifecycle <- lifecycle %>%
  mutate(
    category = ifelse(status == "not_supported", "Not supported", category),
    time_min = ifelse(status == "not_supported", 0, time_min)
  )

# Aggregate by tool and category
agg_life <- lifecycle %>%
  group_by(tool_label, category) %>%
  summarise(total_min = sum(time_min), n_ops = n(), .groups = "drop")

# Compute summary labels
life_summary <- lifecycle %>%
  group_by(tool_label) %>%
  summarise(
    total_min = sum(time_min),
    n_ok = sum(status == "ok"),
    n_na = sum(status == "not_supported"),
    .groups = "drop"
  ) %>%
  mutate(label = ifelse(
    n_na > 0,
    sprintf("%.0f min, %d ops (%d N/A)", total_min, n_ok, n_na),
    sprintf("%.0f min, %d ops", total_min, n_ok)
  ))

# Order categories
cat_order <- c("Import", "Incremental", "Annotation", "FAST export", "VCF/cohort export", "Not supported")
agg_life$category <- factor(agg_life$category, levels = cat_order)
agg_life$tool_label <- factor(agg_life$tool_label, levels = c("bcftools", "GraphMana"))

panel_a <- ggplot(agg_life, aes(x = total_min, y = tool_label, fill = category)) +
  geom_bar(stat = "identity", position = "stack", width = 0.6, color = "white", linewidth = 0.3) +
  scale_fill_manual(values = CAT_COLORS, name = NULL) +
  geom_text(data = life_summary,
            aes(x = total_min + 2, y = tool_label, label = label),
            inherit.aes = FALSE, hjust = 0, size = 2.5, color = "#333333") +
  scale_x_continuous(expand = expansion(mult = c(0, 0.35))) +
  labs(x = "Cumulative wall time (min)", y = NULL) +
  theme(
    legend.position = "bottom",
    legend.key.size = unit(3, "mm"),
    legend.text = element_text(size = 6),
    axis.text = element_text(size = 7),
    axis.title = element_text(size = 8),
    panel.grid.major.y = element_blank()
  ) +
  guides(fill = guide_legend(nrow = 1))


# ==========================================================================
# Panel (b): Head-to-head task comparison — grouped bars
# ==========================================================================

# Aggregate benchmark-level totals (excluding lifecycle)
task_data <- raw %>%
  filter(benchmark != "lifecycle", status == "ok") %>%
  group_by(benchmark, tool) %>%
  summarise(total_s = sum(ifelse(is.na(elapsed_s), 0, elapsed_s)),
            n_ops = n(), .groups = "drop") %>%
  mutate(tool_label = ifelse(tool == "graphmana", "GraphMana", "bcftools"))

# Also count not_supported for multiformat
mf_na <- raw %>%
  filter(benchmark == "multiformat", status == "not_supported") %>%
  nrow()

# Reshape for plotting
task_labels <- c(
  "incremental"       = "Incremental\nadd (3 batches)",
  "cohort_extraction" = "Cohort\nextraction (5)",
  "annotation"        = "Annotation\nupdate",
  "multiformat"       = "Multi-format\nexport"
)

task_data <- task_data %>%
  filter(benchmark %in% names(task_labels)) %>%
  mutate(task = factor(task_labels[benchmark], levels = task_labels))

task_data$tool_label <- factor(task_data$tool_label, levels = c("GraphMana", "bcftools"))

# Annotation speedup
ann_gm <- task_data %>% filter(benchmark == "annotation", tool == "graphmana") %>% pull(total_s)
ann_bc <- task_data %>% filter(benchmark == "annotation", tool == "bcftools") %>% pull(total_s)
speedup <- round(ann_bc / ann_gm)

panel_b <- ggplot(task_data, aes(x = task, y = total_s, fill = tool_label)) +
  geom_bar(stat = "identity", position = position_dodge(width = 0.7),
           width = 0.6, color = "white", linewidth = 0.3) +
  scale_fill_manual(values = c("GraphMana" = GRAPHMANA_BLUE, "bcftools" = BCFTOOLS_ORANGE),
                    name = NULL) +
  # 27x speedup annotation
  annotate("text", x = 3.0, y = max(task_data$total_s) * 0.55,
           label = paste0(speedup, "x faster"),
           size = 3, fontface = "bold", color = GRAPHMANA_BLUE) +
  annotate("segment", x = 3.0, xend = 2.85, y = max(task_data$total_s) * 0.48,
           yend = ann_gm + 30,
           arrow = arrow(length = unit(1.5, "mm")),
           color = GRAPHMANA_BLUE, linewidth = 0.4) +
  # 6 formats vs 1
  annotate("text", x = 4.15, y = max(task_data$total_s) * 0.65,
           label = "6 formats\nvs 1",
           size = 2.3, color = "#555555") +
  labs(x = NULL, y = "Wall time (s)") +
  theme(
    legend.position = "top",
    legend.key.size = unit(3, "mm"),
    legend.text = element_text(size = 7),
    axis.text.x = element_text(size = 6.5),
    axis.text.y = element_text(size = 7),
    axis.title = element_text(size = 8),
    panel.grid.major.x = element_blank()
  )


# ==========================================================================
# Panel (c): Export format breadth — tile grid
# ==========================================================================

formats <- tribble(
  ~format,        ~path,  ~row, ~col,
  "TreeMix",      "FAST",  1,    1,
  "SFS\n(dadi)",  "FAST",  1,    2,
  "SFS\n(fsc)",   "FAST",  1,    3,
  "BED",          "FAST",  1,    4,
  "TSV",          "FAST",  1,    5,
  "JSON",         "FAST",  1,    6,
  "VCF",          "FULL",  2,    1,
  "PLINK\n1.9",   "FULL",  2,    2,
  "PLINK\n2.0",   "FULL",  2,    3,
  "EIGEN-\nSTRAT","FULL",  2,    4,
  "Beagle",       "FULL",  2,    5,
  "STRUC-\nTURE", "FULL",  2,    6,
  "Gene-\npop",   "FULL",  3,    1,
  "Haplo-\ntype", "FULL",  3,    2,
  "BGEN",         "FULL",  3,    3,
  "GDS",          "FULL",  3,    4,
  "Zarr",         "FULL",  3,    5,
)

# Add path labels
path_labels <- tibble(
  label = c("FAST\nPATH", "FULL\nPATH"),
  row = c(1, 2.5),
  col = c(0, 0)
)

panel_c <- ggplot(formats, aes(x = col, y = -row)) +
  geom_tile(aes(fill = path), width = 0.9, height = 0.85, color = "white", linewidth = 0.5) +
  geom_text(aes(label = format), size = 2.3, fontface = "bold", color = "white") +
  scale_fill_manual(values = c("FAST" = FAST_GREEN, "FULL" = FULL_PURPLE), guide = "none") +
  # Path labels on the left
  geom_text(data = path_labels, aes(x = col, y = -row, label = label),
            inherit.aes = FALSE, size = 2.5, fontface = "bold",
            color = c(FAST_GREEN, FULL_PURPLE)) +
  scale_x_continuous(limits = c(-0.7, 6.7), expand = c(0, 0)) +
  scale_y_continuous(limits = c(-3.7, -0.3), expand = c(0, 0)) +
  coord_fixed(ratio = 1) +
  labs(title = "17 export formats from a single database") +
  theme_bw() +
  theme(
    plot.title = element_text(size = 8, face = "bold", hjust = 0.5),
    plot.margin = margin(5, 5, 5, 5),
    panel.grid = element_blank(),
    panel.border = element_blank(),
    axis.text = element_blank(),
    axis.ticks = element_blank(),
    axis.title = element_blank(),
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA)
  )


# ==========================================================================
# Combine panels with ggarrange
# ==========================================================================

fig <- ggarrange(
  panel_a, panel_b, panel_c,
  ncol = 1, nrow = 3,
  labels = c("a", "b", "c"),
  font.label = list(size = 11, face = "bold"),
  heights = c(1, 1.3, 0.9)
)

# Title
fig <- annotate_figure(fig,
  top = text_grob("1000 Genomes Project benchmark (chr22, 1.07M variants, 3,202 samples)",
                  size = 10, face = "bold", color = "#333333")
)

# Save with white background
out_pdf <- file.path(out_dir, "fig2_benchmark_ggplot.pdf")
out_png <- file.path(out_dir, "fig2_benchmark_ggplot.png")
ggsave(out_pdf, fig, width = 180, height = 230, units = "mm", dpi = 300, bg = "white")
ggsave(out_png, fig, width = 180, height = 230, units = "mm", dpi = 300, bg = "white")
cat("Saved:", out_pdf, "\n")
cat("Saved:", out_png, "\n")
