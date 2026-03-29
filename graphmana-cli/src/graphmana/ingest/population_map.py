"""Population map: load panel/PED files and map VCF samples to populations."""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PopulationMap:
    """Maps samples to populations; built once from the panel file.

    Attributes:
        sample_ids: ordered list of sample IDs present in both VCF and panel.
        pop_ids: sorted unique population labels.
        sample_to_pop: mapping of sample ID to population label.
        pop_to_indices: per-population int32 index arrays into VCF sample order.
        n_samples_per_pop: count of samples per population.
        sample_packed_index: sample ID → VCF column index (for packed arrays).
        n_vcf_samples: total VCF samples (including those not in panel).
        sample_to_sex: sample ID → sex (1=male, 2=female, 0=unknown).
    """

    sample_ids: list[str]
    pop_ids: list[str]
    sample_to_pop: dict[str, str]
    pop_to_indices: dict[str, np.ndarray]
    n_samples_per_pop: dict[str, int]
    sample_packed_index: dict[str, int]
    n_vcf_samples: int = 0
    sample_to_sex: dict[str, int] = field(default_factory=dict)


def load_panel(
    panel_path: str | Path,
    stratify_by: str = "superpopulation",
) -> tuple[dict[str, str], dict[str, int]]:
    """Load a panel/PED file and return sample-to-population and sample-to-sex mappings.

    Auto-detects format from the header line:
    - PED format: has 'SampleID' column, whitespace or tab separated.
    - Panel format: has 'sample' column, tab separated.

    Sex encoding: 1=male, 2=female, 0=unknown.

    Args:
        panel_path: path to panel/PED file.
        stratify_by: 'population' or 'superpopulation' — which column to use.

    Returns:
        (sample_to_pop, sample_to_sex) dictionaries.

    Raises:
        ValueError: if required columns cannot be found.
    """
    panel_path = Path(panel_path)

    with open(panel_path) as f:
        first_line = f.readline()

    # Detect separator and read all rows
    is_ped = "SampleID" in first_line or "sampleID" in first_line
    use_tab = "\t" in first_line

    rows: list[dict[str, str]] = []
    with open(panel_path, newline="") as f:
        if use_tab:
            reader = csv.DictReader(f, delimiter="\t")
        else:
            # Whitespace-separated: split manually
            header_line = f.readline().strip()
            headers = header_line.split()
            for line in f:
                line = line.strip()
                if not line:
                    continue
                values = line.split()
                rows.append(dict(zip(headers, values, strict=False)))

    if not rows and use_tab:
        # csv.DictReader was used — need to re-read
        with open(panel_path, newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            rows = list(reader)

    if not rows:
        raise ValueError(f"No data rows found in {panel_path}")

    # Build case-insensitive column lookup
    actual_cols = list(rows[0].keys())
    col_map = {c.lower(): c for c in actual_cols}

    # Find sample column
    if is_ped:
        sample_col = col_map.get("sampleid", col_map.get("sample"))
    else:
        sample_col = col_map.get("sample", col_map.get("sampleid"))

    # Find population column
    if stratify_by == "superpopulation":
        if is_ped:
            pop_col = col_map.get("superpopulation", col_map.get("super_pop"))
        else:
            pop_col = col_map.get("super_pop", col_map.get("superpopulation"))
    else:
        if is_ped:
            pop_col = col_map.get("population", col_map.get("pop"))
        else:
            pop_col = col_map.get("pop", col_map.get("population"))

    if sample_col is None or pop_col is None:
        raise ValueError(
            f"Cannot find sample/population columns in {panel_path}. "
            f"Columns found: {actual_cols}"
        )

    sample_to_pop: dict[str, str] = {}
    for row in rows:
        sid = row[sample_col]
        pop = row[pop_col]
        sample_to_pop[sid] = pop

    # Extract sex metadata
    sample_to_sex: dict[str, int] = {}
    sex_col = col_map.get("sex", col_map.get("gender"))
    if sex_col is not None:
        for row in rows:
            sid = row[sample_col]
            val = row[sex_col].strip().lower()
            if val in ("1", "male"):
                sample_to_sex[sid] = 1
            elif val in ("2", "female"):
                sample_to_sex[sid] = 2
            else:
                sample_to_sex[sid] = 0

    return sample_to_pop, sample_to_sex


def build_pop_map(
    vcf_samples: list[str],
    sample_to_pop: dict[str, str],
    sample_to_sex: dict[str, int] | None = None,
) -> PopulationMap:
    """Build a PopulationMap by intersecting VCF samples with panel data.

    Args:
        vcf_samples: ordered list of sample IDs from the VCF header.
        sample_to_pop: mapping from sample ID to population label.
        sample_to_sex: optional mapping from sample ID to sex code.

    Returns:
        A PopulationMap with index arrays for per-population slicing.
    """
    sample_ids: list[str] = []
    mapping: dict[str, str] = {}
    missing = 0

    for s in vcf_samples:
        if s in sample_to_pop:
            sample_ids.append(s)
            mapping[s] = sample_to_pop[s]
        else:
            missing += 1

    if missing:
        logger.warning(
            "%d samples in VCF not found in panel — excluded from pop counts",
            missing,
        )

    pop_ids = sorted(set(mapping.values()))

    # Build index arrays (positions in VCF sample order)
    vcf_sample_index = {s: i for i, s in enumerate(vcf_samples)}
    pop_to_indices: dict[str, np.ndarray] = {}
    n_samples_per_pop: dict[str, int] = {}

    for pop in pop_ids:
        indices = np.array(
            [vcf_sample_index[s] for s in sample_ids if mapping[s] == pop],
            dtype=np.int32,
        )
        pop_to_indices[pop] = indices
        n_samples_per_pop[pop] = len(indices)

    sample_packed_index = {s: vcf_sample_index[s] for s in sample_ids}

    return PopulationMap(
        sample_ids=sample_ids,
        pop_ids=pop_ids,
        sample_to_pop=mapping,
        pop_to_indices=pop_to_indices,
        n_samples_per_pop=n_samples_per_pop,
        sample_packed_index=sample_packed_index,
        n_vcf_samples=len(vcf_samples),
        sample_to_sex=sample_to_sex or {},
    )
