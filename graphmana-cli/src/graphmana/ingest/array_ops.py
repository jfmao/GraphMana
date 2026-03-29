"""Array extension utilities for incremental sample addition.

Pure numpy functions for extending packed genotype, phase, and ploidy arrays
when new samples are appended to an existing database.
"""

from __future__ import annotations

import numpy as np

from graphmana.ingest.genotype_packer import (
    GT_REMAP,
    unpack_genotypes,
    unpack_phase,
    unpack_ploidy,
    vectorized_gt_pack,
)


def _pack_codes_direct(codes: np.ndarray) -> bytes:
    """Pack already-remapped 2-bit codes (NOT cyvcf2 codes) into bytes.

    Unlike vectorized_gt_pack, this skips the GT_REMAP step because
    the input codes are already in packed encoding (0=HomRef, 1=Het,
    2=HomAlt, 3=Missing).

    Args:
        codes: uint8 array of packed genotype codes (0-3).

    Returns:
        Packed bytes with 4 samples per byte, LSB-first.
    """
    n = len(codes)
    if n == 0:
        return b""
    n_padded = ((n + 3) // 4) * 4
    padded = np.zeros(n_padded, dtype=np.uint8)
    padded[:n] = codes
    groups = padded.reshape(-1, 4)
    packed = groups[:, 0] | (groups[:, 1] << 2) | (groups[:, 2] << 4) | (groups[:, 3] << 6)
    return packed.tobytes()


def _pack_bits(bits: np.ndarray) -> bytes:
    """Pack a 0/1 uint8 array into bytes (1 bit/sample, LSB-first)."""
    if len(bits) == 0:
        return b""
    return np.packbits(bits.astype(np.uint8), bitorder="little").tobytes()


def extend_gt_packed(
    existing: bytes,
    n_existing: int,
    new_gt_types: np.ndarray,
) -> bytes:
    """Extend gt_packed with new samples' cyvcf2 gt_types.

    Unpacks existing (already in packed codes), remaps new (cyvcf2 → packed),
    concatenates, and repacks.

    Args:
        existing: current gt_packed bytes from the database.
        n_existing: number of samples in existing packed array.
        new_gt_types: int8 array of cyvcf2 genotype codes for new samples.

    Returns:
        New gt_packed bytes covering n_existing + len(new_gt_types) samples.
    """
    if n_existing == 0:
        return vectorized_gt_pack(new_gt_types)

    old_codes = unpack_genotypes(existing, n_existing)
    new_codes = GT_REMAP[new_gt_types].astype(np.int8)
    all_codes = np.concatenate([old_codes, new_codes])
    return _pack_codes_direct(all_codes.astype(np.uint8))


def extend_phase_packed(
    existing: bytes,
    n_existing: int,
    new_phase: np.ndarray,
) -> bytes:
    """Extend phase_packed with new samples' phase bits.

    Args:
        existing: current phase_packed bytes from the database.
        n_existing: number of samples in existing packed array.
        new_phase: uint8 array of 0/1 phase flags for new samples.

    Returns:
        New phase_packed bytes covering n_existing + len(new_phase) samples.
    """
    if n_existing == 0:
        return _pack_bits(new_phase)

    old_bits = unpack_phase(existing, n_existing)
    all_bits = np.concatenate([old_bits, new_phase])
    return _pack_bits(all_bits)


def extend_ploidy_packed(
    existing: bytes | None,
    n_existing: int,
    new_haploid: np.ndarray,
) -> bytes | None:
    """Extend ploidy_packed with new samples' haploid flags.

    Args:
        existing: current ploidy_packed bytes (None if all diploid).
        n_existing: number of samples in existing packed array.
        new_haploid: uint8 array of 0/1 haploid flags for new samples.

    Returns:
        New ploidy_packed bytes, or None if all samples are diploid.
    """
    old_bits = unpack_ploidy(existing, n_existing)
    all_bits = np.concatenate([old_bits, new_haploid])
    if not np.any(all_bits):
        return None
    return _pack_bits(all_bits)


def pad_gt_for_new_variant(
    n_existing: int,
    new_gt_types: np.ndarray,
) -> bytes:
    """Create gt_packed for a NEW variant: n_existing HomRef + new samples.

    Args:
        n_existing: number of existing samples (get HomRef = code 0).
        new_gt_types: int8 array of cyvcf2 genotype codes for new samples.

    Returns:
        Packed gt bytes for n_existing + len(new_gt_types) samples.
    """
    prefix = np.zeros(n_existing, dtype=np.uint8)  # HomRef = 0 in packed codes
    new_codes = GT_REMAP[new_gt_types].astype(np.uint8)
    all_codes = np.concatenate([prefix, new_codes])
    return _pack_codes_direct(all_codes)


def pad_phase_for_new_variant(
    n_existing: int,
    new_phase: np.ndarray,
) -> bytes:
    """Create phase_packed for a NEW variant: n_existing zeros + new.

    Args:
        n_existing: number of existing samples (get phase = 0).
        new_phase: uint8 array of 0/1 phase flags for new samples.

    Returns:
        Packed phase bytes for n_existing + len(new_phase) samples.
    """
    prefix = np.zeros(n_existing, dtype=np.uint8)
    all_bits = np.concatenate([prefix, new_phase])
    return _pack_bits(all_bits)


def concatenate_gt_packed(
    target_bytes: bytes,
    n_target: int,
    source_bytes: bytes,
    n_source: int,
) -> bytes:
    """Concatenate two packed genotype arrays (both already in packed encoding).

    Unlike extend_gt_packed which takes raw cyvcf2 codes and remaps them,
    this function takes two already-packed byte arrays and concatenates them.
    Both inputs must already be in packed encoding (0=HomRef, 1=Het,
    2=HomAlt, 3=Missing).

    Args:
        target_bytes: gt_packed bytes from the target database.
        n_target: number of samples in target packed array.
        source_bytes: gt_packed bytes from the source database.
        n_source: number of samples in source packed array.

    Returns:
        Combined gt_packed bytes covering n_target + n_source samples.
    """
    if n_target == 0:
        return source_bytes
    if n_source == 0:
        return target_bytes

    target_codes = unpack_genotypes(target_bytes, n_target)
    source_codes = unpack_genotypes(source_bytes, n_source)
    combined = np.concatenate([target_codes, source_codes])
    return _pack_codes_direct(combined.astype(np.uint8))


def concatenate_phase_packed(
    target_bytes: bytes,
    n_target: int,
    source_bytes: bytes,
    n_source: int,
) -> bytes:
    """Concatenate two packed phase arrays.

    Args:
        target_bytes: phase_packed bytes from the target database.
        n_target: number of samples in target packed array.
        source_bytes: phase_packed bytes from the source database.
        n_source: number of samples in source packed array.

    Returns:
        Combined phase_packed bytes covering n_target + n_source samples.
    """
    if n_target == 0:
        return source_bytes
    if n_source == 0:
        return target_bytes

    target_bits = unpack_phase(target_bytes, n_target)
    source_bits = unpack_phase(source_bytes, n_source)
    combined = np.concatenate([target_bits, source_bits])
    return _pack_bits(combined)


def concatenate_ploidy_packed(
    target_bytes: bytes | None,
    n_target: int,
    source_bytes: bytes | None,
    n_source: int,
) -> bytes | None:
    """Concatenate two packed ploidy arrays.

    Args:
        target_bytes: ploidy_packed bytes from target (None if all diploid).
        n_target: number of samples in target.
        source_bytes: ploidy_packed bytes from source (None if all diploid).
        n_source: number of samples in source.

    Returns:
        Combined ploidy_packed bytes, or None if all samples are diploid.
    """
    target_bits = unpack_ploidy(target_bytes, n_target)
    source_bits = unpack_ploidy(source_bytes, n_source)
    combined = np.concatenate([target_bits, source_bits])
    if not np.any(combined):
        return None
    return _pack_bits(combined)


def _genotype_contributions(gt_codes: np.ndarray) -> dict:
    """Compute aggregate genotype contributions for a batch of samples.

    Args:
        gt_codes: uint8 array of packed genotype codes (0-3) for
            one or more samples at a single variant.

    Returns:
        Dict with ac_delta, an_delta, het_delta, hom_alt_delta.
    """
    ac_delta = 0
    an_delta = 0
    het_delta = 0
    hom_alt_delta = 0
    for gt in gt_codes:
        if gt == 0:  # HomRef
            an_delta += 2
        elif gt == 1:  # Het
            ac_delta += 1
            an_delta += 2
            het_delta += 1
        elif gt == 2:  # HomAlt
            ac_delta += 2
            an_delta += 2
            hom_alt_delta += 1
        # gt == 3 (Missing): no change
    return {
        "ac_delta": ac_delta,
        "an_delta": an_delta,
        "het_delta": het_delta,
        "hom_alt_delta": hom_alt_delta,
    }


def reassign_pop_stats(
    pop_ids: list[str],
    ac: list[int],
    an: list[int],
    het_count: list[int],
    hom_alt_count: list[int],
    gt_codes: np.ndarray,
    old_pop_id: str,
    new_pop_id: str,
) -> dict:
    """Update population arrays after reassigning samples between populations.

    Args:
        pop_ids: Current sorted list of population IDs.
        ac, an, het_count, hom_alt_count: Current per-pop count arrays.
        gt_codes: Genotype codes for the samples being reassigned at this variant.
        old_pop_id: Population to subtract from.
        new_pop_id: Population to add to.

    Returns:
        Dict with updated pop_ids, ac, an, af, het_count, hom_alt_count,
        het_exp, ac_total, an_total, af_total.
    """
    contribs = _genotype_contributions(gt_codes)

    # Build mutable copies indexed by pop_id
    pop_map = {pid: i for i, pid in enumerate(pop_ids)}
    m_ac = list(ac)
    m_an = list(an)
    m_het = list(het_count)
    m_hom = list(hom_alt_count)

    # If new_pop_id not in pop_ids yet, insert it
    if new_pop_id not in pop_map:
        # Insert in sorted order
        merged_pids = sorted(set(pop_ids) | {new_pop_id})
        insert_idx = merged_pids.index(new_pop_id)
        m_ac.insert(insert_idx, 0)
        m_an.insert(insert_idx, 0)
        m_het.insert(insert_idx, 0)
        m_hom.insert(insert_idx, 0)
        pop_map = {pid: i for i, pid in enumerate(merged_pids)}
    else:
        merged_pids = list(pop_ids)

    # Subtract from old population
    old_idx = pop_map[old_pop_id]
    m_ac[old_idx] -= contribs["ac_delta"]
    m_an[old_idx] -= contribs["an_delta"]
    m_het[old_idx] -= contribs["het_delta"]
    m_hom[old_idx] -= contribs["hom_alt_delta"]

    # Add to new population
    new_idx = pop_map[new_pop_id]
    m_ac[new_idx] += contribs["ac_delta"]
    m_an[new_idx] += contribs["an_delta"]
    m_het[new_idx] += contribs["het_delta"]
    m_hom[new_idx] += contribs["hom_alt_delta"]

    # Derived stats
    m_af = [a / n if n > 0 else 0.0 for a, n in zip(m_ac, m_an)]
    m_het_exp = [2.0 * f * (1.0 - f) for f in m_af]
    ac_total = sum(m_ac)
    an_total = sum(m_an)
    af_total = ac_total / an_total if an_total > 0 else 0.0

    return {
        "pop_ids": merged_pids,
        "ac": m_ac,
        "an": m_an,
        "af": m_af,
        "het_count": m_het,
        "hom_alt_count": m_hom,
        "het_exp": m_het_exp,
        "ac_total": ac_total,
        "an_total": an_total,
        "af_total": af_total,
    }


def subtract_sample_from_pop_stats(
    pop_ids: list[str],
    ac: list[int],
    an: list[int],
    het_count: list[int],
    hom_alt_count: list[int],
    gt_codes: np.ndarray,
    pop_id: str,
) -> dict:
    """Subtract sample contributions from population stats (for hard delete).

    Args:
        pop_ids: Current sorted list of population IDs.
        ac, an, het_count, hom_alt_count: Current per-pop count arrays.
        gt_codes: Genotype codes for the samples being removed at this variant.
        pop_id: Population to subtract from.

    Returns:
        Dict with updated pop_ids, ac, an, af, het_count, hom_alt_count,
        het_exp, ac_total, an_total, af_total.
    """
    contribs = _genotype_contributions(gt_codes)

    pop_map = {pid: i for i, pid in enumerate(pop_ids)}
    m_ac = list(ac)
    m_an = list(an)
    m_het = list(het_count)
    m_hom = list(hom_alt_count)

    idx = pop_map[pop_id]
    m_ac[idx] -= contribs["ac_delta"]
    m_an[idx] -= contribs["an_delta"]
    m_het[idx] -= contribs["het_delta"]
    m_hom[idx] -= contribs["hom_alt_delta"]

    m_af = [a / n if n > 0 else 0.0 for a, n in zip(m_ac, m_an)]
    m_het_exp = [2.0 * f * (1.0 - f) for f in m_af]
    ac_total = sum(m_ac)
    an_total = sum(m_an)
    af_total = ac_total / an_total if an_total > 0 else 0.0

    return {
        "pop_ids": list(pop_ids),
        "ac": m_ac,
        "an": m_an,
        "af": m_af,
        "het_count": m_het,
        "hom_alt_count": m_hom,
        "het_exp": m_het_exp,
        "ac_total": ac_total,
        "an_total": an_total,
        "af_total": af_total,
    }


def zero_out_gt_packed(gt_packed: bytes, packed_indices: list[int]) -> bytes:
    """Zero out specific sample slots in gt_packed by setting them to Missing (code 3).

    Args:
        gt_packed: Current gt_packed byte array.
        packed_indices: List of packed_index values to zero out.

    Returns:
        Modified gt_packed bytes with specified slots set to code 3 (Missing).
    """
    arr = bytearray(gt_packed)
    for idx in packed_indices:
        byte_pos = idx // 4
        bit_offset = (idx % 4) * 2
        if byte_pos < len(arr):
            # Clear the 2 bits, then set to 11 (Missing = 3)
            arr[byte_pos] |= 0x03 << bit_offset
    return bytes(arr)


def zero_out_phase_packed(phase_packed: bytes, packed_indices: list[int]) -> bytes:
    """Zero out specific sample slots in phase_packed.

    Args:
        phase_packed: Current phase_packed byte array.
        packed_indices: List of packed_index values to zero out.

    Returns:
        Modified phase_packed bytes with specified bits cleared.
    """
    arr = bytearray(phase_packed)
    for idx in packed_indices:
        byte_pos = idx // 8
        bit_offset = idx % 8
        if byte_pos < len(arr):
            arr[byte_pos] &= ~(1 << bit_offset)
    return bytes(arr)


def merge_pop_stats(
    existing_pop_ids: list[str],
    existing_ac: list[int],
    existing_an: list[int],
    existing_het_count: list[int],
    existing_hom_alt_count: list[int],
    new_pop_ids: list[str],
    new_ac: list[int],
    new_an: list[int],
    new_het_count: list[int],
    new_hom_alt_count: list[int],
) -> dict:
    """Merge population-level statistics from existing and new data.

    Population arrays maintain sorted order by population ID. Overlapping
    populations have their counts summed; populations unique to either side
    are preserved.

    Args:
        existing_pop_ids: sorted list of existing population IDs.
        existing_ac: allele counts per existing population.
        existing_an: allele numbers per existing population.
        existing_het_count: het counts per existing population.
        existing_hom_alt_count: hom-alt counts per existing population.
        new_pop_ids: sorted list of new population IDs.
        new_ac: allele counts per new population.
        new_an: allele numbers per new population.
        new_het_count: het counts per new population.
        new_hom_alt_count: hom-alt counts per new population.

    Returns:
        Dict with keys: pop_ids, ac, an, af, het_count, hom_alt_count,
        het_exp, ac_total, an_total, af_total, call_rate.
        Note: call_rate requires n_total_samples to be accurate;
        the caller must set it separately.
    """
    # Build lookup dicts for existing
    old_map = {pid: i for i, pid in enumerate(existing_pop_ids)}
    new_map = {pid: i for i, pid in enumerate(new_pop_ids)}

    # Merged sorted pop_ids
    merged_pids = sorted(set(existing_pop_ids) | set(new_pop_ids))

    m_ac = []
    m_an = []
    m_het = []
    m_hom = []

    for pid in merged_pids:
        ac_val = 0
        an_val = 0
        het_val = 0
        hom_val = 0

        if pid in old_map:
            j = old_map[pid]
            ac_val += existing_ac[j]
            an_val += existing_an[j]
            het_val += existing_het_count[j]
            hom_val += existing_hom_alt_count[j]

        if pid in new_map:
            k = new_map[pid]
            ac_val += new_ac[k]
            an_val += new_an[k]
            het_val += new_het_count[k]
            hom_val += new_hom_alt_count[k]

        m_ac.append(ac_val)
        m_an.append(an_val)
        m_het.append(het_val)
        m_hom.append(hom_val)

    # Derived stats
    m_af = [a / n if n > 0 else 0.0 for a, n in zip(m_ac, m_an)]
    m_het_exp = [2.0 * f * (1.0 - f) for f in m_af]

    ac_total = sum(m_ac)
    an_total = sum(m_an)
    af_total = ac_total / an_total if an_total > 0 else 0.0

    return {
        "pop_ids": merged_pids,
        "ac": m_ac,
        "an": m_an,
        "af": m_af,
        "het_count": m_het,
        "hom_alt_count": m_hom,
        "het_exp": m_het_exp,
        "ac_total": ac_total,
        "an_total": an_total,
        "af_total": af_total,
    }
