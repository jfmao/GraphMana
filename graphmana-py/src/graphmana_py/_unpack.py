"""Genotype unpacking — standalone copy of the core unpack functions.

Duplicated from graphmana-cli so that graphmana-py can be installed
independently without requiring the CLI package.
"""

from __future__ import annotations

import numpy as np


def unpack_genotypes(gt_packed: bytes, n_samples: int) -> np.ndarray:
    """Unpack gt_packed bytes to per-sample genotype codes.

    Args:
        gt_packed: packed genotype bytes (2 bits/sample, LSB-first).
        n_samples: number of samples encoded.

    Returns:
        int8 array: 0=HomRef, 1=Het, 2=HomAlt, 3=Missing.
    """
    arr = np.frombuffer(gt_packed, dtype=np.uint8)
    bits = np.empty(len(arr) * 4, dtype=np.uint8)
    bits[0::4] = arr & 0x03
    bits[1::4] = (arr >> 2) & 0x03
    bits[2::4] = (arr >> 4) & 0x03
    bits[3::4] = (arr >> 6) & 0x03
    return bits[:n_samples].astype(np.int8)


def unpack_phase(phase_packed: bytes, n_samples: int) -> np.ndarray:
    """Unpack phase_packed bytes to per-sample phase bits.

    Args:
        phase_packed: packed phase bytes (1 bit/sample, LSB-first).
        n_samples: number of samples encoded.

    Returns:
        uint8 array: 0=unphased/REF-first, 1=ALT-first for het.
    """
    arr = np.frombuffer(phase_packed, dtype=np.uint8)
    bits = np.empty(len(arr) * 8, dtype=np.uint8)
    for i in range(8):
        bits[i::8] = (arr >> i) & 0x01
    return bits[:n_samples]


def unpack_ploidy(ploidy_packed: bytes | None, n_samples: int) -> np.ndarray:
    """Unpack ploidy_packed bytes to per-sample ploidy values.

    Args:
        ploidy_packed: packed ploidy bytes (1 bit/sample, LSB-first).
            None means all diploid.
        n_samples: number of samples encoded.

    Returns:
        int8 array: 1=haploid, 2=diploid.
    """
    if ploidy_packed is None:
        return np.full(n_samples, 2, dtype=np.int8)
    arr = np.frombuffer(ploidy_packed, dtype=np.uint8)
    bits = np.empty(len(arr) * 8, dtype=np.uint8)
    for i in range(8):
        bits[i::8] = (arr >> i) & 0x01
    # bit=0 → diploid (2), bit=1 → haploid (1)
    result = np.where(bits[:n_samples] == 0, 2, 1).astype(np.int8)
    return result
