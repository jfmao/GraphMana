"""Pack and unpack genotype, phase, and ploidy arrays.

2-bit genotype packing (gt_packed): 00=HomRef, 01=Het, 10=HomAlt, 11=Missing.
1-bit phase packing (phase_packed): which haplotype carries ALT for het sites.
1-bit ploidy packing (ploidy_packed): bit=1 means haploid.

All arrays use LSB-first byte layout for branchless O(1) per-sample access
in the Java PackedGenotypeReader.
"""

from __future__ import annotations

import numpy as np

# Remap cyvcf2 gt_types → packed 2-bit encoding
# cyvcf2: 0=HOM_REF, 1=HET, 2=MISSING, 3=HOM_ALT
# packed: 00=HomRef, 01=Het, 10=HomAlt, 11=Missing
GT_REMAP = np.array([0, 1, 3, 2], dtype=np.uint8)


def vectorized_gt_pack(gt_types: np.ndarray) -> bytes:
    """Pack gt_types into 2 bits/sample using vectorized numpy operations.

    Args:
        gt_types: int8 array of cyvcf2 genotype codes (0-3).

    Returns:
        Packed bytes with 4 samples per byte, LSB-first.
    """
    gt_remapped = GT_REMAP[gt_types]
    n_all = len(gt_remapped)
    n_padded = ((n_all + 3) // 4) * 4
    padded = np.zeros(n_padded, dtype=np.uint8)
    padded[:n_all] = gt_remapped
    groups = padded.reshape(-1, 4)
    packed = groups[:, 0] | (groups[:, 1] << 2) | (groups[:, 2] << 4) | (groups[:, 3] << 6)
    return packed.tobytes()


def build_ploidy_packed(haploid_flags: np.ndarray) -> bytes:
    """Pack boolean haploid flags into bytes (1 bit/sample, LSB first).

    Args:
        haploid_flags: boolean array where True = haploid.

    Returns:
        Packed bytes with 8 samples per byte.
    """
    return np.packbits(haploid_flags.astype(np.uint8), bitorder="little").tobytes()


def pack_phase(n_samples: int, het_indices: np.ndarray, genotypes: list) -> bytes:
    """Pack phase information for heterozygous sites.

    For each het site, records whether ALT is on the second haplotype
    (genotypes[i][1] == 1 → bit set).

    Args:
        n_samples: total number of samples.
        het_indices: int array of sample indices that are heterozygous.
        genotypes: cyvcf2 genotypes list ([a0, a1, is_phased] per sample).

    Returns:
        Packed phase bytes (1 bit/sample, LSB first).
    """
    phase_packed_len = (n_samples + 7) >> 3
    phase_packed = bytearray(phase_packed_len)
    for i in het_indices:
        g = genotypes[i]
        if len(g) >= 3 and g[1] == 1:
            phase_packed[i >> 3] |= 1 << (i & 7)
    return bytes(phase_packed)


def unpack_genotypes(gt_packed: bytes, n_samples: int) -> np.ndarray:
    """Unpack gt_packed bytes back to per-sample genotype codes.

    Args:
        gt_packed: packed genotype bytes from vectorized_gt_pack.
        n_samples: number of samples encoded.

    Returns:
        int8 array of packed codes (0=HomRef, 1=Het, 2=HomAlt, 3=Missing).
    """
    arr = np.frombuffer(gt_packed, dtype=np.uint8)
    bits = np.empty(len(arr) * 4, dtype=np.uint8)
    bits[0::4] = arr & 0x03
    bits[1::4] = (arr >> 2) & 0x03
    bits[2::4] = (arr >> 4) & 0x03
    bits[3::4] = (arr >> 6) & 0x03
    return bits[:n_samples].astype(np.int8)


def unpack_phase(phase_packed: bytes, n_samples: int) -> np.ndarray:
    """Unpack phase bits to per-sample 0/1 array.

    Args:
        phase_packed: packed phase bytes from pack_phase.
        n_samples: number of samples encoded.

    Returns:
        uint8 array of 0/1 phase flags.
    """
    return np.unpackbits(np.frombuffer(phase_packed, dtype=np.uint8), bitorder="little")[:n_samples]


def unpack_ploidy(ploidy_packed: bytes | None, n_samples: int) -> np.ndarray:
    """Unpack ploidy flags to per-sample 0/1 array.

    Args:
        ploidy_packed: packed ploidy bytes from build_ploidy_packed.
            None means all diploid.
        n_samples: number of samples encoded.

    Returns:
        uint8 array: 0=diploid, 1=haploid.
    """
    if ploidy_packed is None:
        return np.zeros(n_samples, dtype=np.uint8)
    return np.unpackbits(np.frombuffer(ploidy_packed, dtype=np.uint8), bitorder="little")[
        :n_samples
    ]
