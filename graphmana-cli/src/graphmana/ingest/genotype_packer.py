"""Pack and unpack genotype, phase, ploidy, and called arrays.

2-bit genotype packing (gt_packed): 00=HomRef, 01=Het, 10=HomAlt, 11=Missing.
1-bit phase packing (phase_packed): which haplotype carries ALT for het sites.
1-bit ploidy packing (ploidy_packed): bit=1 means haploid.
1-bit called packing (called_packed): bit=1 means sample was interrogated at this
    site; bit=0 means sample was not looked at (distinct from "looked and missing",
    which is gt=11 with called=1). See docs/gvcf-workflow.md.

All arrays use LSB-first byte layout for branchless O(1) per-sample access
in the Java PackedGenotypeReader.

Sparse gt_packed encoding (v1.1): variants dominated by HomRef samples may be
stored in a sparse format to save space. A sparse blob is prefixed with the byte
0x01 and contains only the positions and codes of non-HomRef samples. Dense
blobs are prefixed with 0x00 (or have no prefix, for schema v1.0 back-compat).
Callers should use :func:`encode_gt_blob` / :func:`decode_gt_blob` at the
storage boundary; in-memory code continues to use the raw dense byte array.
"""

from __future__ import annotations

import numpy as np

# Remap cyvcf2 gt_types → packed 2-bit encoding
# cyvcf2: 0=HOM_REF, 1=HET, 2=MISSING, 3=HOM_ALT
# packed: 00=HomRef, 01=Het, 10=HomAlt, 11=Missing
GT_REMAP = np.array([0, 1, 3, 2], dtype=np.uint8)

# Format tags for gt_packed storage blob
GT_BLOB_DENSE = 0x00
GT_BLOB_SPARSE = 0x01


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


def build_called_packed(gt_types: np.ndarray) -> bytes:
    """Pack a per-sample "interrogated" bit array (1 = called, 0 = not interrogated).

    cyvcf2 genotype code 2 (``MISSING``) indicates ``./.`` in the source VCF, which
    for a joint-called input means the sample was not successfully interrogated at
    this position. Any other code (HomRef/Het/HomAlt) implies a real call.

    Args:
        gt_types: int array of cyvcf2 genotype codes (0-3).

    Returns:
        Packed bytes with 8 samples per byte, LSB-first; bit=1 iff called.
    """
    called = (gt_types != 2).astype(np.uint8)
    return np.packbits(called, bitorder="little").tobytes()


def build_called_packed_all(n_samples: int, value: int = 1) -> bytes:
    """Return a called_packed blob with every sample set to ``value`` (0 or 1).

    Used for legacy ``--assume-homref-on-missing`` ingestion and for v1.0→v1.1
    migration back-fill.
    """
    if value not in (0, 1):
        raise ValueError("value must be 0 or 1")
    bit_arr = np.full(n_samples, value, dtype=np.uint8)
    return np.packbits(bit_arr, bitorder="little").tobytes()


def unpack_called_packed(called_packed: bytes | None, n_samples: int) -> np.ndarray:
    """Unpack called bits to a per-sample 0/1 array.

    A ``None`` or empty ``called_packed`` (schema v1.0 databases, or sites emitted
    before v1.1) is treated as "all samples called" to preserve legacy semantics.

    Args:
        called_packed: packed called bytes, or None for legacy all-called.
        n_samples: number of samples encoded.

    Returns:
        uint8 array where 1 = called, 0 = not interrogated.
    """
    if not called_packed:
        return np.ones(n_samples, dtype=np.uint8)
    return np.unpackbits(np.frombuffer(called_packed, dtype=np.uint8), bitorder="little")[
        :n_samples
    ]


def _sparse_threshold_bytes(n_samples: int) -> int:
    """Size of the dense gt_packed blob (without format tag) for n_samples."""
    return (n_samples + 3) >> 2


def encode_gt_blob(gt_packed_dense: bytes, n_samples: int) -> bytes:
    """Wrap a dense gt_packed byte array in the v1.1 storage blob format.

    Returns a tagged blob that the database layer can persist. When the variant
    is dominated by HomRef samples (the common case for rare variants), sparse
    encoding is chosen automatically; otherwise the dense payload is preserved.

    Args:
        gt_packed_dense: dense 2-bit packed array from :func:`vectorized_gt_pack`.
        n_samples: number of samples encoded.

    Returns:
        Tagged blob: ``b'\\x00' + dense`` or ``b'\\x01' + sparse_payload``.
    """
    dense_len = _sparse_threshold_bytes(n_samples)
    arr = np.frombuffer(gt_packed_dense, dtype=np.uint8)

    # Count non-HomRef samples by unpacking the 2-bit slots.
    slots = np.empty(len(arr) * 4, dtype=np.uint8)
    slots[0::4] = arr & 0x03
    slots[1::4] = (arr >> 2) & 0x03
    slots[2::4] = (arr >> 4) & 0x03
    slots[3::4] = (arr >> 6) & 0x03
    slots = slots[:n_samples]

    nonref_mask = slots != 0
    n_nonref = int(nonref_mask.sum())

    # Sparse payload layout (v1.1 sparse_v1):
    #   1 byte  : format tag (0x01)
    #   4 bytes : n_samples (little-endian uint32, for self-describing decode)
    #   4 bytes : n_nonref (little-endian uint32)
    #   n_nonref * 4 bytes : sample indices (little-endian uint32)
    #   ceil(n_nonref/4) bytes : packed 2-bit codes for those indices
    sparse_payload_len = 1 + 4 + 4 + n_nonref * 4 + ((n_nonref + 3) >> 2)
    dense_tagged_len = 1 + dense_len

    if sparse_payload_len >= dense_tagged_len:
        return bytes([GT_BLOB_DENSE]) + gt_packed_dense

    nonref_idx = np.flatnonzero(nonref_mask).astype(np.uint32)
    nonref_codes = slots[nonref_mask].astype(np.uint8)

    # Pack the non-ref codes (which are in {1,2,3}) into 2-bit little-endian slots.
    n_padded = ((n_nonref + 3) // 4) * 4
    padded = np.zeros(n_padded, dtype=np.uint8)
    padded[:n_nonref] = nonref_codes
    groups = padded.reshape(-1, 4)
    packed_codes = (
        groups[:, 0] | (groups[:, 1] << 2) | (groups[:, 2] << 4) | (groups[:, 3] << 6)
    )

    out = bytearray()
    out.append(GT_BLOB_SPARSE)
    out += np.uint32(n_samples).tobytes()
    out += np.uint32(n_nonref).tobytes()
    out += nonref_idx.tobytes()
    out += packed_codes.tobytes()
    return bytes(out)


def decode_gt_blob(blob: bytes, n_samples: int) -> bytes:
    """Decode a v1.1 tagged gt_packed blob back to the dense 2-bit byte array.

    Accepts three input shapes:
      * v1.0 legacy: a bare dense array with no tag (length == ceil(N/4)).
      * v1.1 dense: 0x00 tag followed by a dense array.
      * v1.1 sparse: 0x01 tag followed by the sparse payload described in
        :func:`encode_gt_blob`.

    Args:
        blob: stored byte blob.
        n_samples: number of samples (authoritative; used to size legacy blobs).

    Returns:
        Dense gt_packed byte array, length ``ceil(n_samples / 4)``.
    """
    dense_len = _sparse_threshold_bytes(n_samples)
    if not blob:
        return bytes(dense_len)

    # Legacy (no tag) — blob length equals the dense expected length.
    if len(blob) == dense_len:
        return blob

    tag = blob[0]
    if tag == GT_BLOB_DENSE:
        payload = blob[1:]
        if len(payload) != dense_len:
            raise ValueError(
                f"Dense gt_blob payload length {len(payload)} "
                f"does not match expected {dense_len} for n_samples={n_samples}"
            )
        return payload

    if tag == GT_BLOB_SPARSE:
        header = np.frombuffer(blob, dtype=np.uint32, count=2, offset=1)
        stored_n_samples = int(header[0])
        n_nonref = int(header[1])
        if stored_n_samples != n_samples:
            raise ValueError(
                f"Sparse gt_blob header n_samples={stored_n_samples} "
                f"does not match caller n_samples={n_samples}"
            )
        idx_offset = 1 + 8
        idx_end = idx_offset + n_nonref * 4
        nonref_idx = np.frombuffer(blob, dtype=np.uint32, count=n_nonref, offset=idx_offset)
        packed_codes = np.frombuffer(blob, dtype=np.uint8, offset=idx_end)

        # Unpack the non-ref codes back into a flat array.
        code_slots = np.empty(len(packed_codes) * 4, dtype=np.uint8)
        code_slots[0::4] = packed_codes & 0x03
        code_slots[1::4] = (packed_codes >> 2) & 0x03
        code_slots[2::4] = (packed_codes >> 4) & 0x03
        code_slots[3::4] = (packed_codes >> 6) & 0x03
        codes = code_slots[:n_nonref]

        dense = np.zeros(n_samples, dtype=np.uint8)
        dense[nonref_idx] = codes

        n_padded = ((n_samples + 3) // 4) * 4
        padded = np.zeros(n_padded, dtype=np.uint8)
        padded[:n_samples] = dense
        groups = padded.reshape(-1, 4)
        return (
            groups[:, 0] | (groups[:, 1] << 2) | (groups[:, 2] << 4) | (groups[:, 3] << 6)
        ).tobytes()

    raise ValueError(f"Unknown gt_packed blob tag: 0x{tag:02x}")


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
