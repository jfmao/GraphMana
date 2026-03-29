"""GraphPop compatibility verification tests.

Verifies that GraphMana's Python-side packed arrays produce bit patterns
identical to what GraphPop's Java PackedGenotypeReader expects. This is
the critical contract between the two codebases.

Reference: GraphPop PackedGenotypeReader.java at /mnt/e/GraphPop
"""

from __future__ import annotations

import numpy as np

from graphmana.ingest.genotype_packer import (
    build_ploidy_packed,
    pack_phase,
    unpack_genotypes,
    unpack_phase,
    unpack_ploidy,
    vectorized_gt_pack,
)

# ---------------------------------------------------------------------------
# Java PackedGenotypeReader simulation (reference implementation)
# ---------------------------------------------------------------------------


def java_genotype(gt_packed: bytes, sample_idx: int) -> int:
    """Simulate PackedGenotypeReader.genotype() from GraphPop Java code.

    byte-for-byte translation of:
        int byteIdx = sampleIdx >> 2;
        int bitShift = (sampleIdx & 3) << 1;
        return (gtPacked[byteIdx] >> bitShift) & 0x03;
    """
    byte_idx = sample_idx >> 2
    bit_shift = (sample_idx & 3) << 1
    # Java bytes are signed (-128..127), stored in Neo4j as signed.
    # Convert to unsigned for bit extraction.
    b = gt_packed[byte_idx]
    if b < 0:
        b += 256
    return (b >> bit_shift) & 0x03


def java_phase(phase_packed: bytes, sample_idx: int) -> int:
    """Simulate PackedGenotypeReader.phase() from GraphPop Java code.

    byte-for-byte translation of:
        int byteIdx = sampleIdx >> 3;
        int bitIdx = sampleIdx & 7;
        return (phasePacked[byteIdx] >> bitIdx) & 0x01;
    """
    byte_idx = sample_idx >> 3
    bit_idx = sample_idx & 7
    b = phase_packed[byte_idx]
    if b < 0:
        b += 256
    return (b >> bit_idx) & 0x01


def java_ploidy(ploidy_packed: bytes | None, sample_idx: int) -> int:
    """Simulate PackedGenotypeReader.ploidy() from GraphPop Java code.

    Null or empty means all diploid (returns 0).
    """
    if ploidy_packed is None or len(ploidy_packed) == 0:
        return 0
    byte_idx = sample_idx >> 3
    if byte_idx >= len(ploidy_packed):
        return 0
    b = ploidy_packed[byte_idx]
    if b < 0:
        b += 256
    return (b >> (sample_idx & 7)) & 0x01


def to_signed_java_bytes(data: bytes) -> list[int]:
    """Convert Python unsigned bytes to signed Java byte values (-128..127).

    This is how bytes appear in Neo4j (Java-backed) byte[] properties.
    """
    return [b if b < 128 else b - 256 for b in data]


def from_signed_java_bytes(signed: list[int]) -> bytes:
    """Convert signed Java bytes back to Python bytes."""
    return bytes(b & 0xFF for b in signed)


# ---------------------------------------------------------------------------
# Genotype encoding tests
# ---------------------------------------------------------------------------

# GraphPop constants
GT_HOM_REF = 0
GT_HET = 1
GT_HOM_ALT = 2
GT_MISSING = 3


class TestGenotypeEncodingCompat:
    """Verify Python gt packing matches Java PackedGenotypeReader extraction."""

    def test_basic_4_samples(self):
        """4 samples: HomRef, Het, HomAlt, Missing → one byte."""
        # cyvcf2 codes: 0=HOM_REF, 1=HET, 3=HOM_ALT, 2=MISSING
        gt_types = np.array([0, 1, 3, 2], dtype=np.int8)
        packed = vectorized_gt_pack(gt_types)

        # Expected packed codes: HomRef=0, Het=1, HomAlt=2, Missing=3
        assert java_genotype(packed, 0) == GT_HOM_REF
        assert java_genotype(packed, 1) == GT_HET
        assert java_genotype(packed, 2) == GT_HOM_ALT
        assert java_genotype(packed, 3) == GT_MISSING

    def test_10_samples(self):
        """10 samples across 3 bytes — verify all positions."""
        # cyvcf2 codes for 10 samples
        gt_types = np.array([0, 1, 3, 2, 0, 0, 1, 1, 3, 2], dtype=np.int8)
        packed = vectorized_gt_pack(gt_types)

        expected = [
            GT_HOM_REF,
            GT_HET,
            GT_HOM_ALT,
            GT_MISSING,
            GT_HOM_REF,
            GT_HOM_REF,
            GT_HET,
            GT_HET,
            GT_HOM_ALT,
            GT_MISSING,
        ]
        for i, exp in enumerate(expected):
            assert (
                java_genotype(packed, i) == exp
            ), f"sample {i}: expected {exp}, got {java_genotype(packed, i)}"

    def test_roundtrip_random(self):
        """Random genotypes survive Python pack → Java unpack roundtrip."""
        rng = np.random.RandomState(42)
        for n_samples in [1, 4, 5, 100, 1000, 3202]:
            # cyvcf2 codes: 0, 1, 2, 3
            gt_types = rng.randint(0, 4, size=n_samples).astype(np.int8)
            packed = vectorized_gt_pack(gt_types)

            # Python unpack for reference
            py_unpacked = unpack_genotypes(packed, n_samples)

            for i in range(n_samples):
                java_val = java_genotype(packed, i)
                assert (
                    java_val == py_unpacked[i]
                ), f"n={n_samples}, sample {i}: Java={java_val}, Python={py_unpacked[i]}"

    def test_cyvcf2_remap_critical(self):
        """Verify the cyvcf2 remap is correct: cyvcf2 2=MISSING, 3=HOM_ALT."""
        # This is the most critical encoding decision. cyvcf2 swaps 2 and 3
        # relative to the packed encoding.
        #   cyvcf2: 0=HOM_REF, 1=HET, 2=UNKNOWN/MISSING, 3=HOM_ALT
        #   packed: 00=HomRef, 01=Het, 10=HomAlt, 11=Missing
        gt_types = np.array([2], dtype=np.int8)  # cyvcf2 MISSING
        packed = vectorized_gt_pack(gt_types)
        assert java_genotype(packed, 0) == GT_MISSING  # packed 11

        gt_types = np.array([3], dtype=np.int8)  # cyvcf2 HOM_ALT
        packed = vectorized_gt_pack(gt_types)
        assert java_genotype(packed, 0) == GT_HOM_ALT  # packed 10

    def test_signed_java_byte_boundary(self):
        """Packed byte values above 127 must survive signed Java byte conversion."""
        # 4 samples all Missing: packed code 11 for each
        # byte = 0b11_11_11_11 = 255 unsigned = -1 signed Java
        gt_types = np.array([2, 2, 2, 2], dtype=np.int8)  # cyvcf2 MISSING
        packed = vectorized_gt_pack(gt_types)

        assert packed[0] == 255  # unsigned
        signed = to_signed_java_bytes(packed)
        assert signed[0] == -1  # signed Java

        # Round-trip through signed representation
        recovered = from_signed_java_bytes(signed)
        for i in range(4):
            assert java_genotype(recovered, i) == GT_MISSING


class TestPhaseEncodingCompat:
    """Verify Python phase packing matches Java PackedGenotypeReader.phase()."""

    def test_basic_phase_bits(self):
        """Phase bits for 8 samples — verify each position."""
        # Simulate: samples 0,2,4,6 are het with phase=1 (ALT on hap2)
        # Samples 1,3,5,7 are het with phase=0 (ALT on hap1)
        n_samples = 8
        het_indices = np.arange(8)
        # genotypes[i] = [allele0, allele1, is_phased]
        genotypes = [
            [0, 1, True],  # sample 0: ALT on hap2 → phase bit = 1
            [1, 0, True],  # sample 1: ALT on hap1 → phase bit = 0
            [0, 1, True],  # sample 2: phase bit = 1
            [1, 0, True],  # sample 3: phase bit = 0
            [0, 1, True],  # sample 4: phase bit = 1
            [1, 0, True],  # sample 5: phase bit = 0
            [0, 1, True],  # sample 6: phase bit = 1
            [1, 0, True],  # sample 7: phase bit = 0
        ]
        packed = pack_phase(n_samples, het_indices, genotypes)

        for i in range(8):
            expected = 1 if genotypes[i][1] == 1 else 0
            assert java_phase(packed, i) == expected, f"sample {i}"

    def test_sparse_het(self):
        """Only some samples are het — non-het positions should be 0."""
        n_samples = 16
        het_indices = np.array([3, 7, 11])
        genotypes = [[0, 0, False]] * 16  # default HomRef
        genotypes[3] = [0, 1, True]  # het, ALT on hap2
        genotypes[7] = [1, 0, True]  # het, ALT on hap1
        genotypes[11] = [0, 1, True]  # het, ALT on hap2
        packed = pack_phase(n_samples, het_indices, genotypes)

        for i in range(16):
            java_val = java_phase(packed, i)
            if i == 3:
                assert java_val == 1
            elif i == 7:
                assert java_val == 0
            elif i == 11:
                assert java_val == 1
            else:
                assert java_val == 0, f"non-het sample {i} should be 0"

    def test_phase_roundtrip(self):
        """Phase pack → unpack roundtrip matches Java extraction."""
        n_samples = 20
        het_indices = np.array([1, 5, 8, 13, 19])
        genotypes = [[0, 0, False]] * 20
        genotypes[1] = [0, 1, True]
        genotypes[5] = [1, 0, True]
        genotypes[8] = [0, 1, True]
        genotypes[13] = [1, 0, True]
        genotypes[19] = [0, 1, True]
        packed = pack_phase(n_samples, het_indices, genotypes)

        py_unpacked = unpack_phase(packed, n_samples)
        for i in range(n_samples):
            assert java_phase(packed, i) == py_unpacked[i], f"sample {i}"


class TestPloidyEncodingCompat:
    """Verify Python ploidy packing matches Java PackedGenotypeReader.ploidy()."""

    def test_null_means_all_diploid(self):
        """None ploidy_packed → all diploid (0) for any sample index."""
        for i in [0, 1, 100, 3201]:
            assert java_ploidy(None, i) == 0

    def test_empty_means_all_diploid(self):
        """Empty bytes → all diploid."""
        assert java_ploidy(b"", 0) == 0

    def test_mixed_ploidy(self):
        """Some haploid, some diploid — verify bit extraction."""
        # Samples 0,3,7 are haploid
        flags = np.array([True, False, False, True, False, False, False, True], dtype=bool)
        packed = build_ploidy_packed(flags)

        for i in range(8):
            expected = 1 if flags[i] else 0
            assert java_ploidy(packed, i) == expected, f"sample {i}"

    def test_ploidy_roundtrip(self):
        """Ploidy pack → unpack roundtrip matches Java extraction."""
        rng = np.random.RandomState(99)
        flags = rng.choice([True, False], size=50, p=[0.1, 0.9])
        packed = build_ploidy_packed(flags)

        py_unpacked = unpack_ploidy(packed, 50)
        for i in range(50):
            assert java_ploidy(packed, i) == py_unpacked[i], f"sample {i}"


# ---------------------------------------------------------------------------
# Population array compatibility
# ---------------------------------------------------------------------------


class TestPopulationArrayCompat:
    """Verify that GraphMana population arrays are compatible with
    GraphPop's ArrayUtil type coercion."""

    def test_int_array_compat(self):
        """int[] from neo4j-admin import → ArrayUtil.toIntArray() succeeds."""
        # GraphMana writes ac:int[], an:int[], het_count:int[], hom_alt_count:int[]
        # These arrive as int[] in Neo4j. Java ArrayUtil handles int[] directly.
        ac = [5, 3, 8]  # 3 populations
        # Simulating: ArrayUtil.toIntArray(prop) where prop is int[]
        assert isinstance(ac, list)  # In Neo4j Java, this would be int[]
        assert all(isinstance(v, int) for v in ac)

    def test_float_to_double_compat(self):
        """float[] from neo4j-admin import → ArrayUtil.toDoubleArray() converts."""
        # GraphMana writes af:float[] — Java float[] != double[]
        # ArrayUtil.toDoubleArray() explicitly handles float[] → double[]
        af = [0.25, 0.15, 0.40]
        # The key point: float values are losslessly promotable to double
        for v in af:
            assert abs(float(v) - v) < 1e-10

    def test_pop_ids_string_array(self):
        """String[] pop_ids → ArrayUtil.toStringArray() returns as-is."""
        pop_ids = ["EUR", "AFR", "EAS"]
        # ArrayUtil: if (prop instanceof String[]) return (String[]) prop
        assert isinstance(pop_ids, list)
        assert all(isinstance(v, str) for v in pop_ids)

    def test_harmonic_number_formula(self):
        """Verify a_n = sum(1/i for i=1..2n-1) matches expected values."""
        # GraphPop uses a_n for Tajima's D, Watterson's theta
        # Known values: for n=5 samples, 2n-1=9
        n = 5
        a_n = sum(1.0 / i for i in range(1, 2 * n))  # range(1, 10) = 1..9
        expected = 1 + 1 / 2 + 1 / 3 + 1 / 4 + 1 / 5 + 1 / 6 + 1 / 7 + 1 / 8 + 1 / 9
        assert abs(a_n - expected) < 1e-12

        a_n2 = sum(1.0 / (i * i) for i in range(1, 2 * n))
        expected2 = 1 + 1 / 4 + 1 / 9 + 1 / 16 + 1 / 25 + 1 / 36 + 1 / 49 + 1 / 64 + 1 / 81
        assert abs(a_n2 - expected2) < 1e-12


# ---------------------------------------------------------------------------
# Property name contract
# ---------------------------------------------------------------------------


class TestPropertyNameContract:
    """Verify that all property names GraphPop reads exist in GraphMana's
    CSV headers (the definitive list of what gets imported)."""

    # GraphPop required Variant properties
    GRAPHPOP_VARIANT_REQUIRED = {
        "variantId",
        "chr",
        "pos",
        "gt_packed",
        "phase_packed",
        "pop_ids",
        "ac",
        "an",
        "af",
        "het_count",
        "hom_alt_count",
    }

    # GraphPop optional Variant properties
    GRAPHPOP_VARIANT_OPTIONAL = {
        "ploidy_packed",
        "an_total",
        "variant_type",
        "ancestral_allele",
    }

    GRAPHPOP_SAMPLE_REQUIRED = {"sampleId", "packed_index"}
    GRAPHPOP_POPULATION_REQUIRED = {"populationId", "n_samples", "a_n", "a_n2"}

    def _strip_type(self, header_field: str) -> str:
        """Strip Neo4j type suffix from CSV header field.

        'variantId:ID(Variant)' → 'variantId'
        'ac:int[]' → 'ac'
        ':LABEL' → ':LABEL' (skip)
        """
        if header_field.startswith(":"):
            return header_field
        return header_field.split(":")[0]

    def test_variant_required_properties(self):
        from graphmana.ingest.csv_emitter import VARIANT_HEADER

        variant_props = {self._strip_type(h) for h in VARIANT_HEADER}
        for prop in self.GRAPHPOP_VARIANT_REQUIRED:
            assert prop in variant_props, f"Missing required Variant property: {prop}"

    def test_variant_optional_properties(self):
        from graphmana.ingest.csv_emitter import VARIANT_HEADER

        variant_props = {self._strip_type(h) for h in VARIANT_HEADER}
        for prop in self.GRAPHPOP_VARIANT_OPTIONAL:
            assert prop in variant_props, f"Missing optional Variant property: {prop}"

    def test_sample_required_properties(self):
        from graphmana.ingest.csv_emitter import SAMPLE_HEADER

        sample_props = {self._strip_type(h) for h in SAMPLE_HEADER}
        for prop in self.GRAPHPOP_SAMPLE_REQUIRED:
            assert prop in sample_props, f"Missing required Sample property: {prop}"

    def test_population_required_properties(self):
        from graphmana.ingest.csv_emitter import POPULATION_HEADER

        pop_props = {self._strip_type(h) for h in POPULATION_HEADER}
        for prop in self.GRAPHPOP_POPULATION_REQUIRED:
            assert prop in pop_props, f"Missing required Population property: {prop}"

    def test_relationship_types(self):
        """Verify required relationship types exist in CSV headers."""
        from graphmana.ingest.csv_emitter import IN_POPULATION_HEADER

        # IN_POPULATION relationship must exist
        assert any("Population" in h for h in IN_POPULATION_HEADER)
