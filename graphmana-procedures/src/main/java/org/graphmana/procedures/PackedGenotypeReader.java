package org.graphmana.procedures;

/**
 * Bit-extraction utilities for packed genotype arrays on Variant nodes.
 *
 * <h3>Encoding</h3>
 * <ul>
 *   <li>{@code gt_packed: byte[]} — 2 bits per sample (4 samples per byte).
 *       Sample {@code i} occupies bits {@code (i%4)*2} and {@code (i%4)*2+1}
 *       of byte {@code gt_packed[i/4]}. Values: 00=HomRef, 01=Het, 10=HomAlt, 11=Missing.</li>
 *   <li>{@code phase_packed: byte[]} — 1 bit per sample (8 samples per byte).
 *       Sample {@code i} occupies bit {@code i%8} of byte {@code phase_packed[i/8]}.
 *       Value: which haplotype carries ALT (0 or 1). Only meaningful for Het sites.</li>
 * </ul>
 *
 * <p>Array sizes for N samples: gt_packed = ceil(N/4) bytes, phase_packed = ceil(N/8) bytes.
 * For 3,202 samples: 801 + 401 = 1,202 bytes per variant.</p>
 */
final class PackedGenotypeReader {

    private PackedGenotypeReader() {}

    // ---- Genotype constants (2-bit encoding) ----
    static final int GT_HOM_REF = 0;
    static final int GT_HET     = 1;
    static final int GT_HOM_ALT = 2;
    static final int GT_MISSING = 3;

    /**
     * Extract the 2-bit genotype for sample {@code sampleIdx} from a packed gt array.
     *
     * @param gtPacked packed genotype byte array (2 bits/sample, 4 samples/byte)
     * @param sampleIdx 0-based sample index (matching Sample.packed_index)
     * @return genotype value: 0=HomRef, 1=Het, 2=HomAlt, 3=Missing
     */
    static int genotype(byte[] gtPacked, int sampleIdx) {
        int byteIdx = sampleIdx >> 2;          // sampleIdx / 4
        int bitShift = (sampleIdx & 3) << 1;   // (sampleIdx % 4) * 2
        return (gtPacked[byteIdx] >> bitShift) & 0x03;
    }

    /**
     * Extract the 1-bit phase for sample {@code sampleIdx} from a packed phase array.
     *
     * @param phasePacked packed phase byte array (1 bit/sample, 8 samples/byte)
     * @param sampleIdx 0-based sample index
     * @return phase value: 0 or 1 (which haplotype carries ALT)
     */
    static int phase(byte[] phasePacked, int sampleIdx) {
        int byteIdx = sampleIdx >> 3;          // sampleIdx / 8
        int bitIdx = sampleIdx & 7;            // sampleIdx % 8
        return (phasePacked[byteIdx] >> bitIdx) & 0x01;
    }

    // ---- Packing utilities (used by import pipeline tests) ----

    /**
     * Compute the byte array size needed for gt_packed with the given sample count.
     */
    static int gtPackedLength(int nSamples) {
        return (nSamples + 3) >> 2;  // ceil(nSamples / 4)
    }

    /**
     * Compute the byte array size needed for phase_packed with the given sample count.
     */
    static int phasePackedLength(int nSamples) {
        return (nSamples + 7) >> 3;  // ceil(nSamples / 8)
    }

    /**
     * Set a 2-bit genotype value for sample {@code sampleIdx} in a packed gt array.
     */
    static void setGenotype(byte[] gtPacked, int sampleIdx, int gt) {
        int byteIdx = sampleIdx >> 2;
        int bitShift = (sampleIdx & 3) << 1;
        gtPacked[byteIdx] = (byte) ((gtPacked[byteIdx] & ~(0x03 << bitShift))
                | ((gt & 0x03) << bitShift));
    }

    /**
     * Set a 1-bit phase value for sample {@code sampleIdx} in a packed phase array.
     */
    static void setPhase(byte[] phasePacked, int sampleIdx, int phase) {
        int byteIdx = sampleIdx >> 3;
        int bitIdx = sampleIdx & 7;
        if (phase != 0) {
            phasePacked[byteIdx] |= (byte) (1 << bitIdx);
        } else {
            phasePacked[byteIdx] &= (byte) ~(1 << bitIdx);
        }
    }

    // ---- Ploidy (1-bit per sample, same layout as phase) ----

    /**
     * Extract the 1-bit ploidy for sample {@code sampleIdx} from a packed ploidy array.
     *
     * @param ploidyPacked packed ploidy byte array (1 bit/sample). Null or empty means all diploid.
     * @param sampleIdx 0-based sample index
     * @return 1 if haploid, 0 if diploid
     */
    static int ploidy(byte[] ploidyPacked, int sampleIdx) {
        if (ploidyPacked == null || ploidyPacked.length == 0) return 0;
        int byteIdx = sampleIdx >> 3;
        if (byteIdx >= ploidyPacked.length) return 0;
        return (ploidyPacked[byteIdx] >> (sampleIdx & 7)) & 0x01;
    }

    /**
     * Set a 1-bit ploidy value for sample {@code sampleIdx} in a packed ploidy array.
     *
     * @param ploidyPacked packed ploidy byte array
     * @param sampleIdx 0-based sample index
     * @param haploid 1 = haploid, 0 = diploid
     */
    static void setPloidy(byte[] ploidyPacked, int sampleIdx, int haploid) {
        int byteIdx = sampleIdx >> 3;
        int bitIdx = sampleIdx & 7;
        if (haploid != 0) {
            ploidyPacked[byteIdx] |= (byte) (1 << bitIdx);
        } else {
            ploidyPacked[byteIdx] &= (byte) ~(1 << bitIdx);
        }
    }

    /**
     * Compute the byte array size needed for ploidy_packed with the given sample count.
     * Same layout as phase_packed: 1 bit per sample.
     */
    static int ploidyPackedLength(int nSamples) {
        return (nSamples + 7) >> 3;  // ceil(nSamples / 8)
    }

    // ---- Called (1-bit per sample) ----
    //
    // Schema v1.1. Bit = 1 means the sample was interrogated at this variant
    // (a real call of any kind, including explicit missing ./.), bit = 0
    // means the sample was not looked at. Used by subset-stat procedures to
    // exclude not-interrogated samples from the per-site denominator so that
    // allele frequencies computed from GraphMana graphs remain honest when
    // incremental batches carry different site lists.
    //
    // On schema v1.0 databases the property is absent; callers should pass
    // null, and {@link #called(byte[], int)} will return 1 for every sample
    // (legacy "all samples called" semantics, matching the assumption baked
    // into the v1.0 pop-stat pipeline).

    /**
     * Extract the 1-bit called flag for sample {@code sampleIdx}.
     *
     * @param calledPacked packed called byte array (1 bit/sample). Null or empty
     *     means schema v1.0 — all samples are treated as called.
     * @param sampleIdx 0-based sample index
     * @return 1 if interrogated (or legacy), 0 if not interrogated
     */
    static int called(byte[] calledPacked, int sampleIdx) {
        if (calledPacked == null || calledPacked.length == 0) return 1;
        int byteIdx = sampleIdx >> 3;
        if (byteIdx >= calledPacked.length) return 1;
        return (calledPacked[byteIdx] >> (sampleIdx & 7)) & 0x01;
    }

    /**
     * Set a 1-bit called value for sample {@code sampleIdx} in a packed called array.
     *
     * @param calledPacked packed called byte array
     * @param sampleIdx 0-based sample index
     * @param called 1 = interrogated, 0 = not interrogated
     */
    static void setCalled(byte[] calledPacked, int sampleIdx, int called) {
        int byteIdx = sampleIdx >> 3;
        int bitIdx = sampleIdx & 7;
        if (called != 0) {
            calledPacked[byteIdx] |= (byte) (1 << bitIdx);
        } else {
            calledPacked[byteIdx] &= (byte) ~(1 << bitIdx);
        }
    }

    /**
     * Compute the byte array size needed for called_packed with the given sample count.
     * Same layout as phase_packed/ploidy_packed: 1 bit per sample, LSB-first.
     */
    static int calledPackedLength(int nSamples) {
        return (nSamples + 7) >> 3;  // ceil(nSamples / 8)
    }
}
