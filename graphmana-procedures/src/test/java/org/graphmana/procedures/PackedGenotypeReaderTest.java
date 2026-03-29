package org.graphmana.procedures;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive tests for {@link PackedGenotypeReader} bit-packing and extraction.
 */
class PackedGenotypeReaderTest {

    // ---- Genotype round-trip: all 4 values x 4 byte positions ----

    @Test
    void genotypeRoundTripAllPositions() {
        for (int gt = 0; gt <= 3; gt++) {
            for (int pos = 0; pos < 4; pos++) {
                byte[] packed = new byte[1];
                PackedGenotypeReader.setGenotype(packed, pos, gt);
                assertEquals(gt, PackedGenotypeReader.genotype(packed, pos),
                        "gt=" + gt + " at sampleIdx=" + pos);
            }
        }
    }

    @Test
    void genotypeMultipleSamplesInOneByte() {
        byte[] packed = new byte[1];
        PackedGenotypeReader.setGenotype(packed, 0, PackedGenotypeReader.GT_HOM_REF);
        PackedGenotypeReader.setGenotype(packed, 1, PackedGenotypeReader.GT_HET);
        PackedGenotypeReader.setGenotype(packed, 2, PackedGenotypeReader.GT_HOM_ALT);
        PackedGenotypeReader.setGenotype(packed, 3, PackedGenotypeReader.GT_MISSING);

        assertEquals(0, PackedGenotypeReader.genotype(packed, 0));
        assertEquals(1, PackedGenotypeReader.genotype(packed, 1));
        assertEquals(2, PackedGenotypeReader.genotype(packed, 2));
        assertEquals(3, PackedGenotypeReader.genotype(packed, 3));
    }

    @Test
    void genotypeAcrossMultipleBytes() {
        byte[] packed = new byte[2];
        int[] values = {0, 1, 2, 3, 2, 1, 0, 3};
        for (int i = 0; i < 8; i++) {
            PackedGenotypeReader.setGenotype(packed, i, values[i]);
        }
        for (int i = 0; i < 8; i++) {
            assertEquals(values[i], PackedGenotypeReader.genotype(packed, i),
                    "sampleIdx=" + i);
        }
    }

    @Test
    void genotypeOverwritePreservesNeighbors() {
        byte[] packed = new byte[1];
        for (int i = 0; i < 4; i++) {
            PackedGenotypeReader.setGenotype(packed, i, PackedGenotypeReader.GT_HET);
        }
        PackedGenotypeReader.setGenotype(packed, 2, PackedGenotypeReader.GT_HOM_ALT);

        assertEquals(1, PackedGenotypeReader.genotype(packed, 0));
        assertEquals(1, PackedGenotypeReader.genotype(packed, 1));
        assertEquals(2, PackedGenotypeReader.genotype(packed, 2));
        assertEquals(1, PackedGenotypeReader.genotype(packed, 3));
    }

    // ---- Phase round-trip ----

    @Test
    void phaseRoundTripAllPositions() {
        for (int phase = 0; phase <= 1; phase++) {
            for (int pos = 0; pos < 8; pos++) {
                byte[] packed = new byte[1];
                PackedGenotypeReader.setPhase(packed, pos, phase);
                assertEquals(phase, PackedGenotypeReader.phase(packed, pos),
                        "phase=" + phase + " at sampleIdx=" + pos);
            }
        }
    }

    @Test
    void phaseAllBitsInOneByte() {
        byte[] packed = new byte[1];
        for (int i = 0; i < 8; i++) {
            PackedGenotypeReader.setPhase(packed, i, i % 2 == 0 ? 1 : 0);
        }
        for (int i = 0; i < 8; i++) {
            assertEquals(i % 2 == 0 ? 1 : 0, PackedGenotypeReader.phase(packed, i));
        }
    }

    @Test
    void phaseAcrossMultipleBytes() {
        byte[] packed = new byte[2];
        PackedGenotypeReader.setPhase(packed, 0, 1);
        PackedGenotypeReader.setPhase(packed, 7, 1);
        PackedGenotypeReader.setPhase(packed, 8, 1);
        PackedGenotypeReader.setPhase(packed, 15, 0);

        assertEquals(1, PackedGenotypeReader.phase(packed, 0));
        assertEquals(0, PackedGenotypeReader.phase(packed, 1));
        assertEquals(1, PackedGenotypeReader.phase(packed, 7));
        assertEquals(1, PackedGenotypeReader.phase(packed, 8));
        assertEquals(0, PackedGenotypeReader.phase(packed, 9));
        assertEquals(0, PackedGenotypeReader.phase(packed, 15));
    }

    @Test
    void phaseOverwritePreservesNeighbors() {
        byte[] packed = new byte[1];
        for (int i = 0; i < 8; i++) {
            PackedGenotypeReader.setPhase(packed, i, 1);
        }
        PackedGenotypeReader.setPhase(packed, 3, 0);

        for (int i = 0; i < 8; i++) {
            assertEquals(i == 3 ? 0 : 1, PackedGenotypeReader.phase(packed, i),
                    "sampleIdx=" + i);
        }
    }

    // ---- Length calculations ----

    @Test
    void gtPackedLength() {
        assertEquals(1, PackedGenotypeReader.gtPackedLength(1));
        assertEquals(1, PackedGenotypeReader.gtPackedLength(4));
        assertEquals(2, PackedGenotypeReader.gtPackedLength(5));
        assertEquals(2, PackedGenotypeReader.gtPackedLength(8));
        assertEquals(801, PackedGenotypeReader.gtPackedLength(3202));
    }

    @Test
    void phasePackedLength() {
        assertEquals(1, PackedGenotypeReader.phasePackedLength(1));
        assertEquals(1, PackedGenotypeReader.phasePackedLength(8));
        assertEquals(2, PackedGenotypeReader.phasePackedLength(9));
        assertEquals(2, PackedGenotypeReader.phasePackedLength(16));
        assertEquals(401, PackedGenotypeReader.phasePackedLength(3202));
    }

    // ---- Edge cases ----

    @Test
    void allZerosArray() {
        byte[] gt = new byte[4];
        for (int i = 0; i < 16; i++) {
            assertEquals(PackedGenotypeReader.GT_HOM_REF, PackedGenotypeReader.genotype(gt, i));
        }
    }

    @Test
    void allOnesPhaseArray() {
        byte[] phase = new byte[]{(byte) 0xFF, (byte) 0xFF};
        for (int i = 0; i < 16; i++) {
            assertEquals(1, PackedGenotypeReader.phase(phase, i));
        }
    }

    @Test
    void largeArrayRoundTrip() {
        int nSamples = 3202;
        byte[] gtPacked = new byte[PackedGenotypeReader.gtPackedLength(nSamples)];
        byte[] phasePacked = new byte[PackedGenotypeReader.phasePackedLength(nSamples)];

        for (int i = 0; i < nSamples; i++) {
            PackedGenotypeReader.setGenotype(gtPacked, i, i % 4);
            PackedGenotypeReader.setPhase(phasePacked, i, i % 2);
        }

        for (int i = 0; i < nSamples; i++) {
            assertEquals(i % 4, PackedGenotypeReader.genotype(gtPacked, i),
                    "gt at sample " + i);
            assertEquals(i % 2, PackedGenotypeReader.phase(phasePacked, i),
                    "phase at sample " + i);
        }
    }

    @Test
    void singleSample() {
        byte[] gt = new byte[1];
        byte[] phase = new byte[1];

        PackedGenotypeReader.setGenotype(gt, 0, PackedGenotypeReader.GT_HOM_ALT);
        PackedGenotypeReader.setPhase(phase, 0, 1);

        assertEquals(PackedGenotypeReader.GT_HOM_ALT, PackedGenotypeReader.genotype(gt, 0));
        assertEquals(1, PackedGenotypeReader.phase(phase, 0));
    }

    @Test
    void nonAlignedSampleCount() {
        int n = 5;
        byte[] gt = new byte[PackedGenotypeReader.gtPackedLength(n)];
        byte[] phase = new byte[PackedGenotypeReader.phasePackedLength(n)];

        PackedGenotypeReader.setGenotype(gt, 4, PackedGenotypeReader.GT_MISSING);
        PackedGenotypeReader.setPhase(phase, 4, 1);

        assertEquals(PackedGenotypeReader.GT_MISSING, PackedGenotypeReader.genotype(gt, 4));
        assertEquals(1, PackedGenotypeReader.phase(phase, 4));
        assertEquals(0, PackedGenotypeReader.genotype(gt, 0));
    }

    @Test
    void constants() {
        assertEquals(0, PackedGenotypeReader.GT_HOM_REF);
        assertEquals(1, PackedGenotypeReader.GT_HET);
        assertEquals(2, PackedGenotypeReader.GT_HOM_ALT);
        assertEquals(3, PackedGenotypeReader.GT_MISSING);
    }

    // ---- Ploidy tests ----

    @Test
    void ploidyNull() {
        assertEquals(0, PackedGenotypeReader.ploidy(null, 0));
        assertEquals(0, PackedGenotypeReader.ploidy(null, 100));
    }

    @Test
    void ploidyEmpty() {
        assertEquals(0, PackedGenotypeReader.ploidy(new byte[0], 0));
        assertEquals(0, PackedGenotypeReader.ploidy(new byte[0], 42));
    }

    @Test
    void ploidyRoundTrip() {
        int n = 16;
        byte[] packed = new byte[PackedGenotypeReader.ploidyPackedLength(n)];

        int[] haploidSamples = {0, 3, 7, 8, 15};
        for (int s : haploidSamples) {
            PackedGenotypeReader.setPloidy(packed, s, 1);
        }

        for (int i = 0; i < n; i++) {
            boolean expected = false;
            for (int h : haploidSamples) {
                if (i == h) { expected = true; break; }
            }
            assertEquals(expected ? 1 : 0, PackedGenotypeReader.ploidy(packed, i),
                    "sampleIdx=" + i);
        }
    }

    @Test
    void ploidyBeyondArrayBounds() {
        byte[] packed = new byte[1];
        PackedGenotypeReader.setPloidy(packed, 0, 1);
        assertEquals(1, PackedGenotypeReader.ploidy(packed, 0));
        assertEquals(0, PackedGenotypeReader.ploidy(packed, 16));
    }

    @Test
    void ploidyPackedLengthTest() {
        assertEquals(1, PackedGenotypeReader.ploidyPackedLength(1));
        assertEquals(1, PackedGenotypeReader.ploidyPackedLength(8));
        assertEquals(2, PackedGenotypeReader.ploidyPackedLength(9));
        assertEquals(401, PackedGenotypeReader.ploidyPackedLength(3202));
    }
}
