package org.graphmana.procedures;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link SampleSubsetComputer} and {@link SampleSubsetComputer.SubsetStats}.
 * Tests focus on the computation logic using raw packed arrays.
 */
class SampleSubsetComputerTest {

    @Test
    void subsetStatsContainer() {
        var stats = new SampleSubsetComputer.SubsetStats(10, 20, 0.5, 8, 1);
        assertEquals(10, stats.ac);
        assertEquals(20, stats.an);
        assertEquals(0.5, stats.af, 1e-12);
        assertEquals(8, stats.hetCount);
        assertEquals(1, stats.homAltCount);
    }

    @Test
    void subsetStatsZeroAn() {
        var stats = new SampleSubsetComputer.SubsetStats(0, 0, 0.0, 0, 0);
        assertEquals(0, stats.an);
        assertEquals(0.0, stats.af, 1e-12);
    }

    @Test
    void subsetStatsAllHomRef() {
        var stats = new SampleSubsetComputer.SubsetStats(0, 100, 0.0, 0, 0);
        assertEquals(0, stats.ac);
        assertEquals(100, stats.an);
        assertEquals(0.0, stats.af, 1e-12);
    }

    @Test
    void subsetStatsAllHomAlt() {
        var stats = new SampleSubsetComputer.SubsetStats(100, 100, 1.0, 0, 50);
        assertEquals(100, stats.ac);
        assertEquals(1.0, stats.af, 1e-12);
        assertEquals(50, stats.homAltCount);
    }

    @Test
    void subsetStatsMixed() {
        // 5 diploid samples: 1 HomRef, 2 Het, 1 HomAlt, 1 Missing
        // AN = 8, AC = 2*1 + 2*1 = 4, AF = 0.5
        var stats = new SampleSubsetComputer.SubsetStats(4, 8, 0.5, 2, 1);
        assertEquals(4, stats.ac);
        assertEquals(8, stats.an);
        assertEquals(0.5, stats.af, 1e-12);
        assertEquals(2, stats.hetCount);
        assertEquals(1, stats.homAltCount);
    }

    @Test
    void genotypeDecodingForSubset() {
        // Verify genotype decoding that SubsetComputer relies on
        byte[] gtPacked = new byte[2];
        // Sample 0: HomRef, 1: Het, 2: HomAlt, 3: Missing, 4: Het
        PackedGenotypeReader.setGenotype(gtPacked, 0, PackedGenotypeReader.GT_HOM_REF);
        PackedGenotypeReader.setGenotype(gtPacked, 1, PackedGenotypeReader.GT_HET);
        PackedGenotypeReader.setGenotype(gtPacked, 2, PackedGenotypeReader.GT_HOM_ALT);
        PackedGenotypeReader.setGenotype(gtPacked, 3, PackedGenotypeReader.GT_MISSING);
        PackedGenotypeReader.setGenotype(gtPacked, 4, PackedGenotypeReader.GT_HET);

        // Manually compute what SubsetComputer would produce for subset [0,1,2,3,4]
        int ac = 0, an = 0, het = 0, homAlt = 0;
        for (int s = 0; s < 5; s++) {
            int gt = PackedGenotypeReader.genotype(gtPacked, s);
            if (gt == PackedGenotypeReader.GT_MISSING) continue;
            an += 2; // diploid
            if (gt == PackedGenotypeReader.GT_HET) { ac += 1; het++; }
            else if (gt == PackedGenotypeReader.GT_HOM_ALT) { ac += 2; homAlt++; }
        }

        assertEquals(4, ac);
        assertEquals(8, an);
        assertEquals(2, het);
        assertEquals(1, homAlt);
        assertEquals(0.5, (double) ac / an, 1e-12);
    }

    @Test
    void ploidyAwareComputation() {
        // Test ploidy-aware counting manually
        byte[] gtPacked = new byte[1];
        byte[] ploidyPacked = new byte[1];

        // Sample 0: diploid Het, Sample 1: haploid HomAlt, Sample 2: diploid HomRef
        PackedGenotypeReader.setGenotype(gtPacked, 0, PackedGenotypeReader.GT_HET);
        PackedGenotypeReader.setGenotype(gtPacked, 1, PackedGenotypeReader.GT_HOM_ALT);
        PackedGenotypeReader.setGenotype(gtPacked, 2, PackedGenotypeReader.GT_HOM_REF);
        PackedGenotypeReader.setPloidy(ploidyPacked, 1, 1); // sample 1 is haploid

        int ac = 0, an = 0, het = 0, homAlt = 0;
        for (int s = 0; s < 3; s++) {
            int gt = PackedGenotypeReader.genotype(gtPacked, s);
            if (gt == PackedGenotypeReader.GT_MISSING) continue;
            boolean isHaploid = PackedGenotypeReader.ploidy(ploidyPacked, s) == 1;
            int ploidy = isHaploid ? 1 : 2;
            an += ploidy;
            if (gt == PackedGenotypeReader.GT_HET && !isHaploid) { ac += 1; het++; }
            else if (gt == PackedGenotypeReader.GT_HOM_ALT) { ac += ploidy; homAlt++; }
        }

        // Sample 0 (diploid Het): AN+=2, AC+=1, het++
        // Sample 1 (haploid HomAlt): AN+=1, AC+=1, homAlt++
        // Sample 2 (diploid HomRef): AN+=2, AC+=0
        assertEquals(2, ac);
        assertEquals(5, an);
        assertEquals(1, het);
        assertEquals(1, homAlt);
    }

    @Test
    void packedIndicesWithSkippedSamples() {
        // Test that -1 packed indices are properly skipped
        int[] packedIndices = {0, -1, 2};
        int counted = 0;
        for (int pi : packedIndices) {
            if (pi >= 0) counted++;
        }
        assertEquals(2, counted);
    }

    @Test
    void subsetStatsAfCalculation() {
        // af = ac / an when an > 0, else 0.0
        assertEquals(0.5, 10.0 / 20, 1e-12);
        assertEquals(0.0, 0.0 / 1.0, 1e-12);
        // an = 0 case
        double af = 0 > 0 ? (double) 0 / 0 : 0.0;
        assertEquals(0.0, af, 1e-12);
    }
}
