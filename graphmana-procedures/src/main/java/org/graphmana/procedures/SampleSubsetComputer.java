package org.graphmana.procedures;

import org.neo4j.graphdb.Node;

import java.util.Map;

/**
 * Recomputes population statistics on the fly for a custom sample subset.
 *
 * <p>When a user provides a {@code samples} option, FAST PATH procedures
 * cannot use the pre-computed per-population arrays (ac, an, af, het_count, etc.)
 * because those are for the full population. This class reads packed genotype
 * arrays for the specified samples and recomputes AC/AN/AF/het per variant.</p>
 */
final class SampleSubsetComputer {

    private SampleSubsetComputer() {}

    /**
     * Recompute allele statistics for a single variant from packed genotype arrays.
     *
     * <p>Ploidy-aware: reads optional {@code ploidy_packed} from the variant node.
     * Haploid samples (bit=1) contribute 1 allele to AN per called sample and 1 to AC
     * per ALT call. Diploid samples contribute 2 alleles as before.</p>
     *
     * @param variant       the Variant node (must have gt_packed; ploidy_packed optional)
     * @param sampleIndex   mapping from sampleId to array position (only samples in this map are counted)
     * @param packedIndices array where packedIndices[matrixPos] = VCF column index (packed_index)
     * @return computed statistics for the variant
     */
    static SubsetStats compute(Node variant, Map<String, Integer> sampleIndex, int[] packedIndices) {
        int nSamples = sampleIndex.size();
        int ac = 0;
        int an = 0;
        int hetCount = 0;
        int homAltCount = 0;

        byte[] gtPacked = (byte[]) variant.getProperty("gt_packed");
        Object ploidyObj = variant.getProperty("ploidy_packed", null);
        byte[] ploidyPacked = (ploidyObj instanceof byte[]) ? (byte[]) ploidyObj : null;

        for (int s = 0; s < nSamples; s++) {
            int pi = packedIndices[s];
            if (pi < 0) continue;
            int gt = PackedGenotypeReader.genotype(gtPacked, pi);
            if (gt == PackedGenotypeReader.GT_MISSING) continue;

            boolean isHaploid = PackedGenotypeReader.ploidy(ploidyPacked, pi) == 1;
            int ploidy = isHaploid ? 1 : 2;
            an += ploidy;

            if (gt == PackedGenotypeReader.GT_HET && !isHaploid) {
                ac += 1;
                hetCount++;
            } else if (gt == PackedGenotypeReader.GT_HOM_ALT) {
                ac += ploidy;
                homAltCount++;
            }
            // GT_HOM_REF → 0 alleles
        }

        double af = an > 0 ? (double) ac / an : 0.0;

        return new SubsetStats(ac, an, af, hetCount, homAltCount);
    }

    /**
     * Container for recomputed per-variant statistics from a sample subset.
     */
    static class SubsetStats {
        final int ac;
        final int an;
        final double af;
        final int hetCount;
        final int homAltCount;

        SubsetStats(int ac, int an, double af, int hetCount, int homAltCount) {
            this.ac = ac;
            this.an = an;
            this.af = af;
            this.hetCount = hetCount;
            this.homAltCount = homAltCount;
        }
    }
}
