package org.graphmana.procedures;

/**
 * Hardy–Weinberg Equilibrium exact test following Wigginton et al. (2005).
 *
 * <p>Implements the mid-p exact test, which enumerates all possible heterozygote
 * counts for a fixed allele count and total sample count, computes the probability
 * of each configuration under HWE, and sums the probabilities of configurations
 * as or more extreme than the observed one.</p>
 *
 * <p>Reference: Wigginton JE, Cutler DJ, Abecasis GR. A note on exact tests of
 * Hardy-Weinberg equilibrium. Am J Hum Genet. 2005;76(5):887-93.</p>
 */
final class HWEExact {

    private HWEExact() {}

    /**
     * Compute the HWE exact test mid-p value.
     *
     * @param nHet      observed heterozygote count
     * @param nHomMinor observed count of the minor-allele homozygote
     * @param nTotal    total number of diploid individuals
     * @return mid-p value; values near 0 indicate departure from HWE
     */
    static double hweExactMidP(int nHet, int nHomMinor, int nTotal) {
        if (nTotal <= 0) return 1.0;

        // Derive allele counts
        int nHomMajor = nTotal - nHet - nHomMinor;
        if (nHomMajor < 0) return 1.0;

        int minorCount = 2 * nHomMinor + nHet;
        int majorCount = 2 * nHomMajor + nHet;
        int nAlleles = 2 * nTotal;

        if (minorCount > majorCount) {
            int tmp = minorCount;
            minorCount = majorCount;
            majorCount = tmp;
        }

        if (minorCount == 0) return 1.0;

        int hetMax = minorCount;
        if (hetMax > nTotal) hetMax = nTotal;
        if ((hetMax & 1) != (minorCount & 1)) hetMax--;

        int nSteps = hetMax / 2 + 1;
        double[] probs = new double[nSteps];

        int startHet = minorCount % 2;

        int obsIdx = (nHet - startHet) / 2;
        if (obsIdx < 0 || obsIdx >= nSteps) return 1.0;

        probs[obsIdx] = 1.0;

        // Walk upward from observed het
        for (int i = obsIdx + 1; i < nSteps; i++) {
            int het = startHet + 2 * i;
            int hetPrev = het - 2;
            int homMinorPrev = (minorCount - hetPrev) / 2;
            int homMajorPrev = (majorCount - hetPrev) / 2;
            double ratio = 4.0 * homMinorPrev * homMajorPrev / ((double) het * (het - 1));
            probs[i] = probs[i - 1] * ratio;
        }

        // Walk downward from observed het
        for (int i = obsIdx - 1; i >= 0; i--) {
            int het = startHet + 2 * i;
            int hetNext = het + 2;
            int homMinor = (minorCount - het) / 2;
            int homMajor = (majorCount - het) / 2;
            double ratio = (double) hetNext * (hetNext - 1) / (4.0 * homMinor * homMajor);
            probs[i] = probs[i + 1] * ratio;
        }

        // Normalize to sum to 1
        double total = 0.0;
        for (double p : probs) total += p;
        if (total == 0.0) return 1.0;

        double obsProb = probs[obsIdx] / total;

        // Mid-p: sum of probs strictly less than observed + 0.5 * observed
        double pval = 0.0;
        for (int i = 0; i < nSteps; i++) {
            double p = probs[i] / total;
            if (p < obsProb) {
                pval += p;
            } else if (i == obsIdx) {
                pval += 0.5 * p;
            }
        }

        return pval;
    }
}
