package org.graphmana;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD-accelerated numeric operations for array math.
 *
 * <p>Uses the Java Vector API ({@code jdk.incubator.vector}) to perform
 * vectorised arithmetic on numeric arrays. Statistical procedures
 * (Fst, pi, D', r², etc.) belong to GraphPop — this class provides
 * only the foundational array operations.</p>
 */
public final class VectorOps {

    private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;

    private VectorOps() {
        // utility class
    }

    /**
     * Compute the dot product of two double arrays using SIMD lanes.
     *
     * @param a first array
     * @param b second array (must be same length as {@code a})
     * @return dot product
     */
    public static double dotProduct(double[] a, double[] b) {
        int i = 0;
        int upperBound = SPECIES.loopBound(a.length);
        DoubleVector sum = DoubleVector.zero(SPECIES);

        for (; i < upperBound; i += SPECIES.length()) {
            DoubleVector va = DoubleVector.fromArray(SPECIES, a, i);
            DoubleVector vb = DoubleVector.fromArray(SPECIES, b, i);
            sum = va.fma(vb, sum);
        }

        double result = sum.reduceLanes(jdk.incubator.vector.VectorOperators.ADD);

        // Scalar tail
        for (; i < a.length; i++) {
            result += a[i] * b[i];
        }
        return result;
    }

    /**
     * Compute the sum of all elements in an array using SIMD lanes.
     *
     * @param a the array
     * @return sum of elements
     */
    public static double sum(double[] a) {
        int i = 0;
        int upperBound = SPECIES.loopBound(a.length);
        DoubleVector acc = DoubleVector.zero(SPECIES);

        for (; i < upperBound; i += SPECIES.length()) {
            DoubleVector va = DoubleVector.fromArray(SPECIES, a, i);
            acc = acc.add(va);
        }

        double result = acc.reduceLanes(jdk.incubator.vector.VectorOperators.ADD);

        for (; i < a.length; i++) {
            result += a[i];
        }
        return result;
    }

    /**
     * Compute the sum of squares of all elements using SIMD lanes.
     *
     * @param a the array
     * @return sum of squared elements
     */
    public static double sumOfSquares(double[] a) {
        return dotProduct(a, a);
    }

    /**
     * Compute the Euclidean distance between two vectors.
     *
     * @param a first vector
     * @param b second vector (same length as {@code a})
     * @return Euclidean distance
     */
    public static double euclideanDistance(double[] a, double[] b) {
        int i = 0;
        int upperBound = SPECIES.loopBound(a.length);
        DoubleVector acc = DoubleVector.zero(SPECIES);

        for (; i < upperBound; i += SPECIES.length()) {
            DoubleVector va = DoubleVector.fromArray(SPECIES, a, i);
            DoubleVector vb = DoubleVector.fromArray(SPECIES, b, i);
            DoubleVector diff = va.sub(vb);
            acc = diff.fma(diff, acc);
        }

        double result = acc.reduceLanes(jdk.incubator.vector.VectorOperators.ADD);

        for (; i < a.length; i++) {
            double d = a[i] - b[i];
            result += d * d;
        }
        return Math.sqrt(result);
    }

    /**
     * Compute cosine similarity between two vectors.
     *
     * @param a first vector
     * @param b second vector (same length as {@code a})
     * @return cosine similarity in [-1, 1]
     */
    public static double cosineSimilarity(double[] a, double[] b) {
        int i = 0;
        int upperBound = SPECIES.loopBound(a.length);
        DoubleVector dotAcc = DoubleVector.zero(SPECIES);
        DoubleVector normAAcc = DoubleVector.zero(SPECIES);
        DoubleVector normBAcc = DoubleVector.zero(SPECIES);

        for (; i < upperBound; i += SPECIES.length()) {
            DoubleVector va = DoubleVector.fromArray(SPECIES, a, i);
            DoubleVector vb = DoubleVector.fromArray(SPECIES, b, i);
            dotAcc = va.fma(vb, dotAcc);
            normAAcc = va.fma(va, normAAcc);
            normBAcc = vb.fma(vb, normBAcc);
        }

        double dot = dotAcc.reduceLanes(jdk.incubator.vector.VectorOperators.ADD);
        double normA = normAAcc.reduceLanes(jdk.incubator.vector.VectorOperators.ADD);
        double normB = normBAcc.reduceLanes(jdk.incubator.vector.VectorOperators.ADD);

        for (; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }

        double denom = Math.sqrt(normA) * Math.sqrt(normB);
        return denom == 0.0 ? 0.0 : dot / denom;
    }

    /**
     * Element-wise subtraction: result[i] = a[i] - b[i].
     *
     * @param a first array
     * @param b second array (same length)
     * @return new array with differences
     */
    public static double[] subtract(double[] a, double[] b) {
        double[] result = new double[a.length];
        int i = 0;
        int upperBound = SPECIES.loopBound(a.length);

        for (; i < upperBound; i += SPECIES.length()) {
            DoubleVector va = DoubleVector.fromArray(SPECIES, a, i);
            DoubleVector vb = DoubleVector.fromArray(SPECIES, b, i);
            va.sub(vb).intoArray(result, i);
        }

        for (; i < a.length; i++) {
            result[i] = a[i] - b[i];
        }
        return result;
    }

    /**
     * Compute expected heterozygosity for each population: He = 2*p*(1-p).
     *
     * @param af allele frequency array (one per population)
     * @return array of He values
     */
    public static double[] expectedHeterozygosity(double[] af) {
        double[] he = new double[af.length];
        int i = 0;
        int upperBound = SPECIES.loopBound(af.length);
        DoubleVector two = DoubleVector.broadcast(SPECIES, 2.0);
        DoubleVector one = DoubleVector.broadcast(SPECIES, 1.0);

        for (; i < upperBound; i += SPECIES.length()) {
            DoubleVector p = DoubleVector.fromArray(SPECIES, af, i);
            // He = 2 * p * (1 - p)
            DoubleVector q = one.sub(p);
            two.mul(p).mul(q).intoArray(he, i);
        }

        for (; i < af.length; i++) {
            he[i] = 2.0 * af[i] * (1.0 - af[i]);
        }
        return he;
    }

    /**
     * Element-wise scale: result[i] = a[i] * scalar.
     *
     * @param a      input array
     * @param scalar scale factor
     * @return new array with scaled values
     */
    public static double[] scale(double[] a, double scalar) {
        double[] result = new double[a.length];
        int i = 0;
        int upperBound = SPECIES.loopBound(a.length);
        DoubleVector vScalar = DoubleVector.broadcast(SPECIES, scalar);

        for (; i < upperBound; i += SPECIES.length()) {
            DoubleVector va = DoubleVector.fromArray(SPECIES, a, i);
            va.mul(vScalar).intoArray(result, i);
        }

        for (; i < a.length; i++) {
            result[i] = a[i] * scalar;
        }
        return result;
    }

    /**
     * Normalize an array to unit length (L2 norm).
     *
     * @param a input array
     * @return new array with L2 norm = 1, or zero array if input is zero
     */
    public static double[] normalize(double[] a) {
        double norm = Math.sqrt(sumOfSquares(a));
        if (norm == 0.0) return new double[a.length];
        return scale(a, 1.0 / norm);
    }

    /**
     * Compute the mean of all elements in an array.
     *
     * @param a the array
     * @return mean value, or 0 if empty
     */
    public static double mean(double[] a) {
        if (a.length == 0) return 0.0;
        return sum(a) / a.length;
    }
}
