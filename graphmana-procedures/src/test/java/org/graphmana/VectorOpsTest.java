package org.graphmana;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link VectorOps} array math operations.
 * Statistical methods (Fst, pi, D', r², etc.) belong to GraphPop.
 */
class VectorOpsTest {

    private static final double EPS = 1e-12;

    // ---- dotProduct ----

    @Test
    void dotProductSimple() {
        assertEquals(32.0, VectorOps.dotProduct(
                new double[]{1, 2, 3}, new double[]{4, 5, 6}), EPS);
    }

    @Test
    void dotProductSingleElement() {
        assertEquals(6.0, VectorOps.dotProduct(new double[]{2}, new double[]{3}), EPS);
    }

    @Test
    void dotProductEmpty() {
        assertEquals(0.0, VectorOps.dotProduct(new double[0], new double[0]), EPS);
    }

    @Test
    void dotProductLargeArray() {
        int n = 1000;
        double[] a = new double[n];
        double[] b = new double[n];
        double expected = 0;
        for (int i = 0; i < n; i++) {
            a[i] = i * 0.1;
            b[i] = (n - i) * 0.1;
            expected += a[i] * b[i];
        }
        assertEquals(expected, VectorOps.dotProduct(a, b), 1e-6);
    }

    // ---- sum ----

    @Test
    void sumSimple() {
        assertEquals(10.0, VectorOps.sum(new double[]{1, 2, 3, 4}), EPS);
    }

    @Test
    void sumLargeArray() {
        int n = 10000;
        double[] a = new double[n];
        for (int i = 0; i < n; i++) a[i] = 1.0;
        assertEquals(n, VectorOps.sum(a), EPS);
    }

    // ---- sumOfSquares ----

    @Test
    void sumOfSquares() {
        assertEquals(14.0, VectorOps.sumOfSquares(new double[]{1, 2, 3}), EPS);
    }

    // ---- euclideanDistance ----

    @Test
    void euclideanDistanceSimple() {
        assertEquals(5.0, VectorOps.euclideanDistance(
                new double[]{0, 0}, new double[]{3, 4}), EPS);
    }

    @Test
    void euclideanDistanceSameVector() {
        double[] a = {1, 2, 3};
        assertEquals(0.0, VectorOps.euclideanDistance(a, a), EPS);
    }

    @Test
    void euclideanDistanceLargeArray() {
        int n = 500;
        double[] a = new double[n];
        double[] b = new double[n];
        for (int i = 0; i < n; i++) {
            a[i] = i;
            b[i] = i + 1;
        }
        // Each diff is 1.0, so distance = sqrt(n)
        assertEquals(Math.sqrt(n), VectorOps.euclideanDistance(a, b), 1e-10);
    }

    // ---- cosineSimilarity ----

    @Test
    void cosineSimilarityParallel() {
        assertEquals(1.0, VectorOps.cosineSimilarity(
                new double[]{1, 2, 3}, new double[]{2, 4, 6}), EPS);
    }

    @Test
    void cosineSimilarityAntiparallel() {
        assertEquals(-1.0, VectorOps.cosineSimilarity(
                new double[]{1, 0}, new double[]{-1, 0}), EPS);
    }

    @Test
    void cosineSimilarityOrthogonal() {
        assertEquals(0.0, VectorOps.cosineSimilarity(
                new double[]{1, 0}, new double[]{0, 1}), EPS);
    }

    @Test
    void cosineSimilarityZeroVector() {
        assertEquals(0.0, VectorOps.cosineSimilarity(
                new double[]{0, 0}, new double[]{1, 2}), EPS);
    }

    // ---- subtract ----

    @Test
    void subtractSimple() {
        double[] result = VectorOps.subtract(new double[]{5, 3, 1}, new double[]{1, 2, 3});
        assertArrayEquals(new double[]{4, 1, -2}, result, EPS);
    }

    // ---- expectedHeterozygosity ----

    @Test
    void expectedHeterozygosityFixed() {
        double[] he = VectorOps.expectedHeterozygosity(new double[]{0.0, 1.0});
        assertEquals(0.0, he[0], EPS);
        assertEquals(0.0, he[1], EPS);
    }

    @Test
    void expectedHeterozygosityMaximal() {
        double[] he = VectorOps.expectedHeterozygosity(new double[]{0.5});
        assertEquals(0.5, he[0], EPS); // 2 * 0.5 * 0.5 = 0.5
    }

    @Test
    void expectedHeterozygosityGeneral() {
        double[] he = VectorOps.expectedHeterozygosity(new double[]{0.3});
        assertEquals(2.0 * 0.3 * 0.7, he[0], EPS);
    }

    // ---- scale ----

    @Test
    void scaleSimple() {
        double[] result = VectorOps.scale(new double[]{1, 2, 3}, 2.0);
        assertArrayEquals(new double[]{2, 4, 6}, result, EPS);
    }

    @Test
    void scaleByZero() {
        double[] result = VectorOps.scale(new double[]{5, 10}, 0.0);
        assertArrayEquals(new double[]{0, 0}, result, EPS);
    }

    // ---- normalize ----

    @Test
    void normalizeUnitVector() {
        double[] result = VectorOps.normalize(new double[]{3, 4});
        assertEquals(1.0, Math.sqrt(result[0] * result[0] + result[1] * result[1]), EPS);
        assertEquals(0.6, result[0], EPS);
        assertEquals(0.8, result[1], EPS);
    }

    @Test
    void normalizeZeroVector() {
        double[] result = VectorOps.normalize(new double[]{0, 0, 0});
        assertArrayEquals(new double[]{0, 0, 0}, result, EPS);
    }

    // ---- mean ----

    @Test
    void meanSimple() {
        assertEquals(2.5, VectorOps.mean(new double[]{1, 2, 3, 4}), EPS);
    }

    @Test
    void meanEmpty() {
        assertEquals(0.0, VectorOps.mean(new double[0]), EPS);
    }

    @Test
    void meanSingle() {
        assertEquals(42.0, VectorOps.mean(new double[]{42.0}), EPS);
    }
}
