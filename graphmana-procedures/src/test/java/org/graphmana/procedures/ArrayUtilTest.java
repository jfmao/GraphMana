package org.graphmana.procedures;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ArrayUtil} type conversion utilities.
 */
class ArrayUtilTest {

    @Test
    void toStringArrayFromStringArray() {
        String[] input = {"pop1", "pop2", "pop3"};
        String[] result = ArrayUtil.toStringArray(input);
        assertArrayEquals(input, result);
    }

    @Test
    void toStringArrayFromSemicolonDelimited() {
        String[] result = ArrayUtil.toStringArray("pop1;pop2;pop3");
        assertArrayEquals(new String[]{"pop1", "pop2", "pop3"}, result);
    }

    @Test
    void toStringArrayUnsupportedType() {
        assertThrows(RuntimeException.class, () -> ArrayUtil.toStringArray(42));
    }

    @Test
    void toIntArrayFromIntArray() {
        int[] input = {10, 20, 30};
        int[] result = ArrayUtil.toIntArray(input);
        assertArrayEquals(input, result);
    }

    @Test
    void toIntArrayFromLongArray() {
        long[] input = {100L, 200L, 300L};
        int[] result = ArrayUtil.toIntArray(input);
        assertArrayEquals(new int[]{100, 200, 300}, result);
    }

    @Test
    void toIntArrayFromString() {
        int[] result = ArrayUtil.toIntArray("5;10;15");
        assertArrayEquals(new int[]{5, 10, 15}, result);
    }

    @Test
    void toDoubleArrayFromDoubleArray() {
        double[] input = {0.1, 0.2, 0.3};
        double[] result = ArrayUtil.toDoubleArray(input);
        assertArrayEquals(input, result, 1e-12);
    }

    @Test
    void toDoubleArrayFromIntArray() {
        int[] input = {1, 2, 3};
        double[] result = ArrayUtil.toDoubleArray(input);
        assertArrayEquals(new double[]{1.0, 2.0, 3.0}, result, 1e-12);
    }
}
