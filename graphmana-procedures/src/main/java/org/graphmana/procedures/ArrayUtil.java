package org.graphmana.procedures;

/**
 * Utility methods for converting Neo4j property values to Java arrays.
 *
 * <p>Neo4j stores array properties as typed arrays (int[], long[], double[], etc.)
 * but the exact type depends on the import method. These helpers handle all
 * common representations.</p>
 */
final class ArrayUtil {

    private ArrayUtil() {}

    static String[] toStringArray(Object prop) {
        if (prop instanceof String[]) return (String[]) prop;
        if (prop instanceof String) return ((String) prop).split(";");
        throw new RuntimeException("Cannot convert to String[]: " + prop.getClass());
    }

    static int[] toIntArray(Object prop) {
        if (prop instanceof int[]) return (int[]) prop;
        if (prop instanceof long[]) {
            long[] la = (long[]) prop;
            int[] ia = new int[la.length];
            for (int i = 0; i < la.length; i++) ia[i] = (int) la[i];
            return ia;
        }
        if (prop instanceof String) {
            String[] parts = ((String) prop).split(";");
            int[] ia = new int[parts.length];
            for (int i = 0; i < parts.length; i++) ia[i] = Integer.parseInt(parts[i].trim());
            return ia;
        }
        throw new RuntimeException("Cannot convert to int[]: " + prop.getClass());
    }

    static double[] toDoubleArray(Object prop) {
        if (prop instanceof double[]) return (double[]) prop;
        if (prop instanceof float[]) {
            float[] fa = (float[]) prop;
            double[] da = new double[fa.length];
            for (int i = 0; i < fa.length; i++) da[i] = fa[i];
            return da;
        }
        if (prop instanceof long[]) {
            long[] la = (long[]) prop;
            double[] da = new double[la.length];
            for (int i = 0; i < la.length; i++) da[i] = la[i];
            return da;
        }
        if (prop instanceof int[]) {
            int[] ia = (int[]) prop;
            double[] da = new double[ia.length];
            for (int i = 0; i < ia.length; i++) da[i] = ia[i];
            return da;
        }
        if (prop instanceof String) {
            String[] parts = ((String) prop).split(";");
            double[] da = new double[parts.length];
            for (int i = 0; i < parts.length; i++) da[i] = Double.parseDouble(parts[i].trim());
            return da;
        }
        throw new RuntimeException("Cannot convert to double[]: " + prop.getClass());
    }
}
