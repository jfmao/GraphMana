package org.graphmana.procedures;

import org.neo4j.graphdb.Node;

import java.util.Map;

/**
 * Per-variant property filter applied in the hot loop of every procedure.
 *
 * <p>Parsed once from the user-supplied options map, then checked per variant
 * via {@link #passes}. When no filters are set, {@link #isActive()} returns false
 * and callers can skip the filter entirely for zero overhead.</p>
 *
 * <p>Supported filter keys in the options map:
 * <ul>
 *   <li>{@code min_af} — minimum allele frequency (default 0.0)</li>
 *   <li>{@code max_af} — maximum allele frequency (default 1.0)</li>
 *   <li>{@code min_call_rate} — minimum call rate (default 0.0)</li>
 *   <li>{@code variant_type} — filter by variant type: "SNP" or "INDEL" (default null)</li>
 * </ul>
 * </p>
 */
final class VariantFilter {

    final double minAf;
    final double maxAf;
    final double minCallRate;
    final String variantType;

    private final boolean active;

    private VariantFilter(double minAf, double maxAf, double minCallRate, String variantType) {
        this.minAf = minAf;
        this.maxAf = maxAf;
        this.minCallRate = minCallRate;
        this.variantType = variantType;

        this.active = minAf > 0.0 || maxAf < 1.0 || minCallRate > 0.0 || variantType != null;
    }

    /**
     * Parse filter parameters from the user-supplied options map.
     * Missing keys use safe defaults (no filtering).
     */
    static VariantFilter fromOptions(Map<String, Object> options) {
        if (options == null || options.isEmpty()) {
            return new VariantFilter(0.0, 1.0, 0.0, null);
        }

        double minAf = getDouble(options, "min_af", 0.0);
        double maxAf = getDouble(options, "max_af", 1.0);
        double minCallRate = getDouble(options, "min_call_rate", 0.0);
        String variantType = (String) options.get("variant_type");

        return new VariantFilter(minAf, maxAf, minCallRate, variantType);
    }

    /**
     * Returns true if any filter criterion is active.
     * When false, callers can skip the filter check entirely.
     */
    boolean isActive() {
        return active;
    }

    /**
     * FAST PATH: check a variant against all filter criteria using pre-computed
     * per-population arrays.
     *
     * @param ac          allele count for this population
     * @param an          allele number for this population
     * @param af          allele frequency for this population
     * @param hetCount    heterozygote count for this population
     * @param homAltCount homozygous alt count for this population
     * @param variant     the Variant node (for variant_type check)
     * @return true if the variant passes all filters
     */
    boolean passes(int ac, int an, double af, int hetCount, int homAltCount,
                   Node variant) {
        // AF filter
        if (af < minAf || af > maxAf) return false;

        // Call rate: an / max_an
        if (minCallRate > 0.0 && variant != null) {
            Object anTotalObj = variant.getProperty("an_total", null);
            if (anTotalObj != null) {
                int anTotal = ((Number) anTotalObj).intValue();
                if (anTotal > 0) {
                    double callRate = (double) an / anTotal;
                    if (callRate < minCallRate) return false;
                }
            }
        }

        // Variant type filter
        if (variantType != null && variant != null) {
            Object vt = variant.getProperty("variant_type", null);
            if (vt == null || !variantType.equals(vt)) return false;
        }

        return true;
    }

    /**
     * Convenience: extract arrays from Node and check at a given population index.
     */
    boolean passes(Node variant, int popIndex) {
        int[] acArr = ArrayUtil.toIntArray(variant.getProperty("ac"));
        int[] anArr = ArrayUtil.toIntArray(variant.getProperty("an"));
        double[] afArr = ArrayUtil.toDoubleArray(variant.getProperty("af"));
        int[] hetArr = ArrayUtil.toIntArray(variant.getProperty("het_count"));
        int[] homAltArr = ArrayUtil.toIntArray(variant.getProperty("hom_alt_count"));

        return passes(acArr[popIndex], anArr[popIndex], afArr[popIndex],
                hetArr[popIndex], homAltArr[popIndex], variant);
    }

    private static double getDouble(Map<String, Object> options, String key, double defaultValue) {
        Object val = options.get(key);
        if (val == null) return defaultValue;
        return ((Number) val).doubleValue();
    }
}
