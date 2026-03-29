package org.graphmana.procedures;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link VariantFilter} — AF, call rate, and variant type filtering.
 * HWE filter is intentionally excluded (belongs to GraphPop).
 */
class VariantFilterTest {

    @Test
    void noFiltersInactive() {
        VariantFilter f = VariantFilter.fromOptions(null);
        assertFalse(f.isActive());
    }

    @Test
    void emptyMapInactive() {
        VariantFilter f = VariantFilter.fromOptions(Map.of());
        assertFalse(f.isActive());
    }

    @Test
    void minAfFilterActive() {
        VariantFilter f = VariantFilter.fromOptions(Map.of("min_af", 0.05));
        assertTrue(f.isActive());
        // AF = 0.01 should fail
        assertFalse(f.passes(1, 100, 0.01, 1, 0, null));
        // AF = 0.10 should pass
        assertTrue(f.passes(10, 100, 0.10, 8, 1, null));
    }

    @Test
    void maxAfFilterActive() {
        VariantFilter f = VariantFilter.fromOptions(Map.of("max_af", 0.5));
        assertTrue(f.isActive());
        // AF = 0.8 should fail
        assertFalse(f.passes(80, 100, 0.8, 20, 30, null));
        // AF = 0.3 should pass
        assertTrue(f.passes(30, 100, 0.3, 25, 2, null));
    }

    @Test
    void combinedAfFilter() {
        Map<String, Object> opts = new HashMap<>();
        opts.put("min_af", 0.05);
        opts.put("max_af", 0.95);
        VariantFilter f = VariantFilter.fromOptions(opts);
        assertTrue(f.isActive());

        assertFalse(f.passes(1, 100, 0.01, 1, 0, null));  // too low
        assertFalse(f.passes(98, 100, 0.98, 2, 48, null)); // too high
        assertTrue(f.passes(50, 100, 0.50, 40, 5, null));   // in range
    }

    @Test
    void variantTypeFilter() {
        VariantFilter f = VariantFilter.fromOptions(Map.of("variant_type", "SNP"));
        assertTrue(f.isActive());
        // Without a Node, variant_type check is skipped (no node to read property from)
        assertTrue(f.passes(10, 100, 0.1, 8, 1, null));
    }

    @Test
    void integerValuesInOptions() {
        // Neo4j might return integers instead of doubles
        Map<String, Object> opts = new HashMap<>();
        opts.put("min_af", 0);  // int 0, should become 0.0
        opts.put("max_af", 1);  // int 1, should become 1.0
        VariantFilter f = VariantFilter.fromOptions(opts);
        // 0.0 min and 1.0 max → inactive
        assertFalse(f.isActive());
    }

    @Test
    void defaultValues() {
        VariantFilter f = VariantFilter.fromOptions(null);
        assertEquals(0.0, f.minAf);
        assertEquals(1.0, f.maxAf);
        assertEquals(0.0, f.minCallRate);
        assertNull(f.variantType);
    }

    @Test
    void passesWithZeroAn() {
        VariantFilter f = VariantFilter.fromOptions(Map.of("min_af", 0.0));
        // af = 0.0, should pass default
        assertTrue(f.passes(0, 0, 0.0, 0, 0, null));
    }

    @Test
    void minCallRateFilter() {
        VariantFilter f = VariantFilter.fromOptions(Map.of("min_call_rate", 0.9));
        assertTrue(f.isActive());
        // Without a node, call_rate check is skipped → passes
        assertTrue(f.passes(10, 100, 0.1, 8, 1, null));
    }

    @Test
    void allFiltersInactive() {
        Map<String, Object> opts = new HashMap<>();
        opts.put("min_af", 0.0);
        opts.put("max_af", 1.0);
        opts.put("min_call_rate", 0.0);
        VariantFilter f = VariantFilter.fromOptions(opts);
        assertFalse(f.isActive());
    }

    @Test
    void edgeAfValues() {
        VariantFilter f = VariantFilter.fromOptions(Map.of("min_af", 0.05, "max_af", 0.95));
        // Exact boundary values
        assertTrue(f.passes(5, 100, 0.05, 5, 0, null));   // exactly at min
        assertTrue(f.passes(95, 100, 0.95, 5, 45, null));  // exactly at max
    }

    @Test
    void fieldAccessors() {
        Map<String, Object> opts = new HashMap<>();
        opts.put("min_af", 0.01);
        opts.put("max_af", 0.99);
        opts.put("min_call_rate", 0.8);
        opts.put("variant_type", "INDEL");
        VariantFilter f = VariantFilter.fromOptions(opts);

        assertEquals(0.01, f.minAf, 1e-12);
        assertEquals(0.99, f.maxAf, 1e-12);
        assertEquals(0.8, f.minCallRate, 1e-12);
        assertEquals("INDEL", f.variantType);
    }
}
