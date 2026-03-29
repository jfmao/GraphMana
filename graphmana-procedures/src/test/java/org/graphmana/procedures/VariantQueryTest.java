package org.graphmana.procedures;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link VariantQuery} Cypher query builder.
 */
class VariantQueryTest {

    @Test
    void buildNoFilters() {
        String q = VariantQuery.build(null, null);
        assertTrue(q.contains("MATCH (v:Variant)"));
        assertTrue(q.contains("v.chr = $chr"));
        assertTrue(q.contains("v.pos >= $start"));
        assertTrue(q.contains("v.pos <= $end"));
        assertTrue(q.contains("RETURN DISTINCT v"));
        assertFalse(q.contains("HAS_CONSEQUENCE"));
    }

    @Test
    void buildWithConsequence() {
        String q = VariantQuery.build("missense_variant", null);
        assertTrue(q.contains("HAS_CONSEQUENCE"));
        assertTrue(q.contains("missense_variant"));
        assertFalse(q.contains("IN_PATHWAY"));
    }

    @Test
    void buildWithPathway() {
        String q = VariantQuery.build(null, "Signal Transduction");
        assertTrue(q.contains("IN_PATHWAY"));
        assertTrue(q.contains("Signal Transduction"));
        assertFalse(q.contains("c.consequence"));
    }

    @Test
    void buildWithBothFilters() {
        String q = VariantQuery.build("stop_gained", "Apoptosis");
        assertTrue(q.contains("HAS_CONSEQUENCE"));
        assertTrue(q.contains("IN_PATHWAY"));
        assertTrue(q.contains("stop_gained"));
        assertTrue(q.contains("Apoptosis"));
    }

    @Test
    void buildFromOptionsMap() {
        Map<String, Object> opts = new HashMap<>();
        opts.put("consequence", "synonymous_variant");
        String q = VariantQuery.build(opts);
        assertTrue(q.contains("synonymous_variant"));
    }

    @Test
    void buildFromNullOptions() {
        String q = VariantQuery.build((Map<String, Object>) null);
        assertTrue(q.contains("MATCH (v:Variant)"));
        assertFalse(q.contains("HAS_CONSEQUENCE"));
    }

    @Test
    void buildChromosomeNoFilters() {
        String q = VariantQuery.buildChromosome(null);
        assertTrue(q.contains("v.chr = $chr"));
        assertFalse(q.contains("$start"));
        assertFalse(q.contains("$end"));
        assertTrue(q.contains("ORDER BY v.pos"));
    }

    @Test
    void buildChromosomeWithConsequence() {
        Map<String, Object> opts = Map.of("consequence", "frameshift_variant");
        String q = VariantQuery.buildChromosome(opts);
        assertTrue(q.contains("HAS_CONSEQUENCE"));
        assertTrue(q.contains("frameshift_variant"));
        assertTrue(q.contains("ORDER BY v.pos"));
    }

    @Test
    void sqlInjectionEscaped() {
        String q = VariantQuery.build("test'; DROP TABLE--", null);
        assertTrue(q.contains("test''; DROP TABLE--"));
    }

    @Test
    void buildReturnDistinct() {
        String q = VariantQuery.build("missense_variant", "SomePathway");
        assertTrue(q.contains("RETURN DISTINCT v"));
    }
}
