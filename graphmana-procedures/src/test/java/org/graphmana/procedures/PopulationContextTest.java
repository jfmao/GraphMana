package org.graphmana.procedures;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link PopulationContext} population index resolution.
 */
class PopulationContextTest {

    @Test
    void resolveIndexFirst() {
        String[] popIds = {"CEU", "YRI", "CHB"};
        assertEquals(0, PopulationContext.resolveIndex(popIds, "CEU"));
    }

    @Test
    void resolveIndexMiddle() {
        String[] popIds = {"CEU", "YRI", "CHB"};
        assertEquals(1, PopulationContext.resolveIndex(popIds, "YRI"));
    }

    @Test
    void resolveIndexLast() {
        String[] popIds = {"CEU", "YRI", "CHB"};
        assertEquals(2, PopulationContext.resolveIndex(popIds, "CHB"));
    }

    @Test
    void resolveIndexNotFound() {
        String[] popIds = {"CEU", "YRI", "CHB"};
        assertThrows(RuntimeException.class, () -> PopulationContext.resolveIndex(popIds, "GBR"));
    }

    @Test
    void resolveIndexSinglePop() {
        String[] popIds = {"ALL"};
        assertEquals(0, PopulationContext.resolveIndex(popIds, "ALL"));
    }

    @Test
    void resolveIndexCaseSensitive() {
        String[] popIds = {"CEU", "ceu"};
        assertEquals(0, PopulationContext.resolveIndex(popIds, "CEU"));
        assertEquals(1, PopulationContext.resolveIndex(popIds, "ceu"));
    }

    @Test
    void resolveIndexEmptyString() {
        String[] popIds = {"", "CEU"};
        assertEquals(0, PopulationContext.resolveIndex(popIds, ""));
    }

    @Test
    void resolveIndexManyPops() {
        String[] popIds = new String[26];
        for (int i = 0; i < 26; i++) {
            popIds[i] = "POP" + (char) ('A' + i);
        }
        assertEquals(25, PopulationContext.resolveIndex(popIds, "POPZ"));
    }
}
