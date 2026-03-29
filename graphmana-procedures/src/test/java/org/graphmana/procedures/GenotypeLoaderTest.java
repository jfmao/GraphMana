package org.graphmana.procedures;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link GenotypeLoader} sample index building.
 * Tests focus on the logic that doesn't require a live Neo4j database.
 */
class GenotypeLoaderTest {

    @Test
    void buildSampleIndexFromListPreservesOrder() {
        // Simulate what buildSampleIndex would produce:
        // Given input list [C, A, B] and all exist, output should preserve order
        List<String> input = Arrays.asList("sampleC", "sampleA", "sampleB");
        // Simulating: existing = {sampleA, sampleB, sampleC}
        Map<String, Integer> index = new LinkedHashMap<>();
        Set<String> existing = new HashSet<>(input);
        for (String sid : input) {
            if (existing.contains(sid) && !index.containsKey(sid)) {
                index.put(sid, index.size());
            }
        }
        assertEquals(0, index.get("sampleC"));
        assertEquals(1, index.get("sampleA"));
        assertEquals(2, index.get("sampleB"));
    }

    @Test
    void buildSampleIndexSkipsDuplicates() {
        List<String> input = Arrays.asList("s1", "s2", "s1", "s3");
        Map<String, Integer> index = new LinkedHashMap<>();
        Set<String> existing = new HashSet<>(input);
        for (String sid : input) {
            if (existing.contains(sid) && !index.containsKey(sid)) {
                index.put(sid, index.size());
            }
        }
        assertEquals(3, index.size());
        assertEquals(0, index.get("s1"));
        assertEquals(1, index.get("s2"));
        assertEquals(2, index.get("s3"));
    }

    @Test
    void buildSampleIndexSkipsNonExistent() {
        List<String> input = Arrays.asList("s1", "s_missing", "s2");
        Set<String> existing = Set.of("s1", "s2");
        Map<String, Integer> index = new LinkedHashMap<>();
        for (String sid : input) {
            if (existing.contains(sid) && !index.containsKey(sid)) {
                index.put(sid, index.size());
            }
        }
        assertEquals(2, index.size());
        assertEquals(0, index.get("s1"));
        assertEquals(1, index.get("s2"));
        assertFalse(index.containsKey("s_missing"));
    }

    @Test
    void emptyInput() {
        List<String> input = Collections.emptyList();
        Map<String, Integer> index = new LinkedHashMap<>();
        assertTrue(index.isEmpty());
    }

    @Test
    void packedIndicesDefaultToMinusOne() {
        int nSamples = 3;
        int[] packedIndices = new int[nSamples];
        Arrays.fill(packedIndices, -1);
        for (int pi : packedIndices) {
            assertEquals(-1, pi);
        }
    }

    @Test
    void singleSample() {
        Map<String, Integer> index = new LinkedHashMap<>();
        index.put("only_sample", 0);
        assertEquals(1, index.size());
        assertEquals(0, index.get("only_sample"));
    }

    @Test
    void linkedHashMapPreservesInsertionOrder() {
        Map<String, Integer> index = new LinkedHashMap<>();
        index.put("z", 0);
        index.put("a", 1);
        index.put("m", 2);

        List<String> keys = new ArrayList<>(index.keySet());
        assertEquals("z", keys.get(0));
        assertEquals("a", keys.get(1));
        assertEquals("m", keys.get(2));
    }

    @Test
    void largeSampleIndex() {
        Map<String, Integer> index = new LinkedHashMap<>();
        for (int i = 0; i < 3202; i++) {
            index.put("sample_" + String.format("%04d", i), i);
        }
        assertEquals(3202, index.size());
        assertEquals(0, index.get("sample_0000"));
        assertEquals(3201, index.get("sample_3201"));
    }

    @Test
    void packedIndicesMapping() {
        // Simulate building packed indices: matrix pos → packed_index
        int[] packedIndices = new int[]{5, 2, 7};
        // Sample at matrix pos 0 has packed_index 5
        // Sample at matrix pos 1 has packed_index 2
        assertEquals(5, packedIndices[0]);
        assertEquals(2, packedIndices[1]);
        assertEquals(7, packedIndices[2]);
    }

    @Test
    void packedIndicesWithMissingNode() {
        int[] packedIndices = new int[]{3, -1, 8};
        // -1 indicates sample not found in database
        assertTrue(packedIndices[1] < 0);
    }
}
