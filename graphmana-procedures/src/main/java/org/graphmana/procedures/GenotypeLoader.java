package org.graphmana.procedures;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.*;

/**
 * Builds sample indices and packed_index mappings for genotype access.
 */
final class GenotypeLoader {

    private GenotypeLoader() {}

    /**
     * Build sample index: maps sampleId → array position for a given population.
     * The order is deterministic (sorted by sampleId).
     */
    static Map<String, Integer> buildSampleIndex(Transaction tx, String population) {
        var result = tx.execute(
                "MATCH (s:Sample)-[:IN_POPULATION]->(:Population {populationId: $pop}) " +
                "RETURN s.sampleId AS sid ORDER BY sid",
                Map.of("pop", population)
        );

        Map<String, Integer> index = new LinkedHashMap<>();
        try {
            while (result.hasNext()) {
                String sid = (String) result.next().get("sid");
                index.put(sid, index.size());
            }
        } finally {
            result.close();
        }
        return index;
    }

    /**
     * Build sample index from an explicit list of sample IDs.
     * Queries Sample nodes by ID, preserving the order of the input list.
     * Unknown sample IDs are silently skipped.
     */
    static Map<String, Integer> buildSampleIndex(Transaction tx, List<String> sampleIds) {
        Map<String, Integer> index = new LinkedHashMap<>();
        var result = tx.execute(
                "MATCH (s:Sample) WHERE s.sampleId IN $sids RETURN s.sampleId AS sid",
                Map.of("sids", sampleIds)
        );

        // Collect existing sample IDs
        Set<String> existing = new HashSet<>();
        try {
            while (result.hasNext()) {
                existing.add((String) result.next().get("sid"));
            }
        } finally {
            result.close();
        }

        // Preserve input order, skip non-existent
        for (String sid : sampleIds) {
            if (existing.contains(sid) && !index.containsKey(sid)) {
                index.put(sid, index.size());
            }
        }
        return index;
    }

    /**
     * Build a mapping from matrix position → packed_index (VCF column order)
     * for each sample in the given sample index.
     *
     * <p>The packed_index is stored as an int property on each Sample node and
     * represents the sample's position in the packed genotype arrays on Variant nodes.</p>
     *
     * @param tx          active transaction
     * @param sampleIndex sampleId → matrix position (from buildSampleIndex)
     * @return int array where result[matrixPos] = packed_index for that sample
     */
    static int[] buildPackedIndices(Transaction tx, Map<String, Integer> sampleIndex) {
        int nSamples = sampleIndex.size();
        int[] packedIndices = new int[nSamples];
        // Default to -1 (not found)
        java.util.Arrays.fill(packedIndices, -1);

        for (Map.Entry<String, Integer> entry : sampleIndex.entrySet()) {
            String sid = entry.getKey();
            int matrixPos = entry.getValue();
            Node sampleNode = tx.findNode(Label.label("Sample"), "sampleId", sid);
            if (sampleNode != null) {
                Object piObj = sampleNode.getProperty("packed_index", null);
                if (piObj != null) {
                    packedIndices[matrixPos] = ((Number) piObj).intValue();
                }
            }
        }
        return packedIndices;
    }

}
