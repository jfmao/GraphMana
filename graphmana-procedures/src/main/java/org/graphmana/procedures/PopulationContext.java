package org.graphmana.procedures;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * Resolved population metadata: index into Variant arrays, harmonic numbers,
 * and sample count. Shared across procedures.
 */
final class PopulationContext {

    final int index;
    final double a_n;
    final double a_n2;
    final int nSamples;

    private PopulationContext(int index, double a_n, double a_n2, int nSamples) {
        this.index = index;
        this.a_n = a_n;
        this.a_n2 = a_n2;
        this.nSamples = nSamples;
    }

    /**
     * Resolve the population index from a Variant node's pop_ids array
     * and fetch harmonic numbers from the Population node.
     */
    static PopulationContext resolve(Transaction tx, Node firstVariant, String pop) {
        String[] popIds = ArrayUtil.toStringArray(firstVariant.getProperty("pop_ids"));
        int idx = -1;
        for (int i = 0; i < popIds.length; i++) {
            if (popIds[i].equals(pop)) {
                idx = i;
                break;
            }
        }
        if (idx < 0) {
            throw new RuntimeException("Population not found in variant pop_ids: " + pop);
        }

        Node popNode = tx.findNode(Label.label("Population"), "populationId", pop);
        if (popNode == null) {
            throw new RuntimeException("Population node not found: " + pop);
        }

        double a_n = ((Number) popNode.getProperty("a_n")).doubleValue();
        double a_n2 = ((Number) popNode.getProperty("a_n2")).doubleValue();
        int nSamples = ((Number) popNode.getProperty("n_samples")).intValue();

        return new PopulationContext(idx, a_n, a_n2, nSamples);
    }

    /**
     * Resolve a population index only (no harmonic numbers needed).
     */
    static int resolveIndex(String[] popIds, String pop) {
        for (int i = 0; i < popIds.length; i++) {
            if (popIds[i].equals(pop)) return i;
        }
        throw new RuntimeException("Population not found: " + pop);
    }
}
