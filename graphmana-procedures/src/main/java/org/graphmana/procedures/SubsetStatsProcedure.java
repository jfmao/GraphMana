package org.graphmana.procedures;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Server-side procedure that computes allele counts and frequencies for an
 * arbitrary sample subset without transferring packed arrays to the client.
 *
 * <p>Uses {@link PackedGenotypeReader} and {@link SampleSubsetComputer} to
 * unpack genotypes only for the requested samples, producing per-variant
 * AC/AN/AF/het statistics.</p>
 */
public class SubsetStatsProcedure {

    @Context
    public Transaction tx;

    /**
     * Compute allele statistics for a custom sample subset.
     *
     * @param sampleIds  list of sample IDs to include
     * @param chromosome optional chromosome filter (null = all)
     * @param minAF      optional minimum allele frequency filter
     * @param maxAF      optional maximum allele frequency filter
     * @return stream of per-variant statistics
     */
    @Procedure(name = "graphmana.subsetStats", mode = Mode.READ)
    @Description("Compute allele counts/frequencies for an arbitrary sample subset.")
    public Stream<SubsetStatsResult> subsetStats(
            @Name("sampleIds") List<String> sampleIds,
            @Name(value = "chromosome", defaultValue = "") String chromosome,
            @Name(value = "minAF", defaultValue = "0.0") Double minAF,
            @Name(value = "maxAF", defaultValue = "1.0") Double maxAF) {

        if (sampleIds == null || sampleIds.isEmpty()) {
            return Stream.empty();
        }

        // Build sample index and packed indices
        Map<String, Integer> sampleIndex = GenotypeLoader.buildSampleIndex(tx, sampleIds);
        if (sampleIndex.isEmpty()) {
            return Stream.empty();
        }
        int[] packedIndices = GenotypeLoader.buildPackedIndices(tx, sampleIndex);

        // Build variant query
        String query;
        Map<String, Object> params;
        if (chromosome != null && !chromosome.isEmpty()) {
            query = "MATCH (v:Variant) WHERE v.chr = $chr RETURN v ORDER BY v.pos";
            params = Map.of("chr", chromosome);
        } else {
            query = "MATCH (v:Variant) RETURN v ORDER BY v.chr, v.pos";
            params = Map.of();
        }

        double minAf = minAF != null ? minAF : 0.0;
        double maxAf = maxAF != null ? maxAF : 1.0;

        var result = tx.execute(query, params);

        return StreamSupport.stream(
                new java.util.Spliterators.AbstractSpliterator<SubsetStatsResult>(
                        Long.MAX_VALUE, java.util.Spliterator.ORDERED) {
                    @Override
                    public boolean tryAdvance(java.util.function.Consumer<? super SubsetStatsResult> action) {
                        while (result.hasNext()) {
                            var row = result.next();
                            Node v = (Node) row.get("v");

                            SampleSubsetComputer.SubsetStats stats =
                                    SampleSubsetComputer.compute(v, sampleIndex, packedIndices);

                            // Apply AF filter
                            if (stats.af < minAf || stats.af > maxAf) continue;

                            String variantId = (String) v.getProperty("variantId");
                            String chr = (String) v.getProperty("chr");
                            long pos = ((Number) v.getProperty("pos")).longValue();
                            String ref = (String) v.getProperty("ref");
                            String alt = (String) v.getProperty("alt");

                            action.accept(new SubsetStatsResult(
                                    variantId, chr, pos, ref, alt,
                                    stats.ac, stats.an, stats.af,
                                    stats.hetCount, stats.homAltCount));
                            return true;
                        }
                        result.close();
                        return false;
                    }
                }, false);
    }

    public static class SubsetStatsResult {
        public final String variantId;
        public final String chr;
        public final long pos;
        public final String ref;
        public final String alt;
        public final long ac;
        public final long an;
        public final double af;
        public final long hetCount;
        public final long homAltCount;

        public SubsetStatsResult(String variantId, String chr, long pos, String ref, String alt,
                                 int ac, int an, double af, int hetCount, int homAltCount) {
            this.variantId = variantId;
            this.chr = chr;
            this.pos = pos;
            this.ref = ref;
            this.alt = alt;
            this.ac = ac;
            this.an = an;
            this.af = af;
            this.hetCount = hetCount;
            this.homAltCount = homAltCount;
        }
    }
}
