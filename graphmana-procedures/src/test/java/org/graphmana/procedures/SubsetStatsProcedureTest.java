package org.graphmana.procedures;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link SubsetStatsProcedure} using neo4j-harness.
 */
class SubsetStatsProcedureTest {

    private static Neo4j neo4j;

    @BeforeAll
    static void startNeo4j() {
        neo4j = Neo4jBuilders.newInProcessBuilder()
                .withProcedure(SubsetStatsProcedure.class)
                .withFixture(
                        // Create 4 samples with packed_index values
                        "CREATE (s0:Sample {sampleId: 's0', packed_index: 0, population: 'pop1'})" +
                        "CREATE (s1:Sample {sampleId: 's1', packed_index: 1, population: 'pop1'})" +
                        "CREATE (s2:Sample {sampleId: 's2', packed_index: 2, population: 'pop2'})" +
                        "CREATE (s3:Sample {sampleId: 's3', packed_index: 3, population: 'pop2'})" +
                        "CREATE (pop1:Population {populationId: 'pop1', n_samples: 2, a_n: 1.0, a_n2: 1.0})" +
                        "CREATE (pop2:Population {populationId: 'pop2', n_samples: 2, a_n: 1.0, a_n2: 1.0})" +
                        "CREATE (s0)-[:IN_POPULATION]->(pop1)" +
                        "CREATE (s1)-[:IN_POPULATION]->(pop1)" +
                        "CREATE (s2)-[:IN_POPULATION]->(pop2)" +
                        "CREATE (s3)-[:IN_POPULATION]->(pop2)"
                )
                .build();

        // Create variant with packed genotypes:
        // s0=HomRef(0), s1=Het(1), s2=HomAlt(2), s3=Missing(3)
        // Binary: 11_10_01_00 = 0xE4
        try (var session = neo4j.defaultDatabaseService().beginTx()) {
            byte[] gtPacked = new byte[]{(byte) 0xE4};
            session.execute(
                    "CREATE (v:Variant {" +
                    "  variantId: '22:1000:A:T'," +
                    "  chr: '22'," +
                    "  pos: 1000," +
                    "  ref: 'A'," +
                    "  alt: 'T'," +
                    "  variant_type: 'SNP'," +
                    "  gt_packed: $gt," +
                    "  pop_ids: ['pop1', 'pop2']," +
                    "  ac: [1, 2]," +
                    "  an: [4, 2]," +
                    "  af: [0.25, 1.0]," +
                    "  het_count: [1, 0]," +
                    "  hom_alt_count: [0, 1]," +
                    "  ac_total: 3," +
                    "  an_total: 6," +
                    "  af_total: 0.5" +
                    "})",
                    Map.of("gt", gtPacked)
            );
            session.commit();
        }
    }

    @AfterAll
    static void stopNeo4j() {
        neo4j.close();
    }

    @Test
    void subsetStatsAllSamples() {
        try (var session = neo4j.defaultDatabaseService().beginTx()) {
            var result = session.execute(
                    "CALL graphmana.subsetStats(['s0', 's1', 's2', 's3'], '22', 0.0, 1.0)"
            );
            assertTrue(result.hasNext());
            var row = result.next();
            assertEquals("22:1000:A:T", row.get("variantId"));
            assertEquals("22", row.get("chr"));
            // s0=HomRef, s1=Het, s2=HomAlt, s3=Missing
            // AC = 1(het) + 2(homAlt) = 3, AN = 6 (3 called diploid)
            assertEquals(3L, row.get("ac"));
            assertEquals(6L, row.get("an"));
            assertFalse(result.hasNext());
        }
    }

    @Test
    void subsetStatsSubset() {
        try (var session = neo4j.defaultDatabaseService().beginTx()) {
            // Only samples s0 (HomRef) and s1 (Het)
            var result = session.execute(
                    "CALL graphmana.subsetStats(['s0', 's1'], '22', 0.0, 1.0)"
            );
            assertTrue(result.hasNext());
            var row = result.next();
            assertEquals(1L, row.get("ac"));  // only het
            assertEquals(4L, row.get("an"));  // 2 diploid samples
            assertEquals(1L, row.get("hetCount"));
            assertEquals(0L, row.get("homAltCount"));
        }
    }

    @Test
    void subsetStatsEmptySamples() {
        try (var session = neo4j.defaultDatabaseService().beginTx()) {
            var result = session.execute(
                    "CALL graphmana.subsetStats([], '22', 0.0, 1.0)"
            );
            assertFalse(result.hasNext());
        }
    }

    @Test
    void subsetStatsNonexistentSamples() {
        try (var session = neo4j.defaultDatabaseService().beginTx()) {
            var result = session.execute(
                    "CALL graphmana.subsetStats(['unknown1', 'unknown2'], '22', 0.0, 1.0)"
            );
            assertFalse(result.hasNext());
        }
    }

    @Test
    void subsetStatsAfFilter() {
        try (var session = neo4j.defaultDatabaseService().beginTx()) {
            // Only s0 (HomRef) and s1 (Het): AF = 0.25
            // With minAF = 0.5, should be filtered out
            var result = session.execute(
                    "CALL graphmana.subsetStats(['s0', 's1'], '22', 0.5, 1.0)"
            );
            assertFalse(result.hasNext());
        }
    }

    @Test
    void subsetStatsSingleSample() {
        try (var session = neo4j.defaultDatabaseService().beginTx()) {
            // Only s2 (HomAlt)
            var result = session.execute(
                    "CALL graphmana.subsetStats(['s2'], '22', 0.0, 1.0)"
            );
            assertTrue(result.hasNext());
            var row = result.next();
            assertEquals(2L, row.get("ac"));
            assertEquals(2L, row.get("an"));
            assertEquals(0L, row.get("hetCount"));
            assertEquals(1L, row.get("homAltCount"));
        }
    }

    @Test
    void subsetStatsWrongChromosome() {
        try (var session = neo4j.defaultDatabaseService().beginTx()) {
            var result = session.execute(
                    "CALL graphmana.subsetStats(['s0', 's1'], '1', 0.0, 1.0)"
            );
            assertFalse(result.hasNext());
        }
    }

    @Test
    void subsetStatsAllChromosomes() {
        try (var session = neo4j.defaultDatabaseService().beginTx()) {
            // Empty string for chromosome = all
            var result = session.execute(
                    "CALL graphmana.subsetStats(['s0', 's1', 's2'], '', 0.0, 1.0)"
            );
            assertTrue(result.hasNext());
            var row = result.next();
            assertEquals("22:1000:A:T", row.get("variantId"));
        }
    }
}
