package org.graphmana.procedures;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HealthCheckProcedureTest {

    private static Neo4j neo4j;
    private static Driver driver;

    @BeforeAll
    static void setUp() {
        neo4j = Neo4jBuilders.newInProcessBuilder()
                .withProcedure(HealthCheckProcedure.class)
                .build();
        driver = GraphDatabase.driver(neo4j.boltURI());
    }

    @AfterAll
    static void tearDown() {
        driver.close();
        neo4j.close();
    }

    @Test
    void healthCheckReturnsAlive() {
        try (Session session = driver.session()) {
            Record record = session.run("CALL graphmana.healthCheck()").single();
            assertEquals("GraphMana is alive", record.get("message").asString());
            assertEquals("0.1.0", record.get("version").asString());
        }
    }
}
