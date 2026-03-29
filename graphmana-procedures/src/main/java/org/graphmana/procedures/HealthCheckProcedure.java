package org.graphmana.procedures;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

/**
 * Simple health-check procedure to verify GraphMana plugin is loaded.
 *
 * <p>Usage: {@code CALL graphmana.healthCheck()}</p>
 */
public class HealthCheckProcedure {

    public static class HealthResult {
        public String message;
        public String version;

        public HealthResult(String message, String version) {
            this.message = message;
            this.version = version;
        }
    }

    @Procedure(name = "graphmana.healthCheck", mode = Mode.READ)
    @Description("Returns GraphMana version and status")
    public Stream<HealthResult> healthCheck() {
        return Stream.of(new HealthResult("GraphMana is alive", "0.1.0"));
    }
}
