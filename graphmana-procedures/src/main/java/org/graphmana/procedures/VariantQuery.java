package org.graphmana.procedures;

import java.util.Map;

/**
 * Shared Cypher query builder for variant retrieval with optional
 * consequence and pathway filters.
 *
 * <p>When {@code consequence} or {@code pathway} options are provided, the query
 * joins through HAS_CONSEQUENCE and/or IN_PATHWAY edges. Otherwise, a simple
 * Variant-only query is returned.</p>
 */
final class VariantQuery {

    private VariantQuery() {}

    /**
     * Build a Cypher query for fetching variants in a region, with optional
     * consequence and pathway filters from the options map.
     *
     * @param options user-supplied options (may contain "consequence" and/or "pathway")
     * @return Cypher query string with parameters $chr, $start, $end
     */
    static String build(Map<String, Object> options) {
        String consequence = options != null ? (String) options.get("consequence") : null;
        String pathway = options != null ? (String) options.get("pathway") : null;
        return build(consequence, pathway);
    }

    /**
     * Build a Cypher query for fetching variants in a region.
     *
     * @param consequence VEP consequence filter (e.g., "missense_variant"), or null
     * @param pathway     Reactome pathway name filter, or null
     * @return Cypher query string with parameters $chr, $start, $end
     */
    static String build(String consequence, String pathway) {
        StringBuilder sb = new StringBuilder();

        if (pathway != null && consequence != null) {
            sb.append("MATCH (v:Variant)-[c:HAS_CONSEQUENCE]->(g:Gene)-[:IN_PATHWAY]->(p:Pathway) ");
            sb.append("WHERE v.chr = $chr AND v.pos >= $start AND v.pos <= $end ");
            sb.append("AND c.consequence = '").append(consequence.replace("'", "''")).append("' ");
            sb.append("AND p.name = '").append(pathway.replace("'", "''")).append("' ");
        } else if (pathway != null) {
            sb.append("MATCH (v:Variant)-[:HAS_CONSEQUENCE]->(g:Gene)-[:IN_PATHWAY]->(p:Pathway) ");
            sb.append("WHERE v.chr = $chr AND v.pos >= $start AND v.pos <= $end ");
            sb.append("AND p.name = '").append(pathway.replace("'", "''")).append("' ");
        } else if (consequence != null) {
            sb.append("MATCH (v:Variant)-[c:HAS_CONSEQUENCE]->(g:Gene) ");
            sb.append("WHERE v.chr = $chr AND v.pos >= $start AND v.pos <= $end ");
            sb.append("AND c.consequence = '").append(consequence.replace("'", "''")).append("' ");
        } else {
            sb.append("MATCH (v:Variant) ");
            sb.append("WHERE v.chr = $chr AND v.pos >= $start AND v.pos <= $end ");
        }

        sb.append("RETURN DISTINCT v");
        return sb.toString();
    }

    /**
     * Build a Cypher query for fetching all variants on a chromosome (no region bounds),
     * sorted by position.
     */
    static String buildChromosome(Map<String, Object> options) {
        String consequence = options != null ? (String) options.get("consequence") : null;
        String pathway = options != null ? (String) options.get("pathway") : null;

        StringBuilder sb = new StringBuilder();

        if (pathway != null && consequence != null) {
            sb.append("MATCH (v:Variant)-[c:HAS_CONSEQUENCE]->(g:Gene)-[:IN_PATHWAY]->(p:Pathway) ");
            sb.append("WHERE v.chr = $chr ");
            sb.append("AND c.consequence = '").append(consequence.replace("'", "''")).append("' ");
            sb.append("AND p.name = '").append(pathway.replace("'", "''")).append("' ");
        } else if (pathway != null) {
            sb.append("MATCH (v:Variant)-[:HAS_CONSEQUENCE]->(g:Gene)-[:IN_PATHWAY]->(p:Pathway) ");
            sb.append("WHERE v.chr = $chr ");
            sb.append("AND p.name = '").append(pathway.replace("'", "''")).append("' ");
        } else if (consequence != null) {
            sb.append("MATCH (v:Variant)-[c:HAS_CONSEQUENCE]->(g:Gene) ");
            sb.append("WHERE v.chr = $chr ");
            sb.append("AND c.consequence = '").append(consequence.replace("'", "''")).append("' ");
        } else {
            sb.append("MATCH (v:Variant) ");
            sb.append("WHERE v.chr = $chr ");
        }

        sb.append("RETURN DISTINCT v ORDER BY v.pos");
        return sb.toString();
    }
}
