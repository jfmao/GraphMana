package org.graphmana.procedures;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Server-side procedure for incremental sample extension.
 *
 * <p>Extends packed genotype arrays and population statistics on existing
 * Variant nodes entirely within the Neo4j JVM, avoiding Bolt round-trips
 * for large byte array transfers. This is 5–10× faster than the equivalent
 * Python/Cypher approach for large variant counts.</p>
 *
 * <p>Called from the Python CLI as:</p>
 * <pre>
 *   CALL graphmana.extendVariants($chromosome, $nExisting, $newGenotypes, $batchSize)
 *   CALL graphmana.extendHomRef($chromosome, $nExisting, $nNew, $newPopIds, $newPopAn, $batchSize)
 * </pre>
 */
public class IncrementalExtendProcedure {

    @Context
    public Transaction tx;

    // -----------------------------------------------------------------------
    // extendVariants — extend variants that have actual genotypes in new VCF
    // -----------------------------------------------------------------------

    /**
     * Extend packed arrays on existing Variant nodes with new sample genotypes.
     *
     * @param chromosome   chromosome to process
     * @param nExisting    number of existing samples in the packed arrays
     * @param newGenotypes list of maps, each containing:
     *                     variantId (String), gtCodes (int[]), phaseBits (int[]),
     *                     ploidyBits (int[]),
     *                     popIds (String[]), ac (int[]), an (int[]),
     *                     hetCount (int[]), homAltCount (int[])
     * @param batchSize    variants per logging checkpoint (all in one tx)
     * @return single result row with counts
     */
    @Procedure(name = "graphmana.extendVariants", mode = Mode.WRITE)
    @Description("Extend packed genotype arrays with new sample data (server-side, no Bolt transfer).")
    public Stream<ExtendResult> extendVariants(
            @Name("chromosome") String chromosome,
            @Name("nExisting") Long nExisting,
            @Name("newGenotypes") List<Map<String, Object>> newGenotypes,
            @Name(value = "batchSize", defaultValue = "2000") Long batchSize) {

        int nExist = nExisting.intValue();
        int extended = 0;
        int failed = 0;

        for (Map<String, Object> entry : newGenotypes) {
            try {
                String variantId = (String) entry.get("variantId");
                Node v = findVariant(variantId);
                if (v == null) {
                    failed++;
                    continue;
                }

                int[] gtCodes = toIntArray(entry.get("gtCodes"));
                int[] phaseBits = toIntArray(entry.get("phaseBits"));
                int[] ploidyBits = toIntArray(entry.get("ploidyBits"));
                int nNew = gtCodes.length;
                int nTotal = nExist + nNew;

                // --- Extend gt_packed ---
                byte[] oldGt = (byte[]) v.getProperty("gt_packed");
                byte[] newGt = new byte[PackedGenotypeReader.gtPackedLength(nTotal)];
                System.arraycopy(oldGt, 0, newGt, 0, oldGt.length);
                for (int i = 0; i < nNew; i++) {
                    PackedGenotypeReader.setGenotype(newGt, nExist + i, gtCodes[i]);
                }
                v.setProperty("gt_packed", newGt);

                // --- Extend phase_packed ---
                byte[] oldPhase = getByteArrayOrEmpty(v, "phase_packed", nExist);
                byte[] newPhase = new byte[PackedGenotypeReader.phasePackedLength(nTotal)];
                System.arraycopy(oldPhase, 0, newPhase, 0, oldPhase.length);
                for (int i = 0; i < nNew; i++) {
                    PackedGenotypeReader.setPhase(newPhase, nExist + i, phaseBits[i]);
                }
                v.setProperty("phase_packed", newPhase);

                // --- Extend ploidy_packed ---
                byte[] oldPloidy = getByteArrayOrEmpty(v, "ploidy_packed", nExist);
                byte[] newPloidy = new byte[PackedGenotypeReader.ploidyPackedLength(nTotal)];
                System.arraycopy(oldPloidy, 0, newPloidy, 0, oldPloidy.length);
                boolean anyHaploid = false;
                for (int i = 0; i < nNew; i++) {
                    PackedGenotypeReader.setPloidy(newPloidy, nExist + i, ploidyBits[i]);
                    if (ploidyBits[i] != 0) anyHaploid = true;
                }
                // Check if any existing sample was haploid
                if (!anyHaploid) {
                    for (int i = 0; i < oldPloidy.length; i++) {
                        if (oldPloidy[i] != 0) { anyHaploid = true; break; }
                    }
                }
                if (anyHaploid) {
                    v.setProperty("ploidy_packed", newPloidy);
                } else {
                    v.removeProperty("ploidy_packed");
                }

                // --- Merge population statistics ---
                mergePopStats(v, entry, nTotal);

                extended++;
            } catch (Exception e) {
                failed++;
            }
        }

        return Stream.of(new ExtendResult(extended, failed, 0));
    }

    // -----------------------------------------------------------------------
    // extendHomRef — extend variants absent from new VCF with HomRef
    // -----------------------------------------------------------------------

    /**
     * Extend packed arrays on variants not present in the new VCF with HomRef
     * genotypes (code 0) for all new samples.
     *
     * @param chromosome chromosome to process
     * @param nExisting  number of existing samples
     * @param nNew       number of new samples to add (all HomRef)
     * @param newPopIds  population IDs for new samples
     * @param newPopAn   allele numbers per new population (2 * n_samples_per_pop)
     * @param batchSize  variants per logging checkpoint
     * @return single result row with counts
     */
    @Procedure(name = "graphmana.extendHomRef", mode = Mode.WRITE)
    @Description("Extend variants with HomRef genotypes for new samples (server-side).")
    public Stream<ExtendResult> extendHomRef(
            @Name("chromosome") String chromosome,
            @Name("nExisting") Long nExisting,
            @Name("nNew") Long nNew,
            @Name("newPopIds") List<String> newPopIds,
            @Name("newPopAn") List<Long> newPopAn,
            @Name(value = "batchSize", defaultValue = "2000") Long batchSize) {

        int nExist = nExisting.intValue();
        int nNewInt = nNew.intValue();
        int nTotal = nExist + nNewInt;
        int extended = 0;
        int failed = 0;

        // Pre-compute new array sizes
        int newGtLen = PackedGenotypeReader.gtPackedLength(nTotal);
        int newPhaseLen = PackedGenotypeReader.phasePackedLength(nTotal);
        int newPloidyLen = PackedGenotypeReader.ploidyPackedLength(nTotal);

        // Pre-build the new pop stats for merging (all HomRef: ac=0, het=0, hom=0)
        String[] newPids = newPopIds.toArray(new String[0]);
        int[] newAc = new int[newPids.length]; // all zeros
        int[] newAn = new int[newPids.length];
        int[] newHet = new int[newPids.length]; // all zeros
        int[] newHom = new int[newPids.length]; // all zeros
        for (int i = 0; i < newPids.length; i++) {
            newAn[i] = newPopAn.get(i).intValue();
        }

        // Query all variants on this chromosome
        var result = tx.execute(
                "MATCH (v:Variant) WHERE v.chr = $chr RETURN v",
                Map.of("chr", chromosome));

        while (result.hasNext()) {
            var row = result.next();
            Node v = (Node) row.get("v");
            try {
                // Extend gt_packed (new samples are all HomRef = 0, which is the default in new byte[])
                byte[] oldGt = (byte[]) v.getProperty("gt_packed");
                byte[] newGt = new byte[newGtLen];
                System.arraycopy(oldGt, 0, newGt, 0, oldGt.length);
                // No need to set genotypes — HomRef is 0, and new byte[] is zero-initialized
                v.setProperty("gt_packed", newGt);

                // Extend phase_packed (new samples are all 0)
                byte[] oldPhase = getByteArrayOrEmpty(v, "phase_packed", nExist);
                byte[] newPhase = new byte[newPhaseLen];
                System.arraycopy(oldPhase, 0, newPhase, 0, oldPhase.length);
                v.setProperty("phase_packed", newPhase);

                // Extend ploidy_packed (new samples are all diploid = 0)
                byte[] oldPloidy = getByteArrayOrEmpty(v, "ploidy_packed", nExist);
                boolean hadHaploid = false;
                for (byte b : oldPloidy) { if (b != 0) { hadHaploid = true; break; } }
                if (hadHaploid) {
                    byte[] newPloidy = new byte[newPloidyLen];
                    System.arraycopy(oldPloidy, 0, newPloidy, 0, oldPloidy.length);
                    v.setProperty("ploidy_packed", newPloidy);
                }

                // Merge pop stats
                mergePopStatsFromArrays(v, newPids, newAc, newAn, newHet, newHom, nTotal);

                extended++;
            } catch (Exception e) {
                failed++;
            }
        }
        result.close();

        return Stream.of(new ExtendResult(extended, failed, 0));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private Node findVariant(String variantId) {
        var result = tx.execute(
                "MATCH (v:Variant {variantId: $vid}) RETURN v",
                Map.of("vid", variantId));
        if (result.hasNext()) {
            return (Node) result.next().get("v");
        }
        result.close();
        return null;
    }

    private static byte[] getByteArrayOrEmpty(Node v, String property, int nSamples) {
        if (!v.hasProperty(property)) {
            // Return zero-filled array of correct size
            if (property.equals("gt_packed")) {
                return new byte[PackedGenotypeReader.gtPackedLength(nSamples)];
            } else if (property.equals("ploidy_packed")) {
                return new byte[PackedGenotypeReader.ploidyPackedLength(nSamples)];
            } else {
                return new byte[PackedGenotypeReader.phasePackedLength(nSamples)];
            }
        }
        Object val = v.getProperty(property);
        if (val instanceof byte[]) return (byte[]) val;
        return new byte[0];
    }

    /**
     * Merge population statistics from a newGenotypes entry map onto a variant node.
     */
    private void mergePopStats(Node v, Map<String, Object> entry, int nTotal) {
        String[] newPids = toStringArray(entry.get("popIds"));
        int[] newAc = toIntArray(entry.get("ac"));
        int[] newAn = toIntArray(entry.get("an"));
        int[] newHet = toIntArray(entry.get("hetCount"));
        int[] newHom = toIntArray(entry.get("homAltCount"));

        mergePopStatsFromArrays(v, newPids, newAc, newAn, newHet, newHom, nTotal);
    }

    /**
     * Core pop stats merge: reads existing arrays from node, merges with new, writes back.
     */
    private void mergePopStatsFromArrays(
            Node v, String[] newPids, int[] newAc, int[] newAn,
            int[] newHet, int[] newHom, int nTotal) {

        // Read existing pop stats
        String[] oldPids = ArrayUtil.toStringArray(v.getProperty("pop_ids"));
        int[] oldAc = ArrayUtil.toIntArray(v.getProperty("ac"));
        int[] oldAn = ArrayUtil.toIntArray(v.getProperty("an"));
        int[] oldHet = ArrayUtil.toIntArray(v.getProperty("het_count"));
        int[] oldHom = ArrayUtil.toIntArray(v.getProperty("hom_alt_count"));

        // Build lookup maps
        Map<String, Integer> oldMap = new HashMap<>();
        for (int i = 0; i < oldPids.length; i++) oldMap.put(oldPids[i], i);
        Map<String, Integer> newMap = new HashMap<>();
        for (int i = 0; i < newPids.length; i++) newMap.put(newPids[i], i);

        // Sorted union of population IDs
        TreeSet<String> allPids = new TreeSet<>();
        Collections.addAll(allPids, oldPids);
        Collections.addAll(allPids, newPids);

        int k = allPids.size();
        String[] mergedPids = allPids.toArray(new String[0]);
        int[] mergedAc = new int[k];
        int[] mergedAn = new int[k];
        double[] mergedAf = new double[k];
        int[] mergedHet = new int[k];
        int[] mergedHom = new int[k];
        double[] mergedHetExp = new double[k];

        int acTotal = 0, anTotal = 0;

        int idx = 0;
        for (String pid : mergedPids) {
            int ac = 0, an = 0, het = 0, hom = 0;
            Integer oi = oldMap.get(pid);
            if (oi != null) {
                ac += oldAc[oi]; an += oldAn[oi]; het += oldHet[oi]; hom += oldHom[oi];
            }
            Integer ni = newMap.get(pid);
            if (ni != null) {
                ac += newAc[ni]; an += newAn[ni]; het += newHet[ni]; hom += newHom[ni];
            }
            mergedAc[idx] = ac;
            mergedAn[idx] = an;
            mergedAf[idx] = an > 0 ? (double) ac / an : 0.0;
            mergedHet[idx] = het;
            mergedHom[idx] = hom;
            mergedHetExp[idx] = 2.0 * mergedAf[idx] * (1.0 - mergedAf[idx]);
            acTotal += ac;
            anTotal += an;
            idx++;
        }

        double afTotal = anTotal > 0 ? (double) acTotal / anTotal : 0.0;
        double callRate = nTotal > 0 ? (double) anTotal / (2 * nTotal) : 0.0;

        // Write back all properties
        v.setProperty("pop_ids", mergedPids);
        v.setProperty("ac", mergedAc);
        v.setProperty("an", mergedAn);
        v.setProperty("af", mergedAf);
        v.setProperty("het_count", mergedHet);
        v.setProperty("hom_alt_count", mergedHom);
        v.setProperty("het_exp", mergedHetExp);
        v.setProperty("ac_total", (long) acTotal);
        v.setProperty("an_total", (long) anTotal);
        v.setProperty("af_total", afTotal);
        v.setProperty("call_rate", callRate);
    }

    private static int[] toIntArray(Object val) {
        if (val instanceof int[]) return (int[]) val;
        if (val instanceof long[]) {
            long[] la = (long[]) val;
            int[] result = new int[la.length];
            for (int i = 0; i < la.length; i++) result[i] = (int) la[i];
            return result;
        }
        if (val instanceof List<?>) {
            List<?> list = (List<?>) val;
            int[] result = new int[list.size()];
            for (int i = 0; i < list.size(); i++) {
                result[i] = ((Number) list.get(i)).intValue();
            }
            return result;
        }
        return new int[0];
    }

    private static String[] toStringArray(Object val) {
        if (val instanceof String[]) return (String[]) val;
        if (val instanceof List<?>) {
            List<?> list = (List<?>) val;
            String[] result = new String[list.size()];
            for (int i = 0; i < list.size(); i++) {
                result[i] = list.get(i).toString();
            }
            return result;
        }
        return new String[0];
    }

    // -----------------------------------------------------------------------
    // Result type
    // -----------------------------------------------------------------------

    public static class ExtendResult {
        public final long extended;
        public final long failed;
        public final long created;

        public ExtendResult(long extended, long failed, long created) {
            this.extended = extended;
            this.failed = failed;
            this.created = created;
        }
    }
}
