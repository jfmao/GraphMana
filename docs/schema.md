# GraphMana — Graph Schema Reference

This document is the authoritative reference for the GraphMana Neo4j schema. Both GraphMana and GraphPop read from this same schema. Any changes here must be verified against GraphPop at `/mnt/e/GraphPop`.

---

## Node Types

### Variant

One node per biallelic variant site. Multi-allelic sites are split during import.

```cypher
(:Variant {
    // === Identity ===
    variantId: STRING,              // "chr22:16050075:A:G" — PRIMARY KEY, UNIQUE CONSTRAINT
    chr: STRING,                    // Chromosome (normalized per --chr-style)
    pos: LONG,                      // 1-based position
    ref: STRING,                    // Reference allele
    alt: STRING,                    // Alternate allele (single, after multi-allelic split)
    variant_type: STRING,           // "SNP" | "INDEL" | "MNP" | "SV"

    // === Multi-allelic Tracking ===
    multiallelic_site: STRING,      // "chr22:16050075" if split from multi-allelic; null if native biallelic
    allele_index: INT,              // 1, 2, 3... for split alleles; null if native biallelic

    // === Population Allele Count Arrays ===
    // Parallel arrays of length K (one element per population, same index order)
    pop_ids: [STRING],              // ["AFR", "EUR", "EAS", "SAS", "AMR"]
    ac: [INT],                      // Allele count per population
    an: [INT],                      // Allele number per population (2 * called samples)
    af: [FLOAT],                    // Allele frequency per population (ac/an)
    het_count: [INT],               // Heterozygote count per population
    hom_alt_count: [INT],           // Homozygous-alt count per population
    het_exp: [FLOAT],               // Expected heterozygosity: 2*p*(1-p) per population

    // Global summaries (across all populations)
    ac_total: INT,
    an_total: INT,
    af_total: FLOAT,
    call_rate: FLOAT,               // Fraction of non-missing genotypes across all samples

    // === Packed Genotype Arrays ===
    gt_packed: byte[],              // 2 bits/sample, 4 samples/byte, LSB-first
                                    // Encoding: 00=HomRef, 01=Het, 10=HomAlt, 11=Missing
    phase_packed: byte[],           // 1 bit/sample, 8 samples/byte, LSB-first
                                    // Which haplotype carries ALT for Het sites
    ploidy_packed: byte[],          // 1 bit/sample, 8 samples/byte
                                    // Bit=1 means haploid. NULL = all diploid (autosome default)

    // === Ancestral State ===
    ancestral_allele: STRING,       // From ancestral allele FASTA or AA INFO tag
    is_polarized: BOOLEAN,          // true if ancestral allele is known and matches ref or alt

    // === Site-Level Quality (from VCF INFO/QUAL) ===
    qual: FLOAT,                    // VCF QUAL field
    filter: STRING,                 // VCF FILTER field ("PASS", ".", or filter names)

    // === Functional Annotation Summary ===
    // Most severe consequence across all transcripts (convenience fields)
    consequence: STRING,            // e.g., "missense_variant"
    impact: STRING,                 // "HIGH" | "MODERATE" | "LOW" | "MODIFIER"
    gene_symbol: STRING,            // Gene with most severe consequence

    // === Raw Fields for Lossless Roundtrip ===
    info_raw: STRING,               // Complete VCF INFO field as string
    csq_raw: STRING,                // Complete VEP CSQ field as string

    // === Internally Computed (Tier 1) ===
    population_specificity: FLOAT,  // How population-restricted this variant is

    // === Liftover Tracking ===
    liftover_status: STRING,        // "mapped" | "unmapped" | "ambiguous" | null (native coords)
    original_variantId: STRING,     // Pre-liftover variantId for provenance
})
```

**Notes:**
- `variantId` format is always `chr:pos:ref:alt` (e.g., "chr22:16050075:A:G")
- Population arrays are parallel: `pop_ids[i]`, `ac[i]`, `an[i]`, `af[i]` all refer to the same population
- `gt_packed` size = ceil(N_samples / 4) bytes
- `phase_packed` size = ceil(N_samples / 8) bytes
- `ploidy_packed` is NULL for autosomes (zero storage overhead)

### Sample

One node per individual in the panel.

```cypher
(:Sample {
    sampleId: STRING,               // UNIQUE CONSTRAINT — matches VCF sample column name
    population: STRING,             // Population assignment from panel file
    packed_index: INT,              // CRITICAL: position in packed genotype arrays (0-based)
    sex: STRING,                    // "male" | "female" | "unknown"
    source_dataset: STRING,         // Dataset identifier for provenance
    source_file: STRING,            // VCF filename this sample was imported from
    ingestion_date: DATETIME,       // When this sample was added

    phenotypes: MAP,                // Arbitrary key-value phenotype data

    // Soft delete
    excluded: BOOLEAN,              // true = soft-deleted (default: false)
    exclusion_reason: STRING,       // "qc_fail" | "consent_withdrawn" | "duplicate" | etc.

    // Computed annotations (Tier 1)
    n_het: INT,                     // Number of heterozygous sites
    n_hom_alt: INT,                 // Number of homozygous-alt sites
    heterozygosity: FLOAT,          // n_het / total_called
    call_rate: FLOAT,               // Fraction of non-missing genotypes
    rare_variant_burden: INT,       // Count of variants with af < 0.01
})
```

**Notes:**
- `packed_index` is assigned sequentially during import (0, 1, 2, ...)
- Incremental imports continue from max(packed_index) + 1
- packed_index NEVER changes after assignment (even after soft delete)
- Only hard delete reassigns packed_index (expensive rebuild)

### Population

```cypher
(:Population {
    populationId: STRING,           // UNIQUE CONSTRAINT
    name: STRING,                   // Human-readable name
    n_samples: INT,                 // Count of non-excluded samples
    a_n: FLOAT,                     // Harmonic number: sum(1/i for i in 1..n-1)
    a_n2: FLOAT,                    // Sum of 1/i^2 for i in 1..n-1
})
```

### Chromosome

```cypher
(:Chromosome {
    chromosomeId: STRING,           // UNIQUE CONSTRAINT — canonical name
    length: LONG,                   // From VCF ##contig header
    n_variants: LONG,               // Count of Variant nodes on this chromosome
    aliases: [STRING],              // Alternative names: ["22", "chr22", "CM000684.2"]
})
```

### Gene

```cypher
(:Gene {
    geneId: STRING,                 // UNIQUE CONSTRAINT — Ensembl gene ID or equivalent
    symbol: STRING,                 // Gene symbol (e.g., "BRCA1")
    chr: STRING,
    start: LONG,
    end: LONG,
    strand: STRING,                 // "+" or "-"
    biotype: STRING,                // "protein_coding", "lncRNA", etc.
})
```

### Annotation & Metadata Nodes

```cypher
(:Pathway { id: STRING, name: STRING, source: STRING })
(:GOTerm { id: STRING, name: STRING, namespace: STRING })
(:RegulatoryElement { id: STRING, type: STRING, chr: STRING, start: LONG, end: LONG })

(:VCFHeader {
    dataset_id: STRING,
    source_file: STRING,
    header_text: STRING,            // Complete VCF header verbatim
    file_format: STRING,            // e.g., "VCFv4.2"
    reference: STRING,              // e.g., "GRCh38"
    caller: STRING,                 // e.g., "GATK HaplotypeCaller 4.4"
    import_date: DATETIME,
    info_fields: [STRING],
    format_fields: [STRING],
    filter_fields: [STRING],
    sample_fields_stored: [STRING], // Which FORMAT fields are stored: ["GT","DP","GQ"]
})

(:CohortDefinition {
    name: STRING,                   // UNIQUE CONSTRAINT
    cypher_query: STRING,           // The defining Cypher query
    created_date: DATETIME,
    description: STRING,
})

(:IngestionLog {
    log_id: STRING,
    source_file: STRING,
    dataset_id: STRING,
    mode: STRING,                   // "initial" | "incremental" | "liftover" | "annotation"
    import_date: DATETIME,
    n_samples: INT,
    n_variants: LONG,
    filters_applied: STRING,        // JSON of filter settings used
    fidelity: STRING,               // "minimal" | "default" | "full"
    reference_genome: STRING,
})

(:AnnotationVersion {
    version_id: STRING,             // "VEP_v112", "ClinVar_2025-03"
    source: STRING,                 // "VEP", "ClinVar", "CADD", "GO"
    version: STRING,                // "v112", "2025-03"
    loaded_date: DATETIME,
    n_annotations: LONG,
    description: STRING,
})

(:SchemaMetadata {
    schema_version: STRING,         // "0.5.0"
    graphmana_version: STRING,      // "0.5.0"
    reference_genome: STRING,       // "GRCh38"
    created_date: DATETIME,
    last_modified: DATETIME,
    n_samples: INT,
    n_variants: LONG,
    n_populations: INT,
    chr_naming_style: STRING,       // "ucsc" | "ensembl" | "original"
})
```

---

## Relationship Types

### NEXT (Variant → Variant)

```cypher
(:Variant)-[:NEXT {distance_bp: LONG}]->(:Variant)
```

Linked list of variants along each chromosome, sorted by position. Built during streaming import (assumes position-sorted VCF). Enables sliding-window traversal and distance-aware computation.

### ON_CHROMOSOME (Variant → Chromosome)

```cypher
(:Variant)-[:ON_CHROMOSOME]->(:Chromosome)
```

### IN_POPULATION (Sample → Population)

```cypher
(:Sample)-[:IN_POPULATION]->(:Population)
```

### HAS_CONSEQUENCE (Variant → Gene)

```cypher
(:Variant)-[:HAS_CONSEQUENCE {
    type: STRING,               // VEP consequence type (e.g., "missense_variant")
    impact: STRING,             // "HIGH" | "MODERATE" | "LOW" | "MODIFIER"
    transcript_id: STRING,      // Ensembl transcript ID
    protein_change: STRING,     // e.g., "p.Val600Glu"
    codon_change: STRING,       // e.g., "gTg/gAg"
    sift: STRING,               // SIFT prediction + score
    polyphen: STRING,           // PolyPhen prediction + score
    annotation_source: STRING,  // "VEP" | "SnpEff"
    annotation_version: STRING, // "v112" — for version tracking
}]->(:Gene)
```

Multiple edges per variant (one per transcript). Multiple versions can coexist (different annotation_version values).

### Gene Ontology & Pathway Relationships

```cypher
(:Gene)-[:IN_PATHWAY]->(:Pathway)
(:Gene)-[:HAS_GO_TERM]->(:GOTerm)
(:GOTerm)-[:IS_A]->(:GOTerm)       // GO term hierarchy
```

---

## What Is NOT in the Schema

**There is NO `(:Sample)-[:CARRIES]->(:Variant)` relationship.** Genotype data is stored as packed byte arrays on Variant nodes. See CLAUDE.md "Why Not CARRIES Edges" for rationale (125× storage reduction).

---

## Constraints and Indexes

```cypher
// Uniqueness constraints (also create indexes automatically)
CREATE CONSTRAINT variant_id IF NOT EXISTS FOR (v:Variant) REQUIRE v.variantId IS UNIQUE;
CREATE CONSTRAINT sample_id IF NOT EXISTS FOR (s:Sample) REQUIRE s.sampleId IS UNIQUE;
CREATE CONSTRAINT population_id IF NOT EXISTS FOR (p:Population) REQUIRE p.populationId IS UNIQUE;
CREATE CONSTRAINT gene_id IF NOT EXISTS FOR (g:Gene) REQUIRE g.geneId IS UNIQUE;
CREATE CONSTRAINT chromosome_id IF NOT EXISTS FOR (c:Chromosome) REQUIRE c.chromosomeId IS UNIQUE;

// Composite and property indexes
CREATE INDEX variant_pos IF NOT EXISTS FOR (v:Variant) ON (v.chr, v.pos);
CREATE INDEX variant_type IF NOT EXISTS FOR (v:Variant) ON (v.variant_type);
CREATE INDEX gene_symbol IF NOT EXISTS FOR (g:Gene) ON (g.symbol);
CREATE INDEX sample_pop IF NOT EXISTS FOR (s:Sample) ON (s.population);
CREATE INDEX sample_excluded IF NOT EXISTS FOR (s:Sample) ON (s.excluded);
```

---

## Neo4j Bulk Import CSV Formats

Array delimiter: semicolon (`;`). Packed byte arrays serialized as signed Java bytes (-128 to 127).

### variant_nodes.csv
```
variantId:ID,chr,pos:LONG,ref,alt,variant_type,ac:INT[],an:INT[],af:FLOAT[],het_count:INT[],hom_alt_count:INT[],het_exp:FLOAT[],ac_total:INT,an_total:INT,af_total:FLOAT,call_rate:FLOAT,gt_packed:byte[],phase_packed:byte[],ancestral_allele,is_polarized:BOOLEAN,qual:FLOAT,filter,:LABEL
chr22:16050075:A:G,chr22,16050075,A,G,SNP,10;20;5,100;200;50,0.1;0.1;0.1,8;15;4,1;2;0,0.18;0.18;0.18,35,350,0.1,0.95,43;-127,...,...,A,true,100.0,PASS,Variant
```

### sample_nodes.csv
```
sampleId:ID,population,packed_index:INT,sex,:LABEL
NA12878,CEU,0,female,Sample
```

### population_nodes.csv
```
populationId:ID,name,n_samples:INT,a_n:FLOAT,a_n2:FLOAT,:LABEL
CEU,Central European,99,5.17,1.49,Population
```

### chromosome_nodes.csv
```
chromosomeId:ID,length:LONG,:LABEL
chr22,50818468,Chromosome
```

### next_edges.csv
```
:START_ID,:END_ID,distance_bp:LONG,:TYPE
chr22:16050075:A:G,chr22:16050115:C:T,40,NEXT
```

### on_chromosome_edges.csv
```
:START_ID,:END_ID,:TYPE
chr22:16050075:A:G,chr22,ON_CHROMOSOME
```

### in_population_edges.csv
```
:START_ID,:END_ID,:TYPE
NA12878,CEU,IN_POPULATION
```

### gene_nodes.csv (optional, from VEP)
```
geneId:ID,symbol,:LABEL
ENSG00000183914,MAPK8IP2,Gene
```

### has_consequence_edges.csv (optional, from VEP)
```
:START_ID,:END_ID,type,impact,transcript_id,annotation_source,annotation_version,:TYPE
chr22:16050075:A:G,ENSG00000183914,missense_variant,MODERATE,ENST00000398822,VEP,v112,HAS_CONSEQUENCE
```
