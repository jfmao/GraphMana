# graphmana ingest

## Synopsis

```
Usage: graphmana ingest [OPTIONS]
```

## Help Output

```
Usage: graphmana ingest [OPTIONS]

  Import VCF data: generate CSVs and load into Neo4j.

Options:
  --input FILE                    Input VCF/BCF file(s).  [required]
  --input-list FILE               File listing input VCF paths (one per line).
  --population-map FILE           Population panel/PED file.  [required]
  --mode [auto|initial|incremental]
                                  Import mode.
  --output-csv-dir DIRECTORY      Keep CSVs in this directory (otherwise temp
                                  dir).
  --neo4j-home DIRECTORY          Neo4j installation directory (required for
                                  initial mode).
  --neo4j-data-dir DIRECTORY      Neo4j data directory.
  --database TEXT                 Neo4j database name.
  --auto-start-neo4j              Auto start/stop Neo4j.
  --neo4j-uri TEXT                Neo4j Bolt URI.
  --neo4j-user TEXT               Neo4j username.
  --neo4j-password TEXT           Neo4j password.
  --stratify-by [population|superpopulation]
                                  Population stratification level.
                                  [default: superpopulation]
  --reference TEXT                Reference genome identifier.
  --ancestral-fasta FILE          Ancestral allele FASTA for polarization.
  --chr-style [auto|ucsc|ensembl|original]
                                  Chromosome naming style.
  --chr-map FILE                  Custom chromosome name mapping file.
  --batch-size INTEGER            Variants per chunk.
  --threads INTEGER               Number of threads.
  --filter-min-qual FLOAT         Minimum QUAL threshold.
  --filter-min-call-rate FLOAT    Minimum call rate threshold.
  --filter-maf-min FLOAT          Minimum minor allele frequency.
  --filter-maf-max FLOAT          Maximum minor allele frequency.
  --filter-variant-type [SNP|INDEL|SV]
                                  Include only these variant types.
  --filter-region TEXT            Genomic region to import (e.g.
                                  'chr1:1000-2000').
  --filter-contigs TEXT           Import only these chromosomes/contigs
                                  (repeatable).
  --vep-vcf FILE                  VEP/SnpEff annotated VCF for consequence
                                  annotation.
  --annotation-version TEXT       Annotation version label.
  --dataset-id TEXT               Dataset identifier.
  --on-duplicate [error|skip]     Action when duplicate samples found
                                  (incremental mode).
  --assume-homref-on-missing      Legacy: pad absent samples as HomRef rather
                                  than Missing. Use only with fixed-site-list
                                  workflows (imputed panels, arrays). Default
                                  preserves the HomRef-vs-Missing distinction
                                  via called_packed. See docs/gvcf-workflow.md.
  --include-filtered              Include variants with FILTER != PASS
                                  (default: exclude).
  --verbose / --quiet             Verbose logging.
  --dry-run                       Show what would be done without executing.
  --normalize                     Run bcftools norm before parsing.
  --reference-fasta FILE          Reference FASTA for normalization (required
                                  with --normalize).
  --help                          Show this message and exit.
```
