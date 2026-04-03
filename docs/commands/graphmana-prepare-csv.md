# graphmana prepare-csv

## Synopsis

```
Usage: graphmana prepare-csv [OPTIONS]
```

## Help Output

```
Usage: graphmana prepare-csv [OPTIONS]

  Generate CSV files from VCF (no Neo4j needed).

Options:
  --input FILE                    Input VCF/BCF file(s).  [required]
  --input-list FILE               File listing input VCF paths (one per line).
  --population-map FILE           Population panel/PED file.  [required]
  --output-dir DIRECTORY          Output directory for CSV files.  [required]
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
  --include-filtered              Include variants with FILTER != PASS
                                  (default: exclude).
  --verbose / --quiet             Verbose logging.
  --dry-run                       Show what would be done without executing.
  --normalize                     Run bcftools norm before parsing.
  --reference-fasta FILE          Reference FASTA for normalization (required
                                  with --normalize).
  --help                          Show this message and exit.
```
