# graphmana export

## Synopsis

```
Usage: graphmana export [OPTIONS]
```

## Help Output

```
Usage: graphmana export [OPTIONS]

  Export data from Neo4j to various formats.

Options:
  --output PATH                   Output file path (or stem for PLINK).
                                  [required]
  --format [vcf|plink|plink2|eigenstrat|treemix|sfs-dadi|sfs-fsc|bed|tsv|beagle|structure|genepop|hap|json|zarr|gds|bgen]
                                  Export format.  [required]
  --neo4j-uri TEXT                Neo4j Bolt URI.
  --neo4j-user TEXT               Neo4j username.
  --neo4j-password TEXT           Neo4j password.
  --database TEXT                 Neo4j database name.
  --populations TEXT              Export only these populations.
  --chromosomes TEXT              Export only these chromosomes.
  --region TEXT                   Genomic region (e.g. 'chr1:1000-2000').
  --filter-variant-type [SNP|INDEL|SV]
                                  Include only these variant types.
  --filter-maf-min FLOAT          Minimum minor allele frequency.
  --filter-maf-max FLOAT          Maximum minor allele frequency.
  --filter-min-call-rate FLOAT    Minimum call rate.
  --filter-consequence TEXT       Include variants with these consequence
                                  types (e.g. missense_variant).
  --filter-impact [HIGH|MODERATE|LOW|MODIFIER]
                                  Include variants with these impact levels.
  --filter-gene TEXT              Include variants annotated to these genes
                                  (symbol or Ensembl ID).
  --filter-cadd-min FLOAT         Minimum CADD phred score.
  --filter-cadd-max FLOAT         Maximum CADD phred score.
  --filter-annotation-version TEXT
                                  Include variants with annotations from this
                                  version.
  --filter-sv-type [DEL|DUP|INV|INS|BND|CNV]
                                  Include only structural variants of these
                                  types.
  --filter-liftover-status [mapped|unmapped|collision]
                                  Include only variants with this liftover
                                  status.
  --recalculate-af / --no-recalculate-af
                                  Recalculate allele frequencies after
                                  population filtering. Default: True when
                                  --populations is set, False otherwise.
  --sfs-include-monomorphic       Include monomorphic sites in SFS (default:
                                  exclude).
  --vcf-version [4.1|4.2|4.3]     VCF format version header. Default: 4.3.
  --output-type [v|z|b]           VCF output type: v=VCF, z=gzipped VCF,
                                  b=BCF. Default: auto-detect from extension.
  --phased                        Output phased genotypes (VCF only).
  --reconstruct-multiallelic / --no-reconstruct-multiallelic
                                  Reconstruct multi-allelic VCF lines from
                                  split variants (VCF only).
  --tsv-columns TEXT              Columns to include (TSV only).
  --sfs-populations TEXT          Populations for SFS (required for sfs-
                                  dadi/sfs-fsc).
  --sfs-projection INTEGER        Projection sizes per population (required
                                  for sfs-dadi/sfs-fsc).
  --sfs-polarized / --sfs-folded  Polarized (unfolded) or folded SFS. Default:
                                  polarized.
  --bed-extra-columns TEXT        Extra columns for BED format (e.g.
                                  variant_type, af_total).
  --structure-format [onerow|tworow]
                                  STRUCTURE output format (onerow or tworow).
                                  Default: onerow.
  --json-fields TEXT              Fields to include in JSON output.
  --json-pretty                   Pretty-print JSON output.
  --json-include-genotypes        Include per-sample genotypes in JSON output.
                                  Requires unpacking packed arrays (FULL PATH).
  --zarr-chunk-size INTEGER       Number of variants per chunk in Zarr output.
                                  Default: 10000.
  --filter-cohort TEXT            Filter samples by named cohort.
  --filter-sample-list FILE       File with sample IDs to include (one per
                                  line).
  --threads INTEGER               Number of threads.
  --auto-start-neo4j              Auto start/stop Neo4j around export.
  --neo4j-home DIRECTORY          Neo4j installation directory (required with
                                  --auto-start-neo4j).
  --neo4j-data-dir DIRECTORY      Neo4j data directory.
  --verbose / --quiet             Verbose logging.
  --no-manifest                   Skip writing the .manifest.json sidecar file.
  --help                          Show this message and exit.
```
