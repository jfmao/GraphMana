# graphmana list-formats

List all available export formats with their access path (FAST or FULL) and
a brief description.

This is a local command — it does not connect to Neo4j.

## Usage

```
graphmana list-formats
```

## Options

No options.

## Output

```
Format         Path   Description
------         ----   -----------
treemix        FAST   TreeMix allele count matrix (gzipped)
sfs-dadi       FAST   dadi site frequency spectrum (.fs)
sfs-fsc        FAST   fastsimcoal2 SFS (.obs)
bed            FAST   BED variant positions for bedtools/IGV
tsv            FAST   Tab-separated variant table
json           FAST   JSON Lines variant records
vcf            FULL   VCF/BCF genotype calls
plink          FULL   PLINK 1.9 binary (.bed/.bim/.fam)
plink2         FULL   PLINK 2.0 binary (.pgen/.pvar/.psam)
eigenstrat     FULL   EIGENSTRAT for smartPCA/AdmixTools (.geno/.snp/.ind)
beagle         FULL   Beagle phasing/imputation input
structure      FULL   STRUCTURE population assignment
genepop        FULL   Genepop conservation genetics format
hap            FULL   Haplotype for selscan (.hap/.map)
bgen           FULL   BGEN probabilistic genotypes
gds            FULL   SeqArray/R HDF5-based format
zarr           FULL   Zarr chunked arrays for sgkit/Python

Total: 17 formats

FAST PATH: reads pre-computed population arrays (constant time in N samples)
FULL PATH: unpacks per-sample genotypes (linear time in N samples)
```
