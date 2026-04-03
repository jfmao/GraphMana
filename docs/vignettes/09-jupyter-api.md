# Vignette 09: Python and Jupyter Interactive Analysis

This vignette shows how to use the `graphmana-py` package for interactive exploration of a GraphMana database from Python or Jupyter notebooks. The Python API returns pandas DataFrames and integrates with the standard scientific Python stack.

## Prerequisites

- A running GraphMana database with data imported
- Python 3.11+ with `graphmana-py` installed:

  ```bash
  pip install graphmana-py
  ```

- Jupyter (optional, for notebook use):

  ```bash
  pip install jupyterlab
  ```

The Python API reads directly from the same Neo4j database that the CLI uses. No data conversion or export is needed.

## Connecting to the Database

```python
from graphmana_py import GraphManaClient

# Default connection: bolt://localhost:7687, user=neo4j, password=graphmana
client = GraphManaClient()
client.connect()

# Or with explicit parameters
client = GraphManaClient(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="graphmana",
)
client.connect()
```

The client also works as a context manager:

```python
with GraphManaClient() as client:
    info = client.status()
    print(info)
```

## Database Overview

```python
with GraphManaClient() as client:
    info = client.status()
    print(f"Variants:    {info['counts']['Variant']:,}")
    print(f"Samples:     {info['counts']['Sample']:,}")
    print(f"Populations: {info['counts']['Population']}")
    print(f"Reference:   {info['schema'].get('reference_genome', 'N/A')}")
```

Expected output:

```
Variants:    1,072,533
Samples:     3,202
Populations: 26
Reference:   GRCh38
```

## Querying Samples

```python
with GraphManaClient() as client:
    samples_df = client.samples()
    print(samples_df.head())
```

Expected output:

```
  sampleId population  packed_index   sex     source_file
0  HG00096        GBR             0  None  chr22.vcf.gz
1  HG00097        GBR             1  None  chr22.vcf.gz
2  HG00099        GBR             2  None  chr22.vcf.gz
3  HG00100        GBR             3  None  chr22.vcf.gz
4  HG00101        GBR             4  None  chr22.vcf.gz
```

Count samples per population:

```python
with GraphManaClient() as client:
    samples_df = client.samples()
    pop_counts = samples_df["population"].value_counts()
    print(pop_counts.head(10))
```

Expected output:

```
population
CEU    99
GBR    91
FIN    99
TSI    107
IBS    107
YRI    108
LWK    99
GWD    113
MSL    85
ESN    99
Name: count, dtype: int64
```

To include soft-deleted samples:

```python
with GraphManaClient() as client:
    all_samples = client.samples(include_excluded=True)
    excluded = all_samples[all_samples.get("excluded", False) == True]
    print(f"Excluded samples: {len(excluded)}")
```

## Querying Populations

```python
with GraphManaClient() as client:
    pops_df = client.populations()
    print(pops_df[["populationId", "n_samples"]].to_string(index=False))
```

Expected output:

```
populationId  n_samples
         ACB        96
         ASW         61
         BEB         86
         CDX         93
         CEU         99
         ...        ...
```

## Querying Variants by Region

```python
with GraphManaClient() as client:
    # All variants on chr22
    variants_df = client.variants(chr="22")
    print(f"Total variants on chr22: {len(variants_df):,}")

    # A specific region
    region_df = client.variants(chr="22", start=16000000, end=17000000)
    print(f"Variants in 16-17 Mb: {len(region_df):,}")
    print(region_df[["variantId", "pos", "ref", "alt", "af_total"]].head())
```

Expected output:

```
Total variants on chr22: 1,072,533
Variants in 16-17 Mb: 12,847

                  variantId      pos ref alt  af_total
0  22-16050075-A-G           16050075   A   G    0.0891
1  22-16050115-C-T           16050115   C   T    0.0044
2  22-16050213-C-T           16050213   C   T    0.0706
3  22-16050319-C-T           16050319   C   T    0.0003
4  22-16050527-C-A           16050527   C   A    0.0009
```

## Allele Frequencies by Population (FAST PATH)

The `allele_frequencies()` method reads pre-computed population arrays without unpacking genotypes. It completes in seconds regardless of sample count.

```python
with GraphManaClient() as client:
    af_df = client.allele_frequencies(chr="22", start=16000000, end=17000000)
    print(af_df.columns.tolist()[:10])
    print(af_df[["variantId", "pos", "af_CEU", "af_YRI", "af_CHB"]].head())
```

Expected output:

```
['variantId', 'pos', 'ac_ACB', 'an_ACB', 'af_ACB', 'ac_ASW', 'an_ASW', 'af_ASW', 'ac_BEB', 'an_BEB']

                  variantId      pos    af_CEU    af_YRI    af_CHB
0  22-16050075-A-G           16050075  0.121212  0.032407  0.067961
1  22-16050115-C-T           16050115  0.005051  0.000000  0.004854
2  22-16050213-C-T           16050213  0.090909  0.078704  0.048544
3  22-16050319-C-T           16050319  0.000000  0.000000  0.000000
4  22-16050527-C-A           16050527  0.000000  0.004630  0.000000
```

Identify population-specific variants:

```python
with GraphManaClient() as client:
    af_df = client.allele_frequencies(chr="22")

    # Variants common in YRI but rare elsewhere
    yri_specific = af_df[
        (af_df["af_YRI"] > 0.10) &
        (af_df["af_CEU"] < 0.01) &
        (af_df["af_CHB"] < 0.01)
    ]
    print(f"YRI-specific variants (AF>10%, <1% in CEU/CHB): {len(yri_specific)}")
    print(yri_specific[["variantId", "af_YRI", "af_CEU", "af_CHB"]].head())
```

## Unpacking Genotypes (FULL PATH)

The `genotype_matrix()` method unpacks the gt_packed byte arrays into a samples-by-variants integer matrix. This is a FULL PATH operation -- memory usage scales with n_samples * n_variants.

```python
with GraphManaClient() as client:
    # Small region to keep memory manageable
    gt_matrix = client.genotype_matrix(chr="22", start=16000000, end=16100000)
    print(f"Shape: {gt_matrix.shape}")
    print(f"Columns (variants): {gt_matrix.columns[:5].tolist()}")
    print(f"Index (samples): {gt_matrix.index[:5].tolist()}")
    print()
    print(gt_matrix.iloc[:5, :5])
```

Expected output:

```
Shape: (3202, 1287)
Columns (variants): ['22-16050075-A-G', '22-16050115-C-T', '22-16050213-C-T', '22-16050319-C-T', '22-16050527-C-A']
Index (samples): ['HG00096', 'HG00097', 'HG00099', 'HG00100', 'HG00101']

         22-16050075-A-G  22-16050115-C-T  22-16050213-C-T  22-16050319-C-T  22-16050527-C-A
HG00096                0                0                0                0                0
HG00097                1                0                0                0                0
HG00099                0                0                1                0                0
HG00100                0                0                0                0                0
HG00101                0                0                0                0                0
```

Genotype codes: 0=HomRef, 1=Het, 2=HomAlt, 3=Missing.

### Computing Sample-Level Statistics

```python
with GraphManaClient() as client:
    gt = client.genotype_matrix(chr="22", start=16000000, end=17000000)
    samples_df = client.samples()

    # Per-sample heterozygosity
    het_rate = (gt == 1).sum(axis=1) / (gt != 3).sum(axis=1)
    het_df = het_rate.reset_index()
    het_df.columns = ["sampleId", "het_rate"]

    # Merge with population info
    merged = het_df.merge(samples_df[["sampleId", "population"]], on="sampleId")
    pop_het = merged.groupby("population")["het_rate"].mean()
    print(pop_het.sort_values(ascending=False).head(10))
```

Expected output:

```
population
YRI    0.000847
LWK    0.000831
GWD    0.000828
ESN    0.000825
MSL    0.000821
ACB    0.000798
ASW    0.000776
CLM    0.000712
PUR    0.000706
MXL    0.000695
Name: het_rate, dtype: float64
```

## Querying Gene-Associated Variants

If the database has functional annotations loaded:

```python
with GraphManaClient() as client:
    brca2_df = client.gene_variants("BRCA2")
    print(f"Variants in BRCA2: {len(brca2_df)}")
    print(brca2_df[["variantId", "consequence", "impact", "af_total"]].head())
```

## Listing Cohorts and Annotations

```python
with GraphManaClient() as client:
    # List defined cohorts
    cohorts = client.cohorts()
    print(cohorts)

    # List annotation versions
    annot = client.annotation_versions()
    print(annot)
```

## Integration with Matplotlib

```python
import matplotlib.pyplot as plt

with GraphManaClient() as client:
    af_df = client.allele_frequencies(chr="22")

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(af_df["af_CEU"], af_df["af_YRI"], alpha=0.1, s=1)
    ax.set_xlabel("Allele Frequency (CEU)")
    ax.set_ylabel("Allele Frequency (YRI)")
    ax.set_title("CEU vs YRI Allele Frequencies (chr22)")
    ax.plot([0, 1], [0, 1], "r--", alpha=0.5)
    plt.tight_layout()
    plt.savefig("ceu_vs_yri_af.png", dpi=150)
    plt.show()
```

## Memory Considerations

The genotype matrix is an in-memory NumPy array. For large regions or whole chromosomes, memory usage can be substantial:

| Region size | 3K samples | 10K samples | 50K samples |
|------------|-----------|------------|------------|
| 1,000 variants | 3 MB | 10 MB | 50 MB |
| 100,000 variants | 300 MB | 1 GB | 5 GB |
| 1M variants (full chr22) | 3 GB | 10 GB | 50 GB |

For large-scale analysis, prefer the FAST PATH methods (`allele_frequencies()`) which use pre-computed population arrays and require negligible memory.

## Using Environment Variables

The password can be set via environment variable to avoid hardcoding:

```bash
export GRAPHMANA_NEO4J_PASSWORD=graphmana
```

```python
# Password is picked up automatically from the environment
client = GraphManaClient()
```

## See Also

- [Tutorial](../tutorial.md) -- CLI-based quickstart
- [API Reference](../api-reference.md) -- Full Python API documentation
- [Vignette 06: Sample Management](06-sample-lifecycle.md) -- Managing samples via CLI
- [Vignette 10: Database Administration](10-database-admin.md) -- QC and provenance
