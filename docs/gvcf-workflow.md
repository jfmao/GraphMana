# gVCF Workflow: From joint calling to GraphMana

**Audience.** Users with per-sample gVCFs (or a GenomicsDB store) who want to ingest the resulting cohort into GraphMana for query, analysis, and multi-format export, while preserving the HomRef-vs-Missing distinction across incremental batches.

**Scope.** GraphMana does not parse raw per-sample gVCF reference blocks directly. The upstream joint-calling step is where the HomRef-vs-Missing distinction is resolved; GraphMana ingests the joint-called cohort VCF and preserves the distinction downstream via `called_packed` (schema v1.1). See `paper/v4/supplementary.tex` Supplementary Note 5 for the full rationale.

---

## The problem this workflow solves

Plain VCFs record sites where at least one sample varies. A position absent from a VCF is ambiguous: it could mean "everyone is HomRef" or "nobody was interrogated." Within a single jointly-called cohort the ambiguity is harmless. Across VCFs from independent calling runs, it is catastrophic:

- Allele frequencies become biased at batch-specific sites.
- The site frequency spectrum skews toward the low end.
- PCA and admixture pick up "which batch you came from" as a leading axis.

The fix lives upstream, in the joint caller, and downstream, in GraphMana's `called_packed` mask.

---

## Recommended end-to-end pipeline

```
per-sample gVCFs   →   GenomicsDB / GLnexus   →   joint-called VCF   →   graphmana ingest
(HaplotypeCaller)      (sparse store)              (cohort VCF)          (graph database)
```

Each box does one job. GraphMana depends on the joint caller having already resolved every sample's call status at every polymorphic site; it does not try to reinvent that work.

### Step 1 — call per-sample gVCFs

For every sample `S_i`, run your preferred caller in gVCF mode. The canonical GATK command:

```bash
gatk HaplotypeCaller \
  --reference genome.fa \
  --input "$S_i.bam" \
  --output "$S_i.g.vcf.gz" \
  -ERC GVCF
```

DeepVariant + GLnexus and bcftools have equivalent modes. The result per sample is a gVCF containing variant records plus reference-block records that compactly state which genomic positions were confidently HomRef for that sample.

### Step 2 — aggregate into GenomicsDB or GLnexus

**GenomicsDB (GATK):**

```bash
gatk GenomicsDBImport \
  --genomicsdb-workspace-path cohort_gdb \
  --intervals intervals.bed \
  --sample-name-map sample_gvcfs.tsv
```

**GLnexus (alternative, faster and open-source):**

```bash
glnexus_cli \
  --config gatk \
  --list sample_gvcfs.txt \
  > cohort.bcf
```

Either store can accept new samples incrementally without recalling old ones. This is where gVCF-level incrementality actually lives.

### Step 3 — joint-call to produce a multi-sample VCF

**GATK path:**

```bash
gatk GenotypeGVCFs \
  --reference genome.fa \
  --variant gendb://cohort_gdb \
  --output cohort.joint.vcf.gz
```

**GLnexus path:** the `.bcf` from step 2 is already joint-called; convert to VCF with `bcftools view`.

The output is a multi-sample VCF in which every polymorphic site has an explicit call for every sample — either a genotype or an explicit `./.` where reference-block evidence was insufficient. This is the file GraphMana ingests.

### Step 4 — ingest the joint-called VCF into GraphMana

```bash
graphmana ingest \
  --input cohort.joint.vcf.gz \
  --population-map samples.panel \
  --dataset-id cohort_v1 \
  --reference GRCh38 \
  --mode initial
```

Incremental addition follows the same loop: re-run steps 1–3 over the union of old and new gVCFs, then:

```bash
graphmana ingest \
  --input cohort_v2.joint.vcf.gz \
  --population-map samples_v2.panel \
  --dataset-id cohort_v2 \
  --mode incremental
```

GraphMana will:

- Extend `gt_packed` and `called_packed` on existing Variant nodes for the new samples.
- For variants introduced by the new batch, pad existing samples with Missing (`gt_packed = 3`) and clear their `called_packed` bits.
- Recompute per-population `ac[]`, `an[]`, `af[]` so the denominator excludes not-interrogated samples.

All of this is automatic. Downstream FAST PATH (TreeMix, SFS, BED) and FULL PATH (VCF, PLINK, EIGENSTRAT, Beagle, STRUCTURE, …) exports honor `called_packed` transparently.

---

## Legacy mode for fixed-site-list workflows

If your VCFs come from a fixed-site pipeline (imputed genotypes on a reference panel, SNP arrays, pre-phased reference cohorts like 1000 Genomes Phase 3), "absent = HomRef" is guaranteed by construction. In that case, pass the legacy flag:

```bash
graphmana ingest \
  --input imputed.vcf.gz \
  --population-map samples.panel \
  --mode incremental \
  --assume-homref-on-missing
```

The flag emits a loud warning; use it deliberately. Never use it with a workflow that produces VCFs from independent calling runs.

---

## What GraphMana preserves

With schema v1.1 and a joint-called upstream pipeline, every GraphMana database carries:

| Information | Where it lives |
|---|---|
| Every polymorphic site the joint caller emitted | `Variant` nodes |
| Per-sample genotype at every site (including explicit `./.`) | `gt_packed`, 2 bits/sample |
| Phase (when present) | `phase_packed`, 1 bit/sample |
| Ploidy (chrX, chrY, mitochondria) | `ploidy_packed`, 1 bit/sample |
| Whether each sample was interrogated at each site | `called_packed`, 1 bit/sample |
| Per-population allele counts / freqs | `ac[]`, `an[]`, `af[]`, …  on the Variant |
| Which batch introduced each site | `IngestionLog` nodes (provenance) |

Allele frequencies, SFS, TreeMix and FST computations computed on this database are statistically honest under arbitrary incremental growth, as long as the upstream joint caller was.

---

## What GraphMana does not do

- **Ingest raw per-sample gVCFs directly.** gVCF reference blocks do not map onto the one-Variant-node-per-site graph model; parsing them would require reinventing GenomicsDB/GLnexus inside GraphMana, which is out of scope.
- **Re-joint-call inside the graph.** If you want to try a different joint caller, go back to step 2 and re-ingest.
- **Rescue projects that skipped joint calling.** If earlier batches were ingested as plain VCFs under the legacy `assume_homref_on_missing` assumption, migrating to v1.1 will back-fill `called_packed` with all-ones (preserving the v1.0 behavior) but cannot reconstruct the true call/no-call status retroactively. Document the assumption in your ingestion log, and prefer joint calling for all future batches.

---

## Troubleshooting

**Allele frequencies drop by roughly half after switching from plain VCFs to joint-called VCFs.** Expected. The v1.0 pipeline was counting uninterrogated samples as HomRef, inflating the denominator. The v1.1 pipeline excludes them honestly. The "old" numbers were biased; the "new" numbers are correct.

**PCA leading axis is no longer "batch."** Also expected. The batch-correlated missingness that drove that axis is gone once `called_packed` propagates through allele-frequency recomputation.

**`called_packed` is missing on some Variant nodes.** These are schema v1.0 nodes imported before v1.1. GraphMana's helpers interpret `null` as "all samples called," preserving the old behavior for those rows. Run `graphmana migrate --to-v1.1` to back-fill an explicit all-ones mask and record the assumption.

**Storage ceiling too low for your cohort.** Enable sparse `gt_packed` (automatic for rare variants in v1.1) and consider the v1.2 chunked-property roadmap (see `tasks/v1.1_sparse_called_packed.md`).

---

## References

- GATK Best Practices for joint calling: <https://gatk.broadinstitute.org/hc/en-us>
- GLnexus: <https://github.com/dnanexus-rnd/GLnexus>
- GraphMana schema v1.1 change log: `CHANGELOG.md`
- GraphMana supplementary note: `paper/v4/supplementary.tex` Note 5
