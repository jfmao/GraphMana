# GraphMana v1.1 micro-benchmarks

Reference dataset: 1000 Genomes chromosome 22 scale (N = 3202 samples, 1,066,555 variants).

## 1. Parser overhead (called_packed bit packing on top of v1.0 packer)

- Mean v1.0 packer time per variant: 56.25 µs
- Mean v1.1 packer time per variant: 57.45 µs
- Added time: 1.20 µs (+2.1%)

## 2. Exporter unpack overhead (called_packed → Missing coercion)

- Mean v1.0 unpack time per variant: 14.98 µs
- Mean v1.1 unpack time per variant: 15.26 µs
- Added time: 0.28 µs (+1.9%)

## 3. Storage delta from called_packed

- gt_packed: 801 bytes/variant
- phase_packed: 401 bytes/variant
- called_packed: 401 bytes/variant (new)
- Per-variant total: v1.0 1202 B -> v1.1 1603 B (+33.4%)
- chr22 total (dense, N=3,202): v1.0 1222.6 MB -> v1.1 1630.5 MB
- Whole-genome projection (~85M variants, N=3,202): v1.0 95.15 GB -> v1.1 126.90 GB

## 4. Sparse gt_packed compression across a realistic allele frequency grid

| allele freq | mean dense (B) | mean tagged blob (B) | compression | sparse chosen |
|---|---|---|---|---|
| 0.001 | 802 | 171 | 4.70x | 100% |
| 0.003 | 802 | 225 | 3.57x | 100% |
| 0.005 | 802 | 283 | 2.84x | 100% |
| 0.010 | 802 | 411 | 1.95x | 100% |
| 0.020 | 802 | 680 | 1.18x | 100% |
| 0.050 | 802 | 802 | 1.00x | 0% |
| 0.100 | 802 | 802 | 1.00x | 0% |
| 0.200 | 802 | 802 | 1.00x | 0% |
| 0.300 | 802 | 802 | 1.00x | 0% |
| 0.500 | 802 | 802 | 1.00x | 0% |

Neutral-SFS weighted mean compression: **3.92x** on gt_packed.

Under this compression ratio, the effective per-variant byte budget at the Neo4j property-size comfort zone (~12.5 KB, corresponding to ~50,000 samples in v1.0 dense encoding) now accommodates approximately **195,843** samples at equivalent storage pressure.
