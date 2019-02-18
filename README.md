# Pil

`Pil` is a C++ library specifying a language-independent columnar memory format for flat and hierarchical data, organized for efficient analytic operations on modern hardware. Uniquely, `Pil` databases are constructed using a schema-independent online approach.

We created `Pil` to make the advantages of compressed, efficient columnar data representation with support for very efficient compression and encoding schemes with a focus on genomics. `Pil` allows compression schemes to be specified on a per-column level, and is future-proofed to allow adding more encodings as they are invented and implemented.

## Preliminary results

### FASTQ: Unaligned readset

[ERR194146.1](https://trace.ncbi.nlm.nih.gov/Traces/sra/?run=ERR194146): Illumina HiSeq 2000 run of Coriell CEPH/UTAH 1463 sample.

| Format  | File size  | Import time | Compression ratio |  Random access |
|---------|------------|-------------|-------------------|----------------|
| FASTQ   | 2337920025 | -           | 1                 | No             |
| gzip    | 775486779  | 5m25.651s   | 3.014777          | No             |
| zstd    | 783213067  | 0m32.145s   | 2.985037          | No             |
| fqzcomp | 441223058  | 1m34.187s   | 5.298726          | No             |
| Pil     | 507390923  | 3m34.864s   | 4.607729          | Yes            |

#### Settings

* `gzip -c ERR194146.fastq > ERR194146.fastq.gz`
* `fqzcomp -s3 -e -q2 -n2 ERR194146.fastq ERR194146.fastq.fqz`
* `zstd -3 ERR194146.fastq -o ERR194146.fastq.zst`
* `Pil` used `PIL_COMPRESS_RC_QUAL` and `PIL_COMPRESS_RC_BASES` codecs for per-base quality scores and bases, respectively. Sequence names were tokenized by `:` (colon) and partitioned into `ColumnStores` without additional processing.

### SAM/BAM/CRAM: Unaligned readset

Convert a `FASTQ` file into a `BAM` file using `biobambam2`.