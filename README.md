# Pil

## Experimental repository in pre-alpha. Use with caution!

`Pil` (arrow in Swedish) is an open-source C++ library specifying a language-independent columnar memory format for flat and nested data, organized for efficient analytic operations on modern hardware. The memory-layout permits O(1)-time random access. The layout is highly cache-efficient in analytics workloads and permits SIMD optimizations with modern processors. Uniquely, `Pil` supports streaming construction of schema-agnostic archives.

We created `Pil` to make the advantages of compressed, efficient columnar data representation with support for very efficient compression and encoding schemes with a focus on supporting genomics data. `Pil` allows compression schemes to be specified on a per-column level, and is future-proofed to allow adding more encodings as they are invented and implemented.

## Subproject: Pillar


`Pillar` is the specialized implementation that can consume the majority of the incumbent genomics interchange formats including: [SAM](htsspec), [BAM](htsspec), [CRAM](htsspec), [VCF](htsspec), [BCF](htsspec), [YON](https://github.com/mklarqvist/tachyon), [FASTA](https://en.wikipedia.org/wiki/FASTA_format), [FASTQ](https://academic.oup.com/nar/article/38/6/1767/3112533), [BED](https://www.ensembl.org/info/website/upload/bed.html), [GTF2](https://www.ensembl.org/info/website/upload/gff.html), [GFF3](https://www.ensembl.org/info/website/upload/gff3.html) and have native coding support for sequencing-specific range codecs (CRAM and [fqzcomp](https://github.com/jkbonfield/fqzcomp)), [PBWT](https://github.com/richarddurbin/pbwt) ([BGT](https://github.com/lh3/bgt)), genotype-PBWT (YON), multi-symbol-PBWT, and individual-centric WAH-bitmaps ([GQT](https://github.com/ryanlayer/gqt)).

| Type            | Format(s)          |
|-----------------|--------------------|
| Sequence        | FASTA, 2bit-FASTA  |
| Annotations     | BED, GTF2, GFF3    |
| Read alignments | SAM, BAM, CRAM     |
| Variant         | VCF, BCF, GQT, BGT |

## Preliminary results

### FASTQ: Unaligned readset

[ERR194146.1](https://trace.ncbi.nlm.nih.gov/Traces/sra/?run=ERR194146): Illumina HiSeq 2000 run of Coriell CEPH/UTAH 1463 sample.

| Format  | File size  | Import time | Compression ratio |  Random access |
|---------|------------|-------------|-------------------|----------------|
| FASTQ   | 2337920025 | -           | 1                 | No             |
| gzip    | 775486779  | 5m25.651s   | 3.014777          | No             |
| zstd    | 783213067  | 0m32.145s   | 2.985037          | No             |
| fqzcomp | 441223058  | 1m34.187s   | 5.298726          | No             |
| Pil     | 501741060  | 3m34.864s   | 4.440460          | Yes            |
| Pil-65536     | 469251192  | 5m16.680s   | 4.982236          | Yes            |

#### Settings

* `gzip -c ERR194146.fastq > ERR194146.fastq.gz`
* `fqzcomp -s3 -e -q2 -n2 ERR194146.fastq ERR194146.fastq.fqz`
* `zstd -3 ERR194146.fastq -o ERR194146.fastq.zst`
* `Pil` used `PIL_COMPRESS_RC_QUAL` and `PIL_COMPRESS_RC_BASES` codecs for per-base quality scores and bases, respectively. Sequence names were tokenized by `:` (colon) and partitioned into `ColumnStores` without additional processing.
* `Pil-65536` was run with the same settings but the `RecordBatch` size set to 65536 instead of 8192.

### SAM/BAM/CRAM: Unaligned readset

NA12878J: Illumina HiSeq-X run of Coriell CEPH/UTAH 1463 sample comprising 122.6 gigabases at 30× coverage. The original dataset from the [Garvan Institute of Medical Research](http://www.garvan.org.au/) consists of the following files:

* [NA12878J_HiSeqX_R1.fastq.gz](https://dnanexus-rnd.s3.amazonaws.com/NA12878-xten/reads/NA12878J_HiSeqX_R1.fastq.gz)
* [NA12878J_HiSeqX_R2.fastq.gz](https://dnanexus-rnd.s3.amazonaws.com/NA12878-xten/reads/NA12878J_HiSeqX_R2.fastq.gz)

We will use the first 50 million reads from `NA12878J_HiSeqX_R1` to convert into a `BAM` file using `biobambam2`.

| Format   | File size  | Import time            | Compression ratio | Random access |
|----------|------------|------------------------|-------------------|---------------|
| FASTQ    | 4469877114 | -                      | 1                 | No            |
| FASTQ.gz | 1185209615 | 7m59.405s              | 3.771381          | No            |
| fqzcomp  | 741975278  | 2m24.190s              | 6.024294          | No            |
| SAM      | 4532377114 | -                      | 0.986             | No            |
| SAM.gz   | 1187984017 | 7m48.570s              | 3.762573          | No            |
| BAM      | 1208014419 | 4m8.770s*, 4m47.160s** | 3.700185          | Partial       |
| CRAM     | 860706428  | 1m29.555s              | 5.193266          | Partial       |
| Pil      | 801811920  | 3m59.769s              | 5.57472           | Yes           |

\* `SAM`->`BAM`  
\*\* `FASTQ` -> `BAM`

#### Settings

* `gzip -c NA12878J_HiSeqX_R1_50mil.fastq > NA12878J_HiSeqX_R1_50mil.fastq.gz`
* `fqzcomp -s3 -e -q2 -n2 NA12878J_HiSeqX_R1_50mil.fastq NA12878J_HiSeqX_R1_50mil.fastq.fqz`
* `fastqtobam NA12878J_HiSeqX_R1_50mil.fastq > NA12878J_HiSeqX_R1_50mil.fastq.bam`
* `samtools view NA12878J_HiSeqX_R1_50mil.fastq.sam -O bam > NA12878J_HiSeqX_R1_50mil.fastq.bam`
* `samtools view NA12878J_HiSeqX_R1_50mil.fastq.bam -O cram > NA12878J_HiSeqX_R1_50mil.fastq.cram`
* `gzip -c NA12878J_HiSeqX_R1_50mil.fastq.sam > NA12878J_HiSeqX_R1_50mil.fastq.sam.gz`
* `Pil` used `PIL_COMPRESS_RC_QUAL` and `PIL_COMPRESS_RC_BASES` codecs for per-base quality scores and bases, respectively. Sequence names were tokenized by `:` (colon) and partitioned into `ColumnStores` without additional processing.

### SAM/BAM/CRAM: Aligned readset

Aligned data from above.

| Format | File size              | Import time | Compression ratio   | Random access |
|--------|------------------------|-------------|---------------------|---------------|
| SAM    | 5271405563             | -           | 1                   | No            |
| BAM    | 1540663158             | 5m5.326s    | 3.421517            | Partial       |
| CRAM   | 534863873*             | 2m5.874s    | 9.855602            | Partial       |
| Pil    | 1000020959 (560239248**) | 5m30.744s   | 5.271295 (9.409204**) | Yes           |

\* `CRAM` requires an external reference, in this case `hg19.fa.gz`, that is 948731427 bytes.  
\*\* Running `Pil` with an external reference sequence like `CRAM`.

#### Settings


* `samtools view NA12878J_HiSeqX_R1_50mil.fastq.aligned.sam -O bam > NA12878J_HiSeqX_R1_50mil.fastq.aligned.bam`
* `samtools view NA12878J_HiSeqX_R1_50mil.fastq.aligned.bam -O cram > NA12878J_HiSeqX_R1_50mil.fastq.aligned.cram`
* `Pil` used `PIL_COMPRESS_RC_QUAL` and `PIL_COMPRESS_RC_BASES` codecs for per-base quality scores and bases, respectively. Sequence names were tokenized by `:` (colon) and partitioned into `ColumnStores` without additional processing.


[htsspec]: https://github.com/samtools/hts-specs