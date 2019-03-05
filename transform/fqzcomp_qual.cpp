// We use generic maps to turn 0-M into 0-N where N <= M
// before adding these into the context.  These are used
// for positions, running-diffs and quality values.
//
// This can be used as a simple divisor, eg pos/24 to get
// 2 bits of positional data for each quarter along a 100bp
// read, or it can be tailored for specific such as noting
// the first 5 cycles are poor, then we have stability and
// a gradual drop off in the last 20 or so.  Perhaps we then
// map pos 0-4=0, 5-79=1, 80-89=2, 90-99=3.
//
// We don't need to specify how many bits of data we are
// using (2 in the above example), as that is just implicit
// in the values in the map.  Specify not to use a map simply
// disables that context type (our map is essentially 0-M -> 0).

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <math.h>
#include <cstdint>

#include "fqzcomp_qual.h"
#include "range_coder.h"
#include "frequency_model.h"

#define QMAX 256
#define QBITS 12
#define QSIZE (1<<QBITS)

#ifndef UNSAFE_MIN
#define UNSAFE_MIN(a,b) ((a)<(b)?(a):(b))
#endif
#ifndef UNSAFE_MAX
#define UNSAFE_MAX(a,b) ((a)>(b)?(a):(b))
#endif

namespace pil {

// An array of 0,0,0, 1,1,1,1, 3, 5,5
// is turned into a run-length of 3x0, 4x1, 0x2, 1x4, 0x4, 2x5,
// which then becomes 3 4 0 1 0 2.
//
// NB: size > 255 therefore means we need to repeatedly read to find
// the actual run length.
// Alternatively we could bit-encode instead of byte encode, eg BETA.
static int store_array(uint8_t *out, uint32_t *array, int size) {
    int i, j, k;
    for (i = j = k = 0; i < size; j++) {
        int run_len = i;
        while (i < size && array[i] == j)
            i++;
        run_len = i-run_len;

        int r;
        do {
            r = UNSAFE_MIN(255, run_len);
            out[k++] = r;
            run_len -= r;
        } while (r == 255);
    }
    while (i < size)
        out[k++] = 0, j++;

    // RLE on out.
    //    1 2 3 3 3 3 3 4 4    5
    // => 1 2 3 3 +3... 4 4 +0 5
    int last = -1;
    for (i = j = 0; j < k; i++) {
        out[i] = out[j++];
        if (out[i] == last) {
            int n = j;
            while (j < k && out[j] == last)
                j++;
            out[++i] = j-n;
        } else {
            last = out[i];
        }
    }
    k = i;

//    fprintf(stderr, "Store_array %d => %d {", size, k);
//    for (i = 0; i < k; i++)
//  fprintf(stderr, "%d,", out[i]);
//    fprintf(stderr, "}\n");
    return k;
}

static int read_array(uint8_t *in, uint32_t *array, int size) {
    int i, j, k, last = -1, r2 = 0;

    for (i = j = k = 0; j < size; i++) {
        int run_len;
        if (r2) {
            run_len = last;
        } else {
            run_len = 0;
            int r, loop = 0;
            do {
                r = in[k++];
                //fprintf(stderr, "%d,", r);
                if (++loop == 3)
                    run_len += r*255, r=255;
                else
                    run_len += r;
            } while (r == 255);
        }
        if (r2 == 0 &&  run_len == last) {
            r2 = in[k++];
            //fprintf(stderr, "%d,", r2);
        } else {
            if (r2) r2--;
            last = run_len;
        }

        while (run_len && j < size)
            run_len--, array[j++] = i;
    }
    //fprintf(stderr, "}\n");

    return k;
}

// FIXME: how to auto-tune these rather than trial and error?
static int strat_opts[][10] = {
    {10, 5, 4, -1, 2, 1, 0, 9, 10, 14}, // basic options (level < 7)
  //{10, 5, 7, 0, 2, 1, 0, 15, 9, 14},  // e.g. HiSeq 2000
    {9, 5, 7, 0, 2, 0, 7, 15, 0, 14},   // e.g. HiSeq 2000, ~2.1% smaller than above
    {12, 6, 2, 0, 2, 3, 0, 9, 12, 14},  // e.g. MiSeq
    {12, 6, 0, 0, 0, 0, 0, 12, 0, 0},   // e.g. IonTorrent; adaptive O1
    {0, 0, 0, 0, 0,  0, 0, 0, 0, 0},    // custom
};

int FqzCompQual::Compress(int vers, int level, std::shared_ptr<ColumnSet> quals, std::shared_ptr<ResizableBuffer> out, size_t& out_size)
{
    //approx sqrt(delta), must be sequential
    int dsqr[] = {
        0, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3,
        4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5,
        5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
        6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7
    };
    uint32_t qtab[256]  = {0};
    uint32_t ptab[1024] = {0};
    uint32_t dtab[256]  = {0};
    uint32_t stab[256]  = {0};

    int comp_idx = 0;
    size_t i, j;
    ssize_t rec = 0;
    uint32_t last = 0, qlast = 0;
    RangeCoder rc;
    uint8_t q1 = 1;

    int strat = vers >> 8; // stratification options appears to be the second byte
    if (strat > 4) strat = 4;
    vers &= 0xff;

    const uint32_t n_records = quals->columns[0]->n_records - 1;
    const uint32_t in_size = quals->columns[1]->uncompressed_size;
    const uint8_t* in = quals->columns[1]->mutable_data();
    const uint32_t* q_len = &reinterpret_cast<const uint32_t*>(quals->columns[0]->mutable_data())[1]; // first value is always 0
    uint8_t* comp = out->mutable_data();

#define NP 128
    uint32_t qhist[256] = {0}, nsym, max_sym;
    uint32_t qhist1[256][NP] = {{0}}, qhist2[256][NP] = {{0}};
    int do_dedup = 0;
    int do_rev = 0; // legacy (false)
    for (i = 0; i < in_size; i++)
        qhist[in[i]]++;

    // Compute possible strand dependence and whether dedup helps.
    int dir = 0;
    int last_len = 0;
    uint64_t t1[NP] = {0}, t2[NP] = {0};
    for (rec = i = j = 0; i < in_size; i++, j--) {
        if (j == 0) {
            if (rec < n_records) {
                j = q_len[rec];
                //dir = s->crecs[rec].flags & BAM_FREAD2 ? 1 : 0;
                dir = 0;
                if (i > 0 && j == last_len && !memcmp(in+i-last_len, in+i, j))
                    do_dedup++; // cache which records are dup?
                last_len = j;
            } else {
                j = in_size - i;
                dir = 0;
            }
            rec++;
        }

        if (dir)
            qhist2[in[i]][j&(NP-1)]++, t2[j&(NP-1)]++;
        else
            qhist1[in[i]][j&(NP-1)]++, t1[j&(NP-1)]++;
    }
    do_dedup = ((rec+1) / (do_dedup+1) < 500);
    rec = 0;
    last_len = 0;

    double e2 = 0;
    for (j = 0; j < NP; j++) {
        if (!t1[j] || !t2[j]) continue;
        for (i = 0; i < 256; i++) {
            if (!qhist[i]) continue;
            e2 += qhist1[i][j] > qhist2[i][j]
                ? qhist1[i][j]/(t1[j]+1.) - qhist2[i][j]/(t2[j]+1.)
                : qhist2[i][j]/(t2[j]+1.) - qhist1[i][j]/(t1[j]+1.);
        }
    }
    int do_strand = (e2 > 77);

    for (i = max_sym = nsym = 0; i < 256; i++)
        if (qhist[i])
            max_sym = i, nsym++;

    int store_qmap = (nsym <= 8 && nsym*2 < max_sym);

    // Check for fixed length.
    int first_len = q_len[0];
    for (i = 1; i < n_records; i++) {
        if (q_len[i] != first_len)
            break;
    }
    int fixed_len = (i == n_records);

    int store_qtab  = 0; // unused by current encoder
    int q_qctxbits  = strat_opts[strat][0];
    int q_qctxshift = strat_opts[strat][1];
    int q_pctxbits  = strat_opts[strat][2];
    int q_pctxshift = strat_opts[strat][3];
    int q_dctxbits  = strat_opts[strat][4];
    int q_dctxshift = strat_opts[strat][5];
    int q_qloc      = strat_opts[strat][6];
    int q_sloc      = strat_opts[strat][7];
    int q_ploc      = strat_opts[strat][8];
    int q_dloc      = strat_opts[strat][9];

    if (strat >= 4) {
        goto manually_set; // used in TEST_MAIN for debugging
    }

    if (q_pctxshift < 0) {
        //q_pctxshift = UNSAFE_MAX(0, log((double)s->crecs[0].len/(1<<q_pctxbits))/log(2)+.5);
        q_pctxshift = UNSAFE_MAX(0, log((double)q_len[0] / (1 << q_pctxbits)) / log(2) + .5);
    }

    if (nsym <= 4) {
        // NovaSeq
        q_qctxshift = 2; // qmax 64, although we can store up to 256 if needed
        if (in_size < 5000000) {
            q_pctxbits  = 2;
            q_pctxshift = 5;
        }
    } else if (nsym <= 8) {
        // HiSeqX
        q_qctxbits = UNSAFE_MIN(q_qctxbits, 9);
        q_qctxshift = 3;
        if (in_size < 5000000)
            q_qctxbits = 6 ;
    }

    if (in_size < 300000) {
        q_qctxbits = q_qctxshift;
        q_dctxbits = 2;
    }

 manually_set:
    // If nsym is significant lower than max_sym, store a lookup table and
    // transform prior to compression.  Eg 4 or 8 binned data.
    comp[comp_idx++] = 5; // FMT 5: this code
    comp[comp_idx++] =
        (store_qtab << 7) |
        ((q_dctxbits>0) << 6) |
        ((q_pctxbits>0) << 5) |
        (do_rev << 4) |
        (do_strand << 3) |
        (fixed_len << 2) |
        (do_dedup << 1) |
        (store_qmap);
    comp[comp_idx++] = max_sym;

//    fprintf(stderr, "strat %d 0x%x%x%x%x%x%x%x%x%x%x\n", strat,
//      q_qctxbits, q_qctxshift,
//      q_pctxbits, q_pctxshift,
//      q_dctxbits, q_dctxshift,
//      q_qloc    , q_sloc,
//      q_ploc    , q_dloc);

    for (i = 0; i < sizeof(dsqr)/sizeof(*dsqr); i++) {
        if (dsqr[i] > (1 << q_dctxbits) - 1)
            dsqr[i] = (1 << q_dctxbits) - 1;
    }

    comp[comp_idx++] = (q_qctxbits << 4) | q_qctxshift;
    comp[comp_idx++] = (q_qloc << 4) | q_sloc;
    comp[comp_idx++] = (q_ploc << 4) | q_dloc;

    if (store_qmap) {
        comp[comp_idx++] = nsym;
        int comp_idx_start = comp_idx;
        for (i = 0; i < 256; i++) {
            if (qhist[i])
                comp[comp_idx] = i, qhist[i] = comp_idx++ -comp_idx_start;
        }
        max_sym = nsym;
    } else {
        nsym = 255;
        for (i = 0; i < 256; i++)
            qhist[i] = i;
    }

    // Produce ptab from pctxshift.
    if (q_qctxbits) {
        for (i = 0; i < 256; i++) {
            qtab[i] = i;
            // Alternative mappings:
            //qtab[i] = i<max_sym ? i : max_sym;
            //qtab[i] = i > 30 ? UNSAFE_MIN(max_sym,i)-15 : i/2;  // eg for 9827 BAM
            //qtab[i] = pow(i,1.5)*0.12;
        }

    // Eg map Q8 to Q4 and then compress with 2 bit instead of 3.
    //  int j = 0;
    //  qtab[0] = j;
    //  qtab[1] = j++;
    //  qtab[2] = j;
    //  qtab[3] = j++;
    //  qtab[4] = j;
    //  qtab[5] = j++;
    //  qtab[6] = j;
    //  q_qctxshift = 2;

        // custom qtab
        if (store_qtab)
            comp_idx += store_array(comp + comp_idx, qtab, 256);
    }

    if (q_pctxbits) {
        for (i = 0; i < 1024; i++)
            ptab[i] = UNSAFE_MIN((1 << q_pctxbits) - 1, i >> q_pctxshift);

    //  // Eg a particular novaseq run, 2.8% less, -x 0xa2302108ae
    //  i = j = 0;
    //  while (i <= 10)  {ptab[i++] = j;}  j++;
    //  while (i <= 12)  {ptab[i++] = j;}  j++;
    //  while (i <= 22)  {ptab[i++] = j;}  j++;
    //  while (i <= 24)  {ptab[i++] = j;}  j++;
    //  while (i <= 27)  {ptab[i++] = j;}  j++;
    //  while (i <= 37)  {ptab[i++] = j;}  j++;
    //  while (i <= 83)  {ptab[i++] = j;}  j++;
    //  while (i <= 95)  {ptab[i++] = j;}  j++;
    //  while (i <= 127) {ptab[i++] = j;}  j++;
    //  while (i <= 143) {ptab[i++] = j;}  j++;
    //  while (i <= 1023){ptab[i++] = j;}  j++;

        // custom ptab
        comp_idx += store_array(comp+comp_idx, ptab, 1024);

        for (i = 0; i < 1024; i++)
            ptab[i] <<= q_ploc;
    }

    if (q_dctxbits) {
        for (i = 0; i < 256; i++)
            dtab[i] = dsqr[UNSAFE_MIN(sizeof(dsqr) / sizeof(*dsqr) - 1, i >> q_dctxshift)];

        // custom dtab
        comp_idx += store_array(comp + comp_idx, dtab, 256);

        for (i = 0; i < 256; i++)
            dtab[i] <<= q_dloc;
    }

    if (do_strand)
        stab[1] = 1<<q_sloc;

    const uint32_t n_qmodels = (1 << 16);
    FrequencyModel<QMAX>* model_qual = new FrequencyModel<QMAX>[n_qmodels];
    // Not default constructor
    for (i = 0; i < n_qmodels; i++) model_qual[i].Initiate(max_sym + 1);

    FrequencyModel<256> model_len[4];
    FrequencyModel<2>   model_revcomp(2);
    FrequencyModel<2>   model_strand(2);
    FrequencyModel<2>   model_dup(2);

    rc.SetOutput((char*)comp+comp_idx);
    rc.StartEncode();

    int delta = 0, rev = 0;
    int ndup0 = 0, ndup1 = 0;

    // We encode in original orientation for V3.1 (automatically done for V4.0).
    // Therefore during decode we'll need to do two passes, first to decode
    // and perform delta, and second to reverse back again.
    // These two can be merged with a bit of cleverness, but we do it simply for now.
    int read2 = 0;
    for (i = j = 0; i < in_size; i++, j--) {
        //std::cerr << i << "," << j << "," << in_size << std::endl;
        if (j == 0) {
            //std::cerr << "j==0" <<std::endl;

            // Quality buffer maybe longer than sum of reads if we've
            // inserted a specific base + quality pair.
            // Todo; fix
            //int len = rec < n_records ? q_len[rec] : in_size - s->crecs[n_records-1].qual;
            //std::cerr << "rec=" << rec << std::endl;
            int len = q_len[rec];
            //std::cerr << "len=" << len << std::endl;

            if (!fixed_len || rec == 0) {
                model_len[0].EncodeSymbol(&rc, (len >> 0) & 0xff);
                model_len[1].EncodeSymbol(&rc, (len >> 8) & 0xff);
                model_len[2].EncodeSymbol(&rc, (len >>16) & 0xff);
                model_len[3].EncodeSymbol(&rc, (len >>24) & 0xff);
            }

            if (do_rev) {
                // no need to reverse complement for V4.0 as the core format
                // already has this feature.
                //if (s->crecs[rec].flags & BAM_FREVERSE)
                //    model_revcomp.EncodeSymbol(&rc, 1);
                //else
                    model_revcomp.EncodeSymbol(&rc, 0);
            }

            if (do_strand) {
                //read2 = (s->crecs[rec].flags & BAM_FREAD2) ? 0 : 1;
                read2 = 0;
                model_strand.EncodeSymbol(&rc, read2);
            }

            rec++;
            j = len;
            delta = last = qlast = q1 = 0;

            if (do_dedup) {
                // Possible dup of previous read?
                if (i && len == last_len && !memcmp(in + i - last_len, in + i, len)) {
                    model_dup.EncodeSymbol(&rc, 1);
                    i += len - 1;
                    j = 1;
                    ndup1++;
                    continue;
                }
                model_dup.EncodeSymbol(&rc, 0);
                ndup0++;

                last_len = len;
            }
        }

        uint8_t q = in[i];
        model_qual[last].EncodeSymbol(&rc, qhist[q]);

        qlast = (qlast << q_qctxshift) + qtab[qhist[q]];
        last  = (qlast & ((1 << q_qctxbits) - 1)) << q_qloc;
        last += ptab[UNSAFE_MIN(j,1024)]; //limits max pos
        last += stab[read2];
        last += dtab[delta];
        last &= 0xffff;
        assert(last < n_qmodels);

        _mm_prefetch((const char *)&model_qual[last], _MM_HINT_T0);

        delta += (q1 != q) * (delta<255);  // limits delta (not in original code)
        q1 = q;
    }

    rc.FinishEncode();

    if (do_rev) {
        // Pass 3, un-reverse all seqs if necessary.
        i = rec = j = 0;
        while (i < in_size) {

            //int len = rec < n_records-1
            //    ? s->crecs[rec].len
            //    : in_size - i;

            int len = rec < n_records - 1
                ? q_len[rec]
                : in_size - i;

            // PIL has no knowledge of this
            /*
            if (s->crecs[rec].flags & BAM_FREVERSE) {
                // Reverse complement sequence - note: modifies buffer
                int I,J;
                uint8_t *cp = in+i;
                for (I = 0, J = len-1; I < J; I++, J--) {
                    uint8_t c;
                    c = cp[I];
                    cp[I] = cp[J];
                    cp[J] = c;
                }
            }
            */

            i += len;
            rec++;
        }
    }

    out_size = comp_idx + rc.OutSize();

//    fprintf(stderr, "%d / %d %d %d %d %d / %d %d %d %d %d %d %d %d %d %d = %d to %d\n",
//      nsym, do_rev, do_strand, fixed_len, do_dedup, store_qmap,
//      q_qctxbits,
//      q_qctxshift,
//      q_pctxbits,
//      q_pctxshift,
//      q_dctxbits,
//      q_dctxshift,
//      q_qloc,
//      q_sloc,
//      q_ploc,
//      q_dloc,
//      (int)in_size, (int)out_size);

    delete[] model_qual;
    //return comp;
    return(out_size);
}

int FqzCompQual::Decompress(std::shared_ptr<ColumnSet> quals, std::shared_ptr<ResizableBuffer> out, size_t& out_size)
{
    uint32_t qtab[256]  = {0};
    uint32_t ptab[1024] = {0};
    uint32_t dtab[256]  = {0};
    uint32_t stab[256]  = {0};

    uint8_t* uncomp = out->mutable_data();
    RangeCoder rc;
    size_t i, j, rec = 0, len = out_size, in_idx = 0;
    uint8_t q1 = 1;
    uint32_t last = 0, qlast = 0;

    uint8_t* in = quals->columns[1]->mutable_data();


    int vers = in[in_idx++];
    if (vers != 5) {
        fprintf(stderr, "This version of fqzcomp only supports format 5\n");
        return -1;
    }

    int flags      = in[in_idx++];
    int have_qtab  = flags & 128;
    int have_dtab  = flags & 64;
    int have_ptab  = flags & 32;
    int do_rev     = flags & 16;
    int do_strand  = flags & 8;
    int fixed_len  = flags & 4;
    int do_dedup   = flags & 2;
    int store_qmap = flags & 1;
    int max_sym    = in[in_idx++];

    int q_qctxbits = in[in_idx] >> 4;
    int q_qctxshift= in[in_idx++] & 15;
    int q_qloc     = in[in_idx] >> 4;
    int q_sloc     = in[in_idx++] & 15;
    int q_ploc     = in[in_idx] >> 4;
    int q_dloc     = in[in_idx++] & 15;

    int qmap[256];
    if (store_qmap) {
        int nsym = in[in_idx++];
        for (i = 0; i < nsym; i++)
            qmap[i] = in[in_idx++];
        max_sym = nsym;
    } else {
        for (i = 0; i < 256; i++)
            qmap[i] = i;
    }

    // Produce ptab from pctxshift.
    if (q_qctxbits) {
        if (have_qtab)
            in_idx += read_array(in+in_idx, qtab, 256);
        else
            for (i = 0; i < 256; i++)
            qtab[i] = i;
    }

    if (have_ptab)
        in_idx += read_array(in+in_idx, ptab, 1024);
    for (i = 0; i < 1024; i++)
        ptab[i] <<= q_ploc;

    if (have_dtab)
        in_idx += read_array(in+in_idx, dtab, 256);
    for (i = 0; i < 256; i++)
        dtab[i] <<= q_dloc;

    if (do_strand)
        stab[1] = 1<<q_sloc;

    const uint32_t n_qmodels = (1 << 16);
    FrequencyModel<QMAX>* model_qual = new FrequencyModel<QMAX>[n_qmodels];
    // Not default constructor
    for (i = 0; i < n_qmodels; i++) model_qual[i].Initiate(max_sym + 1);

    if (!model_qual)
        return -2;

    FrequencyModel<256> model_len[4];
    FrequencyModel<2> model_revcomp(2);
    FrequencyModel<2> model_strand(2);

    int delta = 0, rev = 0;

    FrequencyModel<2> model_dup(2);

    rc.SetInput((char *)in+in_idx);
    rc.StartDecode();

    int nrec = 1000;
    char *rev_a = (char*)malloc(nrec);
    int *len_a = (int*)malloc(nrec * sizeof(int));
    if (!rev_a || !len_a) return -3;

    int last_len = 0, read2 = 0;
    for (rec = i = j = 0; i < len; i++, j--) {
        if (rec >= nrec) {
            nrec *= 2;
            rev_a = (char*)realloc(rev_a, nrec);
            len_a = (int*)realloc(len_a, nrec*sizeof(int));
            if (!rev_a || !len_a) return -4;
        }

        if (j == 0) {
            int len = last_len;
            if (!fixed_len || rec == 0) {
                len   = model_len[0].DecodeSymbol(&rc);
                len  |= model_len[1].DecodeSymbol(&rc) << 8;
                len  |= model_len[2].DecodeSymbol(&rc) << 16;
                len  |= model_len[3].DecodeSymbol(&rc) << 24;
                last_len = len;
            }

            if (do_rev) {
                rev = model_revcomp.DecodeSymbol(&rc);
                rev_a[rec] = rev;
                len_a[rec] = len;
            }

            if (do_strand)
                read2 = model_strand.DecodeSymbol(&rc);

            if (do_dedup) {
                if (model_dup.DecodeSymbol(&rc)) {
                    // Dup of last line
                    memcpy(uncomp+i, uncomp+i-len, len);
                    i += len-1;
                    j = 1;
                    rec++;
                    continue;
                }
            }

            rec++;
            j = len;
            delta = last = qlast = q1 = 0;
        }

        uint8_t q, Q;

        Q = model_qual[last].DecodeSymbol(&rc);
        q = qmap[Q];

        qlast = (qlast<<q_qctxshift) + qtab[Q];
        last = (qlast & ((1<<q_qctxbits)-1)) << q_qloc;
        last += ptab[UNSAFE_MIN(j,1024)]; //limits max pos
        last += stab[read2];
        last += dtab[delta];

        last &= 0xffff;

        delta += (q1 != q) * (delta<255);
        q1 = q;
        uncomp[i] = q;
    }
    rev_a[rec] = rev;
    len_a[rec] = len;

    if (do_rev) {
        for (i = rec = 0; i < len; i += len_a[rec++]) {
            if (!rev_a[rec])
                continue;

            int I, J;
            uint8_t *cp = uncomp+i;
            for (I = 0, J = len_a[rec]-1; I < J; I++, J--) {
                uint8_t c;
                c = cp[I];
                cp[I] = cp[J];
                cp[J] = c;
            }
        }
    }

    rc.FinishDecode();
    free(rev_a);
    free(len_a);

    return out_size;
}

} // end namespace pil
