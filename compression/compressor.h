#ifndef ALGORITHM_COMPRESSION_ZSTD_CODEC_H_
#define ALGORITHM_COMPRESSION_ZSTD_CODEC_H_

#include <cstdint>

#include "zstd.h"
#include "zstd_errors.h"

#include "../table_dict.h"
#include "../columnstore.h"
#include "frequency_model.h"
#include "fastdelta.h"
#include "base_model.h"

namespace pil {

class Compressor {
public:
    Compressor() : pool_(pil::default_memory_pool()){}
    virtual ~Compressor(){}

    int Compress(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field);
    int CompressAuto(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field);
    virtual int Decompress(){ return 1; }

    inline std::shared_ptr<ResizableBuffer> data() const { return(buffer); }

protected:
    // Any memory is owned by the respective Buffer instance (or its parents).
    MemoryPool* pool_;
    std::shared_ptr<ResizableBuffer> buffer;
};

class ZstdCompressor : public Compressor {
private:
	typedef        ZstdCompressor self_type;
	typedef struct ZSTD_CCtx_s    ZSTD_CCtx;
	typedef struct ZSTD_DCtx_s    ZSTD_DCtx;

public:
	ZstdCompressor();
	~ZstdCompressor();

	int Compress(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field, const int compression_level = 1) {
	    if(cset.get() == nullptr) return(-1);

	    int ret = 0;
        if(field.cstore == PIL_CSTORE_COLUMN) {
           std::cerr << "in cstore col: n=" << cset->size() << std::endl;
           for(int i = 0; i < cset->size(); ++i) {
               int ret2 = Compress(
                       cset->columns[i]->buffer->mutable_data(),
                       cset->columns[i]->uncompressed_size,
                       compression_level);

               cset->columns[i]->compressed_size = ret2;
               ret += ret2;
           }
           return(ret);
        } else if(field.cstore == PIL_CSTORE_TENSOR) {

        }
        return(ret);
	}

	int Compress(const uint8_t* src, const uint32_t n_src, const int compression_level = 1) {
	    if(buffer.get() == nullptr) {
            assert(AllocateResizableBuffer(pool_, n_src + 16384, &buffer) == 1);
        }

        if(buffer->capacity() < n_src + 16384){
            //std::cerr << "here in limit=" << n*sizeof(T) << "/" << buffer->capacity() << std::endl;
            assert(buffer->Reserve(n_src + 16384) == 1);
        }

        const int32_t ret = ZSTD_compress(buffer->mutable_data(), buffer->capacity(), src, n_src, compression_level);
        if(ZSTD_isError(ret)){
            std::cerr << "zstd error: " << ZSTD_getErrorString(ZSTD_getErrorCode(ret)) << std::endl;
            return(-1);
        }

        return(ret);
	}

    //int Compress(std::shared_ptr<ResizableBuffer> in_buffer, const int compression_level = 1) { return 1; }
    //int Compress(std::shared_ptr<ColumnStore> cstore, const int compression_level = 1) { return 1; }
    int Decompress() override{ return 1; }
};


#define QMAX 64
/* QBITS is the size of the quality context, 2x quals */
#define QBITS 12
#define QSIZE (1 << QBITS)

#ifdef __SSE__
#   include <xmmintrin.h>
#else
#   define _mm_prefetch(a,b)
#endif

// Unsafe
#define ABS(a)   ((a)>0?(a):-(a))
#define MIN(a,b) ((a)<(b)?(a):(b))
#define MAX(a,b) ((a)>(b)?(a):(b))

class QualityCompressor : public Compressor {
public:
    QualityCompressor(){}

    int Compress(std::shared_ptr<ColumnSet> cset, PIL_CSTORE_TYPE cstore) {
        if(cset.get() == nullptr) return(-1);

        int ret = 0;
        if(cstore == PIL_CSTORE_COLUMN) {
            std::cerr << "in cstore col" << std::endl;
            for(int i = 0; i < cset->columns.size(); ++i) {
                uint32_t n_l = cset->columns[i]->n;
                int ret2 = Compress(cset->columns[i]->buffer->mutable_data(), cset->columns[i]->n, &n_l, 1);
                cset->columns[i]->compressed_size = ret2;
                cset->columns[i]->transformations.push_back(PIL_COMPRESS_RC_QUAL);
                ret += ret2;
            }
        } else if(cstore == PIL_CSTORE_TENSOR) {
            std::cerr << "in cstore tensor" << std::endl;
            //std::cerr << "buffer believe=" << cset->columns[1]->buffer->size() << " and " << cset->columns[1]->uncompressed_size << " and n=" << cset->columns[0]->n << std::endl;
            std::cerr << "computing deltas in place" << std::endl;
            compute_deltas_inplace(reinterpret_cast<uint32_t*>(cset->columns[0]->buffer->mutable_data()),
                                   cset->columns[0]->n,
                                   0);

            cset->columns[0]->transformations.push_back(PIL_ENCODE_DELTA);

            int ret2 = Compress(cset->columns[1]->buffer->mutable_data(),
                            cset->columns[1]->uncompressed_size,
                            reinterpret_cast<uint32_t*>(cset->columns[0]->buffer->mutable_data()),
                            cset->columns[0]->n);

            cset->columns[1]->compressed_size = ret2;
            cset->columns[1]->transformations.push_back(PIL_COMPRESS_RC_QUAL);
            cset->columns[0]->transformations.push_back(PIL_COMPRESS_ZSTD);
            ret += ret2;

            std::cerr << "done: " << ret << std::endl;
        } else {
            //std::cerr << "unknown cstore type" << std::endl;
            return(-1);
        }

        return(ret);
    }

    int Compress(const uint8_t* qual, const uint32_t n_src, uint32_t qlevel, RangeCoder* rc, FrequencyModel<QMAX>* model_qual) {
        uint32_t last = 0;
        int delta = 5;
        int i, len2 = n_src;
        int q1 = 0, q2 = 0;
        // Todo: fix illumina hack
        bool illumina_fastq = true;
        int val0 = illumina_fastq ? 59 : 33;

        /* Removing "Killer Bees" */
        while (len2 > 0 && qual[len2 - 1] == '#') len2--;

        for (i = 0; i < len2; i++) {
            //std::cerr << i << ": " << (char)qual[i] << "->" << (int)qual[i] << std::endl;
            const uint8_t q = (qual[i] - val0) & (QMAX - 1); // normalize quality value and mask out upper bits
            model_qual[last].EncodeSymbol(rc, q); // encode this symbol in RC

            // previous 2-3 bytes
            //if (QBITS == 12) {
                last = ((MAX(q1, q2) << 6) + q) & ((1 << QBITS) - 1);
            //} else {
            //    last = ((last << 6) + q) & ((1 << QBITS) - 1);
            //}

            //std::cerr << "first last=" << (int)last << std::endl;

            // Branchless tricks.
            if (qlevel > 1) {
                last  += (q1 == q2) << QBITS; // shift in equality between q1 and q2
                delta += (q1 > q) * (q1 - q); // if q1 > q then add the delta (q1 - q)
                last  += (MIN(7 * 8, delta) & 0xf8) << (QBITS - 2); // 0xf8 = 0b11111000
                //std::cerr << "q1 last=" << (int)last << " delta=" << (int)delta << std::endl;
            }

            if (qlevel > 2) {
                last += (MIN(i + 15, 127) & (15 << 3)) << (QBITS + 1);
                //std::cerr << "q2 add=" << (int)(MIN(i + 15, 127) & (15 << 3)) << " and shift= " << (int)(QBITS+1) << std::endl;
                //std::cerr << "q2 total=" << (int)((MIN(i + 15, 127) & (15 << 3)) << (QBITS + 1)) << "/" << (int)(QSIZE*16) << std::endl;
                //std::cerr << "q2 last=" << (int)last << std::endl;
            }

            _mm_prefetch((const char *)&model_qual[last], _MM_HINT_T0);
            q2 = q1; q1 = q;

            //std::cerr << (int)last << "/" << (QSIZE*16) << std::endl;
            assert(last < (QSIZE*16));
        }

        if (n_src != len2)
            model_qual[last].EncodeSymbol(rc, QMAX-1); /* terminator */

        return(1);
    }

    int Compress(const uint8_t* qual, const uint32_t n_src, const uint32_t* lengths, const uint32_t n_lengths) {
        if(buffer.get() == nullptr) {
            assert(AllocateResizableBuffer(pool_, n_src + 16384, &buffer) == 1);
        }

        if(buffer->capacity() < n_src + 16384){
            //std::cerr << "here in limit=" << n*sizeof(T) << "/" << buffer->capacity() << std::endl;
            assert(buffer->Reserve(n_src + 16384) == 1);
        }

        int qlevel = 2; // should be parameterized
        int qsize = QSIZE;
        if (qlevel > 1) qsize *= 16;
        if (qlevel > 2) qsize *= 16;
        FrequencyModel<QMAX>* model_qual = new FrequencyModel<QMAX>[qsize];

        RangeCoder rc;
        rc.StartEncode();
        rc.output(reinterpret_cast<char*>(buffer->mutable_data()));

        uint32_t cum_offset = 0;
        for(int i = 0; i < n_lengths; ++i){
            Compress(&qual[cum_offset], lengths[i], qlevel, &rc, model_qual);
            cum_offset += lengths[i];
        }

        rc.FinishEncode();
        std::cerr << "qual encodings=" << n_src << "->" << rc.size_out() << " (" << (float)n_src/rc.size_out() << "-fold)" << std::endl;

        delete[] model_qual;

        return(rc.size_out());
    }
};

class SequenceCompressor : public Compressor {
public:
    int Compress(std::shared_ptr<ColumnSet> cset, PIL_CSTORE_TYPE cstore) {
        if(cset.get() == nullptr) return(-1);

        int ret = 0;
        if(cstore == PIL_CSTORE_COLUMN) {
            std::cerr << "in cstore col: COMPUTING BASES" << std::endl;
            for(int i = 0; i < cset->columns.size(); ++i) {
                uint32_t n_l = cset->columns[i]->n;
                int ret2 = Compress(cset->columns[i]->buffer->mutable_data(), cset->columns[i]->n, &n_l, 1);
                cset->columns[i]->transformations.push_back(PIL_COMPRESS_RC_BASES);
                cset->columns[i]->compressed_size = ret2;
                ret += ret2;
            }
        } else if(cstore == PIL_CSTORE_TENSOR) {
            std::cerr << "in cstore tensor: COMPUTING BASES" << std::endl;
            //std::cerr << "buffer believe=" << cset->columns[1]->buffer->size() << " and " << cset->columns[1]->uncompressed_size << " and n=" << cset->columns[0]->n << std::endl;
            std::cerr << "computing deltas in place" << std::endl;
            compute_deltas_inplace(reinterpret_cast<uint32_t*>(cset->columns[0]->buffer->mutable_data()),
                                   cset->columns[0]->n,
                                   0);

            cset->columns[0]->transformations.push_back(PIL_ENCODE_DELTA);

            int ret2 = Compress(cset->columns[1]->buffer->mutable_data(),
                            cset->columns[1]->uncompressed_size,
                            reinterpret_cast<uint32_t*>(cset->columns[0]->buffer->mutable_data()),
                            cset->columns[0]->n);

            cset->columns[1]->compressed_size = ret2;
            cset->columns[1]->transformations.push_back(PIL_COMPRESS_RC_BASES);
            cset->columns[0]->transformations.push_back(PIL_COMPRESS_ZSTD);

            ret += ret2;
            std::cerr << "done: " << ret << std::endl;
        } else {
            //std::cerr << "unknown cstore type" << std::endl;
            return(-1);
        }

        return(ret);
    }

    int Compress(const uint8_t* bases, const uint32_t n_src, const uint32_t* lengths, const uint32_t n_lengths) {
        if(buffer.get() == nullptr) {
            assert(AllocateResizableBuffer(pool_, n_src + 16384, &buffer) == 1);
        }

        if(buffer->capacity() < n_src + 16384){
            assert(buffer->Reserve(n_src + 16384) == 1);
        }

        // slevel in range [1,9);
        int slevel = 3; // Number of bases of sequence context.
        int NS = 7 + slevel;
        BaseModel<uint16_t>* model_seq16 = new BaseModel<uint16_t>[1 << (2 * NS)];

        int last, last2;

        const int NS_MASK = ((1 << (2*NS)) - 1);
        uint32_t cum_offset = 0;

        int L[256];
        /* ACGTN* */
        for (int i = 0; i < 256; i++) L[i] = 0;
        L['A'] = L['a'] = 0;
        L['C'] = L['c'] = 1;
        L['G'] = L['g'] = 2;
        L['T'] = L['t'] = 3;
        L['N'] = L['n'] = 4; // is this correct?

        RangeCoder rc;
        rc.StartEncode();
        rc.output(reinterpret_cast<char*>(buffer->mutable_data()));


        for(int i = 0; i < n_lengths; ++i) {
            /* Corresponds to a 12-mer word that doesn't occur in human genome. */
            last  = 0x7616c7 & NS_MASK;
            last2 = (0x2c6b62ff >> (32 - 2*NS)) & NS_MASK;

            for (int j = 0; j < lengths[i]; ++j) {
                unsigned char  b = L[(unsigned char)bases[cum_offset + j]];
                model_seq16[last].EncodeSymbol(&rc, b);
                last = (last*4 + b) & NS_MASK;
                //volatile int p = *(int *)&model_seq16[last];
                _mm_prefetch((const char *)&model_seq16[last], _MM_HINT_T0);
            }
            cum_offset += lengths[i];
        }

       rc.FinishEncode();
       std::cerr << "BASES encodings=" << n_src << "->" << rc.size_out() << " (" << (float)n_src/rc.size_out() << "-fold)" << std::endl;

       delete[] model_seq16;
       return(rc.size_out());
    }
};


}

#endif /* ALGORITHM_COMPRESSION_ZSTD_CODEC_H_ */
