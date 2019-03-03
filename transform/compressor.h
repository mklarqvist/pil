#ifndef ALGORITHM_COMPRESSION_ZSTD_CODEC_H_
#define ALGORITHM_COMPRESSION_ZSTD_CODEC_H_

#include <cstdint>

#include "zstd.h"
#include "zstd_errors.h"

#include "transformer.h"
#include "base_model.h"
#include "frequency_model.h"

#define PIL_ZSTD_DEFAULT_LEVEL 1

namespace pil {

class Compressor : public Transformer {
public:
    Compressor(){}
    Compressor(std::shared_ptr<ResizableBuffer> data) : Transformer(data){}
    ~Compressor(){}

    inline std::shared_ptr<ResizableBuffer> data() const { return(buffer); }
};

class ZstdCompressor : public Compressor {
private:
	typedef        ZstdCompressor self_type;
	typedef struct ZSTD_CCtx_s    ZSTD_CCtx;
	typedef struct ZSTD_DCtx_s    ZSTD_DCtx;

public:
	int Compress(std::shared_ptr<ColumnSet> cset, const PIL_CSTORE_TYPE& field_type, const int compression_level = 1);
	int Compress(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field, const int compression_level = 1);
	int Compress(const uint8_t* src, const uint32_t n_src, const int compression_level = 1);
    int Decompress(){ return 1; }
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
    int Compress(std::shared_ptr<ColumnSet> cset, PIL_CSTORE_TYPE cstore);

    int Compress(const uint8_t* qual,
                 const uint32_t n_src,
                 uint32_t qlevel,
                 RangeCoder* rc,
                 FrequencyModel<QMAX>* model_qual);

    int Compress(uint8_t* qual, const uint32_t n_src, const uint32_t* lengths, const uint32_t n_lengths);
};

class SequenceCompressor : public Compressor {
public:
    int Compress(std::shared_ptr<ColumnSet> cset, PIL_CSTORE_TYPE cstore);
    int Compress(const uint8_t* bases, const uint32_t n_src, const uint32_t* lengths, const uint32_t n_lengths);
};


}

#endif /* ALGORITHM_COMPRESSION_ZSTD_CODEC_H_ */
