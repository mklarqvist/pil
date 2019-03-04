#ifndef ALGORITHM_COMPRESSION_ZSTD_CODEC_H_
#define ALGORITHM_COMPRESSION_ZSTD_CODEC_H_

#include <cstdint>

#include "zstd.h"
#include "zstd_errors.h"


#include "fqzcomp_qual.h"

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
    /**<
     * Decompression requires that `compressed_size` is properly set to the
     * correct value. Unsafe method as we do not know the uncompressed size
     * or the correctness of decompressor.
     * @param cstore
     * @param back_copy
     * @return
     */
    int UnsafeDecompress(std::shared_ptr<ColumnStore> cstore, const bool back_copy = true);

    /**<
     * Safe decompression of the target ColumnStore.
     * @param cstore
     * @param meta
     * @param back_copy
     * @return
     */
    int Decompress(std::shared_ptr<ColumnStore> cstore, std::shared_ptr<TransformMeta> meta, const bool back_copy = true);
};


#define QMAX 64
/* QBITS is the size of the quality context, 2x quals */
#define QBITS 12
#define QSIZE (1 << QBITS)

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
    int Decompress(){ return -1; }

    int Compress2(std::shared_ptr<ColumnSet> cset, PIL_CSTORE_TYPE cstore);
    int Decompress2(std::shared_ptr<ColumnSet> cset, PIL_CSTORE_TYPE cstore);
};

class SequenceCompressor : public Compressor {
public:
    int Compress(std::shared_ptr<ColumnSet> cset, PIL_CSTORE_TYPE cstore);
    int Compress(const uint8_t* bases, const uint32_t n_src, const uint32_t* lengths, const uint32_t n_lengths);
    int Decompress(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field);
};


}

#endif /* ALGORITHM_COMPRESSION_ZSTD_CODEC_H_ */
