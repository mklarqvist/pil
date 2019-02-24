#ifndef PIL_H_
#define PIL_H_

#include <string>

namespace pil {

/*------ Core enums --------*/
typedef enum {
    PIL_TYPE_UNKNOWN,
    PIL_TYPE_INT8,
    PIL_TYPE_UINT8,
    PIL_TYPE_INT16,
    PIL_TYPE_UINT16,
    PIL_TYPE_INT32,
    PIL_TYPE_UINT32,
    PIL_TYPE_INT64,
    PIL_TYPE_UINT64,
    PIL_TYPE_FLOAT,
    PIL_TYPE_DOUBLE,
    PIL_TYPE_BOOLEAN,
    PIL_TYPE_BYTE_ARRAY, // these must be followed by a second PIL_PRIMITIVE_TYPE in their implementation
    PIL_TYPE_FIXED_LEN_BYTE_ARRAY
} PIL_PRIMITIVE_TYPE;

const std::string PIL_PRIMITIVE_TYPE_STRING[14] = {
        "UNKNOWN",
        "INT8", "UINT8", "INT16", "UINT16", "INT32", "UINT32", "INT64", "UINT64",
        "FLOAT", "DOUBLE",
        "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"
};

typedef enum {
    PIL_CSTORE_UNKNOWN,
    PIL_CSTORE_TENSOR,
    PIL_CSTORE_COLUMN
} PIL_CSTORE_TYPE;

const std::string PIL_CSTORE_TYPE_STRING[3] = {"UNKNOWN", "TENSOR", "COLUMN-SPLIT"};

typedef enum {
    PIL_COMPRESS_AUTO, /** Automatic compression **/
    PIL_COMPRESS_ZSTD, /** ZSTD compression **/
    PIL_COMPRESS_NONE, /** Leave data uncompressed **/
    PIL_COMPRESS_RC_QUAL, /** Range codec with models for sequence quality scores **/
    PIL_COMPRESS_RC_BASES, /** Range codec with models for sequence bases **/
    PIL_COMPRESS_RC_ILLUMINA_NAME, /** Range codec models for Illumina sequence names **/
    PIL_ENCODE_DICT, /** Dictionary encoding **/
    PIL_ENCODE_DELTA, /** Delta encoding of arithmetic progression - requires uint32_t **/
    PIL_ENCODE_DELTA_DELTA, /** Delta of deltas **/
    PIL_ENCODE_BASES_2BIT, /** 2-bit encoding of sequence bases with additional mask **/
    PIL_ENCODE_CIGAR_NIBBLE /** Nibble-encoding of CIGAR strings **/
} PIL_COMPRESSION_TYPE;

}

#endif /* PIL_H_ */
