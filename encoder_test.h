#ifndef ENCODER_TEST_H_
#define ENCODER_TEST_H_

#include "encoder.h"
#include <gtest/gtest.h>

namespace pil {

TEST(EncoderTests, ValidityEmptyList) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    ASSERT_EQ(true, Encoder::ValidTransformationOrder(ctypes));
}

TEST(EncoderTests, ValiditySingleList) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes = {PIL_COMPRESS_ZSTD};

    ASSERT_EQ(true, Encoder::ValidTransformationOrder(ctypes));
}

TEST(EncoderTests, ValidityDictCompress) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes = {PIL_ENCODE_DICT, PIL_COMPRESS_ZSTD};

    ASSERT_EQ(true, Encoder::ValidTransformationOrder(ctypes));
}

TEST(EncoderTests, ValidityDictCompressMultiple) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes = {PIL_ENCODE_DICT, PIL_COMPRESS_ZSTD, PIL_COMPRESS_RC_BASES};

    ASSERT_EQ(true, Encoder::ValidTransformationOrder(ctypes));
}

TEST(EncoderTests, ValidityIllegalDictMultiple) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes = {PIL_ENCODE_DICT, PIL_ENCODE_DICT};

    ASSERT_EQ(false, Encoder::ValidTransformationOrder(ctypes));
}

TEST(EncoderTests, ValidityIllegalDictOrder) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes = {PIL_COMPRESS_ZSTD, PIL_ENCODE_DICT};

    ASSERT_EQ(false, Encoder::ValidTransformationOrder(ctypes));
}

TEST(EncoderTests, ValidityIllegalDictOrder2) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes = {PIL_ENCODE_DICT, PIL_ENCODE_DELTA};

    ASSERT_EQ(false, Encoder::ValidTransformationOrder(ctypes));
}

TEST(EncoderTests, ValidityComplex) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes =
        {PIL_ENCODE_BASES_2BIT,PIL_ENCODE_CIGAR_NIBBLE,PIL_ENCODE_DELTA_DELTA,
         PIL_ENCODE_DICT, PIL_COMPRESS_RC_QUAL};

    ASSERT_EQ(true, Encoder::ValidTransformationOrder(ctypes));
}

TEST(EncoderTests, ValidityComplex2) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes =
        {PIL_ENCODE_BASES_2BIT,PIL_ENCODE_CIGAR_NIBBLE,PIL_ENCODE_DELTA_DELTA,
         PIL_ENCODE_DICT, PIL_COMPRESS_RC_QUAL, PIL_COMPRESS_RC_ILLUMINA_NAME,
         PIL_COMPRESS_RC_BASES};

    ASSERT_EQ(true, Encoder::ValidTransformationOrder(ctypes));
}

TEST(EncoderTests, ValidityIllegalDuplicate) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes =
        {PIL_ENCODE_BASES_2BIT,PIL_ENCODE_CIGAR_NIBBLE,PIL_ENCODE_DELTA_DELTA,
         PIL_ENCODE_DICT,PIL_ENCODE_DICT, PIL_COMPRESS_RC_QUAL};

    ASSERT_EQ(false, Encoder::ValidTransformationOrder(ctypes));
}

TEST(EncoderTests, ValidityIllegalAuto) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes =
        {PIL_COMPRESS_AUTO,PIL_ENCODE_DICT, PIL_COMPRESS_RC_QUAL};

    ASSERT_EQ(false, Encoder::ValidTransformationOrder(ctypes));
}

}

#endif /* ENCODER_TEST_H_ */
