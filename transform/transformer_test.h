#ifndef TRANSFORMER_TEST_H_
#define TRANSFORMER_TEST_H_

#include "transformer.h"
#include <gtest/gtest.h>

namespace pil {

TEST(TransformerTests, ValidityEmptyList) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    ASSERT_EQ(true, Transformer::ValidTransformationOrder(ctypes));
}

TEST(TransformerTests, ValiditySingleList) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes = {PIL_COMPRESS_ZSTD};

    ASSERT_EQ(true, Transformer::ValidTransformationOrder(ctypes));
}

TEST(TransformerTests, ValidityDictCompress) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes = {PIL_ENCODE_DICT, PIL_COMPRESS_ZSTD};

    ASSERT_EQ(true, Transformer::ValidTransformationOrder(ctypes));
}

TEST(TransformerTests, ValidityDictCompressMultiple) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes = {PIL_ENCODE_DICT, PIL_COMPRESS_ZSTD, PIL_COMPRESS_RC_BASES};

    ASSERT_EQ(true, Transformer::ValidTransformationOrder(ctypes));
}

TEST(TransformerTests, ValidityIllegalDictMultiple) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes = {PIL_ENCODE_DICT, PIL_ENCODE_DICT};

    ASSERT_EQ(false, Transformer::ValidTransformationOrder(ctypes));
}

TEST(TransformerTests, ValidityIllegalDictOrder) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes = {PIL_COMPRESS_ZSTD, PIL_ENCODE_DICT};

    ASSERT_EQ(false, Transformer::ValidTransformationOrder(ctypes));
}

TEST(TransformerTests, ValidityIllegalDictOrder2) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes = {PIL_ENCODE_DICT, PIL_ENCODE_DELTA};

    ASSERT_EQ(false, Transformer::ValidTransformationOrder(ctypes));
}

TEST(TransformerTests, ValidityComplex) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes =
        {PIL_ENCODE_BASES_2BIT,PIL_ENCODE_DELTA_DELTA,
         PIL_ENCODE_DICT, PIL_COMPRESS_RC_QUAL};

    ASSERT_EQ(true, Transformer::ValidTransformationOrder(ctypes));
}

TEST(TransformerTests, ValidityComplex2) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes =
        {PIL_ENCODE_BASES_2BIT,PIL_ENCODE_DELTA_DELTA,
         PIL_ENCODE_DICT, PIL_COMPRESS_RC_QUAL, PIL_COMPRESS_RC_ILLUMINA_NAME,
         PIL_COMPRESS_RC_BASES};

    ASSERT_EQ(true, Transformer::ValidTransformationOrder(ctypes));
}

TEST(TransformerTests, ValidityIllegalDuplicate) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes =
        {PIL_ENCODE_BASES_2BIT,PIL_ENCODE_DELTA_DELTA,
         PIL_ENCODE_DICT,PIL_ENCODE_DICT, PIL_COMPRESS_RC_QUAL};

    ASSERT_EQ(false, Transformer::ValidTransformationOrder(ctypes));
}

TEST(TransformerTests, ValidityIllegalAuto) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes =
        {PIL_COMPRESS_AUTO,PIL_ENCODE_DICT, PIL_COMPRESS_RC_QUAL};

    ASSERT_EQ(false, Transformer::ValidTransformationOrder(ctypes));
}

}

#endif /* TRANSFORMER_TEST_H_ */
