#ifndef RECORD_BUILDER_TEST_H_
#define RECORD_BUILDER_TEST_H_

#include "record_builder.h"
#include <gtest/gtest.h>

namespace pil {

TEST(RecordBuilderTests, AddUnknown) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(-1, rbuild.Add("FIELD-1", PIL_TYPE_UNKNOWN, (uint8_t)3));
}

TEST(RecordBuilderTests, AddArraySingleSlot) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(-1, rbuild.Add("FIELD-1", PIL_TYPE_BYTE_ARRAY, (uint8_t)3));
}

TEST(RecordBuilderTests, AddFixedArraySingleSlot) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(-1, rbuild.Add("FIELD-1", PIL_TYPE_FIXED_LEN_BYTE_ARRAY, (uint8_t)3));
}

TEST(RecordBuilderTests, AddUINT8) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_UINT8, (uint8_t)3));
}

TEST(RecordBuilderTests, AddINT8) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_INT8, (int8_t)3));
}

TEST(RecordBuilderTests, AddUINT16) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_UINT16, (uint16_t)3));
}

TEST(RecordBuilderTests, AddINT16) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_INT16, (int16_t)3));
}

TEST(RecordBuilderTests, AddUINT32) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_UINT32, (uint32_t)3));
}

TEST(RecordBuilderTests, AddINT32) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_INT32, (int32_t)3));
}

TEST(RecordBuilderTests, AddUINT64) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_UINT64, (uint64_t)3));
}

TEST(RecordBuilderTests, AddINT64) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_INT64, (int64_t)3));
}

TEST(RecordBuilderTests, AddFLOAT) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_FLOAT, (float)3));
}

TEST(RecordBuilderTests, AddDOUBLE) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_DOUBLE, (double)3));
}


TEST(RecordBuilderTests, AddColumns) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    uint32_t vals[3] = {1,2,3};
    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_UINT32, vals, 3));
}

TEST(RecordBuilderTests, AddColumnsMany) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    uint32_t vals[40] = {1,2,3,4,1,1,1,111,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1};
    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_UINT32, vals, 40));
}

TEST(RecordBuilderTests, AddColumnsVector) {
    //Encoder encoder;
    std::vector<PIL_COMPRESSION_TYPE> ctypes;

    RecordBuilder rbuild;
    std::vector<uint32_t> vals = {1,2,3,4,1,1,1,111,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1};

    ASSERT_EQ(1, rbuild.Add("FIELD-1", PIL_TYPE_UINT32, vals));
}

}


#endif /* RECORD_BUILDER_TEST_H_ */
