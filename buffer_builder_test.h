#ifndef BUFFER_BUILDER_TEST_H_
#define BUFFER_BUILDER_TEST_H_

#include "buffer_builder.h"
#include <gtest/gtest.h>

namespace pil {

TEST(BufferBuilderTests, EmptyAllocation) {
    BufferBuilder builder;

    ASSERT_EQ(0, builder.length());
    ASSERT_EQ(0, builder.capacity());
}

TEST(BufferBuilderTests, SingleAddition) {
    BufferBuilder builder;

    uint64_t val = 451;
    ASSERT_EQ(1, builder.Append(reinterpret_cast<uint8_t*>(&val), sizeof(uint64_t)));
    ASSERT_EQ(sizeof(uint64_t), builder.length());
    const uint64_t* data = reinterpret_cast<const uint64_t*>(builder.data());
    ASSERT_EQ(451, data[0]);
}

TEST(BufferBuilderTests, MultipleAdditions) {
    BufferBuilder builder;

    for(int i = 0; i < 100; ++i) {
        uint64_t val = 451 + i;
        ASSERT_EQ(1, builder.Append(reinterpret_cast<uint8_t*>(&val), sizeof(uint64_t)));
        ASSERT_EQ(sizeof(uint64_t)*(i+1), builder.length());
    }
    ASSERT_EQ(sizeof(uint64_t)*100, builder.length());

    const uint64_t* data = reinterpret_cast<const uint64_t*>(builder.data());
    for(int i = 0; i < 100; ++i) {
        ASSERT_EQ(451 + i, data[i]);
    }
}

TEST(BufferBuilderTests, ArrayAddition) {
    BufferBuilder builder;

    std::vector<uint64_t> vals = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    ASSERT_EQ(1, builder.Append(reinterpret_cast<uint8_t*>(vals.data()), sizeof(uint64_t)*vals.size()));
    ASSERT_EQ(sizeof(uint64_t)*vals.size(), builder.length());
    const uint64_t* data = reinterpret_cast<const uint64_t*>(builder.data());
    for(size_t i = 0; i < vals.size(); ++i) ASSERT_EQ(i+1, data[i]);
}

TEST(BufferBuilderTests, ArrayAdditionUnsafeShrink) {
    BufferBuilder builder;

    std::vector<uint64_t> vals = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    ASSERT_EQ(1, builder.Append(reinterpret_cast<uint8_t*>(vals.data()), sizeof(uint64_t)*vals.size()));
    ASSERT_EQ(sizeof(uint64_t)*vals.size(), builder.length());
    builder.UnsafeSetLength(sizeof(uint64_t)*4);
    ASSERT_EQ(sizeof(uint64_t)*4, builder.length());
}

TEST(BufferBuilderTests, ArrayAdditionUnsafeShrinkAddition) {
    BufferBuilder builder;

    std::vector<uint64_t> vals = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    ASSERT_EQ(1, builder.Append(reinterpret_cast<uint8_t*>(vals.data()), sizeof(uint64_t)*vals.size()));
    ASSERT_EQ(sizeof(uint64_t)*vals.size(), builder.length());
    builder.UnsafeSetLength(sizeof(uint64_t)*4);
    ASSERT_EQ(sizeof(uint64_t)*4, builder.length());
    vals = {35};
    ASSERT_EQ(1, builder.Append(reinterpret_cast<uint8_t*>(vals.data()), sizeof(uint64_t)*vals.size()));
    ASSERT_EQ(sizeof(uint64_t)*5, builder.length());

    const uint64_t* data = reinterpret_cast<const uint64_t*>(builder.data());
    ASSERT_EQ(35, data[4]);
}

}


#endif /* BUFFER_BUILDER_TEST_H_ */
