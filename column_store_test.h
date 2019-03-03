#ifndef COLUMN_STORE_TEST_H_
#define COLUMN_STORE_TEST_H_

#include <gtest/gtest.h>
#include "column_store.h"

namespace pil {

TEST(ColumnStoreTests, Empty) {
    ColumnStoreBuilder<uint32_t> builder;
    ASSERT_EQ(0, builder.size());
    ASSERT_EQ(nullptr, builder.nullity.get());
}

TEST(ColumnStoreTests, Single) {
    ColumnStoreBuilder<uint32_t> builder;
    ASSERT_EQ(1, builder.Append(10));
    ASSERT_EQ(1, builder.size());
    ASSERT_EQ(1, builder.n_elements);
}

TEST(ColumnStoreTests, Nullity) {
    ColumnStoreBuilder<uint32_t> builder;
    for(int i = 0; i < 32; ++i){
        ASSERT_EQ(1, builder.AppendValidity(true, 0));
        ++builder.n_records; // hack to add increment number of records
    }
    ASSERT_EQ(1, builder.AppendValidity(false, 0));
    ++builder.n_records;
    for(int i = 0; i < 50; ++i) {
        ASSERT_EQ(1, builder.AppendValidity(true, 0));
        ++builder.n_records;
    }

    for(int i = 0; i < 32; ++i) ASSERT_EQ(true, builder.IsValid(i));
    ASSERT_EQ(false, builder.IsValid(32));
    for(int i = 33; i < 33+50; ++i) ASSERT_EQ(true, builder.IsValid(i));
}

TEST(ColumnStoreTests, NullityHuge) {
    ColumnStoreBuilder<uint32_t> builder;
    for(int i = 0; i < 500000; ++i){
        ASSERT_EQ(1, builder.AppendValidity(true, 0));
        ++builder.n_records; // hack to add increment number of records
    }
    ASSERT_EQ(1, builder.AppendValidity(false, 0));
    ++builder.n_records;
    for(int i = 0; i < 500000; ++i) {
        ASSERT_EQ(1, builder.AppendValidity(true, 0));
        ++builder.n_records;
    }

    for(int i = 0; i < 500000; ++i) ASSERT_EQ(true, builder.IsValid(i));
    ASSERT_EQ(false, builder.IsValid(500000));
    for(int i = 500001; i < 500001+500000; ++i) ASSERT_EQ(true, builder.IsValid(i));
}

TEST(ColumnStoreTests, RangeInsert) {
    ColumnStoreBuilder<uint32_t> builder;
    uint32_t vals[] = {241, 10, 9, 42, 137};
    ASSERT_EQ(1, builder.Append(&vals[0], 5));
    ASSERT_EQ(5, builder.size());
    ASSERT_EQ(5, builder.n_elements);
}

TEST(ColumnStoreTests, RangeInsertVector) {
    ColumnStoreBuilder<uint32_t> builder;
    std::vector<uint32_t> vals = {241, 10, 9, 42, 137};
    ASSERT_EQ(1, builder.Append(vals));
    ASSERT_EQ(5, builder.size());
    ASSERT_EQ(5, builder.n_elements);
}

TEST(ColumnStoreTests, RangeInsertArray) {
    ColumnStoreBuilder<uint32_t> builder;
    uint32_t vals[] = {241, 10, 9, 42, 137};
    ASSERT_EQ(1, builder.AppendArray(&vals[0], 5));
    ASSERT_EQ(1, builder.size());
    ASSERT_EQ(5, builder.n_elements);
}

TEST(ColumnStoreTests, RangeInsertVectorArray) {
    ColumnStoreBuilder<uint32_t> builder;
    std::vector<uint32_t> vals = {241, 10, 9, 42, 137};
    ASSERT_EQ(1, builder.AppendArray(vals));
    ASSERT_EQ(1, builder.size());
    ASSERT_EQ(5, builder.n_elements);
}

}



#endif /* COLUMN_STORE_TEST_H_ */
