#ifndef COLUMNSTORE_TEST_H_
#define COLUMNSTORE_TEST_H_

#include "columnstore.h"
#include <gtest/gtest.h>

namespace pil {

TEST(ColumnStoreTests, Empty) {
    ColumnStoreBuilder<uint32_t> builder;
    ASSERT_EQ(0, builder.size());
}

TEST(ColumnStoreTests, Single) {
    ColumnStoreBuilder<uint32_t> builder;
    ASSERT_EQ(1, builder.Append(10));
}

TEST(ColumnStoreTests, RangeInsert) {
    ColumnStoreBuilder<uint32_t> builder;
    uint32_t vals[] = {241, 10, 9, 42, 137};
    ASSERT_EQ(1, builder.Append(&vals[0], 5));
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

}



#endif /* COLUMNSTORE_TEST_H_ */
