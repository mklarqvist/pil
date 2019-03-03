#ifndef COMPRESSOR_TEST_H_
#define COMPRESSOR_TEST_H_

#include <vector>
#include <random>
#include <climits>
#include <algorithm>
#include <functional>
#include <memory> // static_ptr_cast

#include "compressor.h"
#include <gtest/gtest.h>

namespace pil {

// METHOD compress empty
// METHOD compress
// METHOD compress bad size
// METHOD decompress empty
// METHOD decompress
// METHOD decompress corrupted
// METHOD decompress bad size

using random_bytes_engine = std::independent_bits_engine<std::default_random_engine, CHAR_BIT, unsigned char>;

TEST(ZstdTests, CompressRandom) {
    ZstdCompressor zstd;

    std::shared_ptr<ColumnStore > cstore = std::make_shared<ColumnStore>();
    std::shared_ptr<ColumnStoreBuilder<uint8_t> > builder = std::static_pointer_cast< ColumnStoreBuilder<uint8_t> >(cstore);
    random_bytes_engine rbe;
    std::vector<uint8_t> data(10000);
    std::generate(begin(data), end(data), std::ref(rbe));
    ASSERT_EQ(1, builder->Append(data));
    ASSERT_EQ(data.size(), cstore->size());

    std::shared_ptr<ColumnSet> cset = std::make_shared<ColumnSet>();
    ASSERT_EQ(1, cset->Append(builder));

    DictionaryFieldType field;
    field.cstore = PIL_CSTORE_COLUMN;
    field.ptype  = PIL_TYPE_UINT8;

    ASSERT_GT(zstd.Compress(cset, field), 0);
}

TEST(ZstdTests, CompressUniformSingle) {
    ZstdCompressor zstd;

    std::shared_ptr<ColumnStore > cstore = std::make_shared<ColumnStore>();
    std::shared_ptr<ColumnStoreBuilder<uint8_t> > builder = std::static_pointer_cast< ColumnStoreBuilder<uint8_t> >(cstore);

    for(int i = 0; i < 10000; ++i) ASSERT_EQ(1, builder->Append(201));
    ASSERT_EQ(10000, cstore->size());

    std::shared_ptr<ColumnSet> cset = std::make_shared<ColumnSet>();
    ASSERT_EQ(1, cset->Append(builder));

    DictionaryFieldType field;
    field.cstore = PIL_CSTORE_COLUMN;
    field.ptype  = PIL_TYPE_UINT8;

    ASSERT_GT(zstd.Compress(cset, field), 0);
}

}


#endif /* COMPRESSOR_TEST_H_ */
