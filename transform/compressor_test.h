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

TEST(ZstdTests, CompressColumnRandom) {
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
    ASSERT_EQ(1, cstore->transformation_args.size());

    // Make sure the MD5 checksum is not 0
    uint8_t bad_md5[16]; memset(bad_md5, 0, 16);
    ASSERT_NE(0, memcmp(cset->columns[0]->mutable_data(), bad_md5, 16));
}

TEST(ZstdTests, CompressColumnUniformSingle) {
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
    ASSERT_EQ(1, cstore->transformation_args.size());

    // Make sure the MD5 checksum is not 0
    uint8_t bad_md5[16]; memset(bad_md5, 0, 16);
    ASSERT_NE(0, memcmp(cset->columns[0]->mutable_data(), bad_md5, 16));
}

TEST(ZstdTests, CompressDecompressColumnRandomUnsafe) {
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

    cstore->ComputeChecksum();
    ASSERT_GT(zstd.Compress(cset, field), 0);
    ASSERT_EQ(1, cstore->transformation_args.size());

    // Make sure the MD5 checksum is not 0
    uint8_t md5[16]; memset(md5, 0, 16);
    ASSERT_NE(0, memcmp(cset->columns[0]->mutable_data(), md5, 16));

    ASSERT_GT(zstd.UnsafeDecompress(cstore, true), 0);
    Digest::GenerateMd5(cstore->mutable_data(), cstore->uncompressed_size, md5);
    ASSERT_EQ(0, memcmp(md5, cstore->md5_checksum, 16));
}

TEST(ZstdTests, CompressDecompressColumnRandomSafe) {
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

    cstore->ComputeChecksum();
    ASSERT_GT(zstd.Compress(cset, field), 0);
    ASSERT_EQ(1, cstore->transformation_args.size());

    // Make sure the MD5 checksum is not 0
    uint8_t md5[16]; memset(md5, 0, 16);
    ASSERT_NE(0, memcmp(cset->columns[0]->mutable_data(), md5, 16));

    // Different compressor with empty buffer.
    ZstdCompressor zstd2;

    ASSERT_GT(zstd2.Decompress(cstore, cstore->transformation_args.back(), true), 0);
    Digest::GenerateMd5(cstore->mutable_data(), cstore->uncompressed_size, md5);
    ASSERT_EQ(0, memcmp(md5, cstore->md5_checksum, 16));
}

TEST(ZstdTests, CompressDecompressColumnRandomSafeIncorrectTyping) {
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

    cstore->ComputeChecksum();
    ASSERT_GT(zstd.Compress(cset, field), 0);
    ASSERT_EQ(1, cstore->transformation_args.size());

    // Make sure the MD5 checksum is not 0
    uint8_t md5[16]; memset(md5, 0, 16);
    ASSERT_NE(0, memcmp(cset->columns[0]->mutable_data(), md5, 16));

    // Different compressor with empty buffer.
    ZstdCompressor zstd2;
    // Corrupt typing
    cstore->transformation_args.back()->ctype = PIL_COMPRESS_NONE;

    // Throw error because ctype is not PIL_COMPRESS_ZSTD
    ASSERT_EQ(-1, zstd2.Decompress(cstore, cstore->transformation_args.back(), true));
}

TEST(DeltaTests, EncodeDecode) {
    Transformer transformer;

    DictionaryFieldType field;
    field.cstore = PIL_CSTORE_COLUMN;
    field.ptype  = PIL_TYPE_UINT32;

    std::shared_ptr<ColumnStore > cstore = std::make_shared<ColumnStore>();
    std::shared_ptr<ColumnStoreBuilder<uint32_t> > builder = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(cstore);
    for(int i = 0; i < 10000; ++i) builder->Append(101*(i+1));
    ASSERT_EQ(10000, cstore->size());
    std::shared_ptr<ColumnSet> cset = std::make_shared<ColumnSet>();
    ASSERT_EQ(1, cset->Append(builder));

    cstore->ComputeChecksum();
    ASSERT_EQ(1, transformer.DeltaEncode(cset, field));
    ASSERT_EQ(1, cstore->transformation_args.size());

    // Make sure the MD5 checksum is not the same
    ASSERT_NE(0, memcmp(cset->columns[0]->md5_checksum, cstore->transformation_args.back()->md5_checksum, 16));

}

}


#endif /* COMPRESSOR_TEST_H_ */
