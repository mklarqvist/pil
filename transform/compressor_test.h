#ifndef COMPRESSOR_TEST_H_
#define COMPRESSOR_TEST_H_

#include <vector>
#include <random>
#include <climits>
#include <algorithm>
#include <functional>
#include <memory> // static_ptr_cast

#include "compressor.h"
#include "encoder.h"
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

TEST(DeltaTests, EncodeDecodeColumn) {
    DeltaEncoder transformer;

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
    ASSERT_EQ(1, transformer.Encode(cset, field));
    ASSERT_EQ(1, cstore->transformation_args.size());

    // Make sure the MD5 checksum is not the same
    ASSERT_NE(0, memcmp(cset->columns[0]->md5_checksum, cstore->transformation_args.back()->md5_checksum, 16));

    ASSERT_EQ(1, transformer.UnsafePrefixSum(cstore, field));
    uint8_t md5[16]; memset(md5, 0, 16);
    Digest::GenerateMd5(cset->columns[0]->mutable_data(), cset->columns[0]->buffer.length(), md5);
    ASSERT_EQ(0, memcmp(cset->columns[0]->md5_checksum, md5, 16));
}

TEST(DeltaTests, EncodeDecodeColumnSetTensor) {
    DeltaEncoder transformer;

    DictionaryFieldType field;
    field.cstore = PIL_CSTORE_TENSOR;
    field.ptype  = PIL_TYPE_UINT32;

    std::shared_ptr<ColumnSet > cset = std::make_shared<ColumnSet>();
    std::shared_ptr<ColumnSetBuilderTensor<uint32_t> > builder = std::static_pointer_cast< ColumnSetBuilderTensor<uint32_t> >(cset);

    std::vector<uint32_t> x = {1, 2, 3, 4, 5};
    for(int i = 0; i < 10000; ++i){
        builder->Append(x);
    }
    ASSERT_EQ(2, cset->size());
    ASSERT_EQ(10001, cset->columns[0]->size());
    ASSERT_EQ(10000 * x.size(), cset->columns[1]->n_elements);

    cset->columns[0]->ComputeChecksum();
    cset->columns[1]->ComputeChecksum();
    ASSERT_EQ(1, transformer.Encode(cset, field));

    // Make sure the MD5 checksum is not the same
    ASSERT_NE(0, memcmp(cset->columns[0]->md5_checksum, cset->columns[0]->transformation_args.back()->md5_checksum, 16));

    ASSERT_EQ(1, transformer.PrefixSum(cset, field));
    uint8_t md5[16]; memset(md5, 0, 16);
    Digest::GenerateMd5(cset->columns[0]->mutable_data(), cset->columns[0]->buffer.length(), md5);
    ASSERT_EQ(0, memcmp(cset->columns[0]->md5_checksum, md5, 16));
}

TEST(DeltaTests, EncodeDecodeColumnSetColumnSplit) {
    DeltaEncoder transformer;

    DictionaryFieldType field;
    field.cstore = PIL_CSTORE_COLUMN;
    field.ptype  = PIL_TYPE_UINT32;

    std::shared_ptr<ColumnSet > cset = std::make_shared<ColumnSet>();
    std::shared_ptr<ColumnSetBuilder<uint32_t> > builder = std::static_pointer_cast< ColumnSetBuilder<uint32_t> >(cset);

    std::vector<uint32_t> x = {1, 2, 3, 4, 5};
    for(int i = 0; i < 10000; ++i){
        builder->Append(x);
    }
    ASSERT_EQ(x.size(), cset->size());
    for(int i = 0; i < x.size(); ++i) {
        ASSERT_EQ(10000, cset->columns[i]->size());
        cset->columns[i]->ComputeChecksum();
    }

    // Encode the set
    ASSERT_EQ(1, transformer.Encode(cset, field));

    // Make sure the MD5 checksum is not the same
    for(int i = 0; i < x.size(); ++i) {
        ASSERT_NE(0, memcmp(cset->columns[i]->md5_checksum, cset->columns[i]->transformation_args.back()->md5_checksum, 16));
    }

    ASSERT_EQ(1, transformer.PrefixSum(cset, field));
    uint8_t md5[16]; memset(md5, 0, 16);
    for(int i = 0; i < x.size(); ++i) {
        Digest::GenerateMd5(cset->columns[i]->mutable_data(), cset->columns[i]->buffer.length(), md5);
        ASSERT_EQ(0, memcmp(cset->columns[i]->md5_checksum, md5, 16));
    }
}

TEST(DeltaTests, EncodeDecodeColumnSetColumnSplitFail) {
    DeltaEncoder transformer;

    DictionaryFieldType field;
    field.cstore = PIL_CSTORE_TENSOR; // set to incorrect storage type
    field.ptype  = PIL_TYPE_UINT32;

    std::shared_ptr<ColumnSet > cset = std::make_shared<ColumnSet>();
    std::shared_ptr<ColumnSetBuilder<uint32_t> > builder = std::static_pointer_cast< ColumnSetBuilder<uint32_t> >(cset);

    std::vector<uint32_t> x = {1, 2, 3, 4, 5};
    for(int i = 0; i < 10000; ++i){
        builder->Append(x);
    }
    ASSERT_EQ(x.size(), cset->size());
    for(int i = 0; i < x.size(); ++i) {
        ASSERT_EQ(10000, cset->columns[i]->size());
        cset->columns[i]->ComputeChecksum();
    }

    // Attempt to encode the set and fail
    ASSERT_EQ(-4, transformer.Encode(cset, field));
}

TEST(QualityTests, EncodeDecode) {
    QualityCompressor transformer;

    DictionaryFieldType field;
    field.cstore = PIL_CSTORE_TENSOR;
    field.ptype  = PIL_TYPE_UINT32;

    std::shared_ptr<ColumnSet > cset = std::make_shared<ColumnSet>();
    std::shared_ptr<ColumnSetBuilderTensor<uint8_t> > builder = std::static_pointer_cast< ColumnSetBuilderTensor<uint8_t> >(cset);

    std::random_device rd; // obtain a random number from hardware
    std::mt19937 eng(rd()); // seed the generator
    std::uniform_int_distribution<uint8_t> distr(0, 66);
    std::vector<uint8_t> data(100);
    for(int i = 0; i < 10000; ++i) {
        for(int j = 0; j < 100; ++j) data[j] = distr(eng);
        ASSERT_EQ(1, builder->Append(data));
    }
    ASSERT_EQ(2, cset->size());

    ASSERT_EQ(10001, cset->columns[0]->size());
    ASSERT_EQ(10000 * 100, cset->columns[1]->n_elements);

    cset->columns[0]->ComputeChecksum();
    cset->columns[1]->ComputeChecksum();
    ASSERT_GT(transformer.Compress(cset, field.cstore), 0);

    // Make sure the MD5 checksum is not the same
    ASSERT_NE(0, memcmp(cset->columns[1]->md5_checksum, cset->columns[1]->transformation_args.back()->md5_checksum, 16));

    transformer.Decompress(cset, field.cstore);
    uint8_t md5[16]; memset(md5, 0, 16);
    Digest::GenerateMd5(cset->columns[1]->mutable_data(), cset->columns[1]->buffer.length(), md5);
    ASSERT_EQ(0, memcmp(cset->columns[1]->md5_checksum, md5, 16));
}

TEST(QualityTests, EncodeDecodeLong100kb) {
    QualityCompressor transformer;

    DictionaryFieldType field;
    field.cstore = PIL_CSTORE_TENSOR;
    field.ptype  = PIL_TYPE_UINT32;

    std::shared_ptr<ColumnSet > cset = std::make_shared<ColumnSet>();
    std::shared_ptr<ColumnSetBuilderTensor<uint8_t> > builder = std::static_pointer_cast< ColumnSetBuilderTensor<uint8_t> >(cset);

    std::random_device rd; // obtain a random number from hardware
    std::mt19937 eng(rd()); // seed the generator
    std::uniform_int_distribution<uint8_t> distr(0, 66);
    std::vector<uint8_t> data(100000);
    for(int i = 0; i < 50; ++i) {
        for(int j = 0; j < 100000; ++j) data[j] = distr(eng);
        ASSERT_EQ(1, builder->Append(data));
    }
    ASSERT_EQ(2, cset->size());

    ASSERT_EQ(51, cset->columns[0]->size());
    ASSERT_EQ(50 * 100000, cset->columns[1]->n_elements);

    cset->columns[0]->ComputeChecksum();
    cset->columns[1]->ComputeChecksum();
    ASSERT_GT(transformer.Compress(cset, field.cstore), 0);

    // Make sure the MD5 checksum is not the same
    ASSERT_NE(0, memcmp(cset->columns[1]->md5_checksum, cset->columns[1]->transformation_args.back()->md5_checksum, 16));

    transformer.Decompress(cset, field.cstore);
    uint8_t md5[16]; memset(md5, 0, 16);
    Digest::GenerateMd5(cset->columns[1]->mutable_data(), cset->columns[1]->buffer.length(), md5);
    ASSERT_EQ(0, memcmp(cset->columns[1]->md5_checksum, md5, 16));
}

TEST(QualityTests, EncodeDecodeLong1mb) {
    QualityCompressor transformer;

    DictionaryFieldType field;
    field.cstore = PIL_CSTORE_TENSOR;
    field.ptype  = PIL_TYPE_UINT32;

    std::shared_ptr<ColumnSet > cset = std::make_shared<ColumnSet>();
    std::shared_ptr<ColumnSetBuilderTensor<uint8_t> > builder = std::static_pointer_cast< ColumnSetBuilderTensor<uint8_t> >(cset);

    std::random_device rd; // obtain a random number from hardware
    std::mt19937 eng(rd()); // seed the generator
    std::uniform_int_distribution<uint8_t> distr(0, 66);
    std::vector<uint8_t> data(1000000);
    for(int i = 0; i < 2; ++i) {
        for(int j = 0; j < 1000000; ++j) data[j] = distr(eng);
        ASSERT_EQ(1, builder->Append(data));
    }
    ASSERT_EQ(2, cset->size());

    ASSERT_EQ(3, cset->columns[0]->size());
    ASSERT_EQ(2 * 1000000, cset->columns[1]->n_elements);

    cset->columns[0]->ComputeChecksum();
    cset->columns[1]->ComputeChecksum();
    ASSERT_GT(transformer.Compress(cset, field.cstore), 0);

    // Make sure the MD5 checksum is not the same
    ASSERT_NE(0, memcmp(cset->columns[1]->md5_checksum, cset->columns[1]->transformation_args.back()->md5_checksum, 16));

    transformer.Decompress(cset, field.cstore);
    uint8_t md5[16]; memset(md5, 0, 16);
    Digest::GenerateMd5(cset->columns[1]->mutable_data(), cset->columns[1]->buffer.length(), md5);
    ASSERT_EQ(0, memcmp(cset->columns[1]->md5_checksum, md5, 16));
}

TEST(SeqTests, EncodeDecode) {
    SequenceCompressor transformer;

    DictionaryFieldType field;
    field.cstore = PIL_CSTORE_TENSOR;
    field.ptype  = PIL_TYPE_UINT32;

    std::shared_ptr<ColumnSet > cset = std::make_shared<ColumnSet>();
    std::shared_ptr<ColumnSetBuilderTensor<uint8_t> > builder = std::static_pointer_cast< ColumnSetBuilderTensor<uint8_t> >(cset);

    std::random_device rd; // obtain a random number from hardware
    std::mt19937 eng(rd()); // seed the generator
    std::uniform_int_distribution<uint8_t> distr(0, 4); // right inclusive
    std::vector<uint8_t> data(100);
    char map[] = {'A', 'T', 'G', 'C', 'N'};
    for(int i = 0; i < 10000; ++i) {
        for(int j = 0; j < 100; ++j) data[j] = map[distr(eng)];
        if(i < 2) {
            for(int j = 0; j < 100; ++j) std::cerr << data[j];
            std::cerr << std::endl;
        }
        ASSERT_EQ(1, builder->Append(data));
    }
    ASSERT_EQ(2, cset->size());

    ASSERT_EQ(10001, cset->columns[0]->size());
    ASSERT_EQ(10000 * 100, cset->columns[1]->n_elements);

    cset->columns[0]->ComputeChecksum();
    cset->columns[1]->ComputeChecksum();
    ASSERT_GT(transformer.Compress(cset, field.cstore), 0);

    // Make sure the MD5 checksum is not the same
    ASSERT_NE(0, memcmp(cset->columns[1]->md5_checksum, cset->columns[1]->transformation_args.back()->md5_checksum, 16));

    transformer.Decompress(cset, field);
    uint8_t md5[16]; memset(md5, 0, 16);
    Digest::GenerateMd5(cset->columns[1]->mutable_data(), cset->columns[1]->buffer.length(), md5);
    ASSERT_EQ(0, memcmp(cset->columns[1]->md5_checksum, md5, 16));
}

}


#endif /* COMPRESSOR_TEST_H_ */
