#ifndef DICTIONARY_BUILDER_TEST_H_
#define DICTIONARY_BUILDER_TEST_H_

#include "dictionary_builder.h"
#include <gtest/gtest.h>

namespace pil {

TEST(DictionaryBuilderTests, FindSingleInt) {
    std::shared_ptr< ColumnSetBuilder<uint32_t> > cbuild = std::make_shared< ColumnSetBuilder<uint32_t> >();

    cbuild->Append(10);

    NumericDictionaryBuilder<uint32_t> dict;
    ASSERT_EQ(1, dict.Encode(cbuild->columns[0], true));
    ASSERT_EQ(1, dict.NumberRecords());
    ASSERT_EQ(1, dict.Contains<uint32_t>(10));
}

TEST(DictionaryBuilderTests, FindSingleIntFail) {
    std::shared_ptr< ColumnSetBuilder<uint32_t> > cbuild = std::make_shared< ColumnSetBuilder<uint32_t> >();

    cbuild->Append(10);

    NumericDictionaryBuilder<uint32_t> dict;
    ASSERT_EQ(1, dict.Encode(cbuild->columns[0], true));
    ASSERT_EQ(1, dict.NumberRecords());
    ASSERT_EQ(0, dict.Contains<uint32_t>(5));
}

TEST(DictionaryBuilderTests, FindSingleLargeSet) {
    std::shared_ptr< ColumnSetBuilder<uint32_t> > cbuild = std::make_shared< ColumnSetBuilder<uint32_t> >();

    for(int i = 0; i < 1000; ++i)
        cbuild->Append(i);

    NumericDictionaryBuilder<uint32_t> dict;
    ASSERT_EQ(1, dict.Encode(cbuild->columns[0], true));
    ASSERT_EQ(1000, dict.NumberRecords());
    ASSERT_EQ(1, dict.Contains<uint32_t>(541));
}

TEST(DictionaryBuilderTests, FindSingleLargeSetFail) {
    std::shared_ptr< ColumnSetBuilder<uint32_t> > cbuild = std::make_shared< ColumnSetBuilder<uint32_t> >();

    for(int i = 0; i < 1000; ++i)
        cbuild->Append(i);

    NumericDictionaryBuilder<uint32_t> dict;
    ASSERT_EQ(1, dict.Encode(cbuild->columns[0], true));
    ASSERT_EQ(1000, dict.NumberRecords());
    ASSERT_EQ(0, dict.Contains<uint32_t>(2999));
}

TEST(DictionaryBuilderTests, FindSingleIntTensorAny) {
    std::shared_ptr< ColumnSetBuilderTensor<uint32_t> > cbuild = std::make_shared< ColumnSetBuilderTensor<uint32_t> >();

    std::vector<uint32_t> vals = {10,12,14,21};
    cbuild->Append(vals.data(), vals.size());

    NumericDictionaryBuilder<uint32_t> dict;
    ASSERT_EQ(1, dict.Encode(cbuild->columns[1], cbuild->columns[0], true));
    ASSERT_EQ(1, dict.NumberRecords());
    ASSERT_EQ(4, dict.NumberElements());
    ASSERT_EQ(1, dict.Contains<uint32_t>(10));
}

TEST(DictionaryBuilderTests, FindSingleIntTensorAnyNone) {
    std::shared_ptr< ColumnSetBuilderTensor<uint32_t> > cbuild = std::make_shared< ColumnSetBuilderTensor<uint32_t> >();

    std::vector<uint32_t> vals = {10,12,14,21};
    cbuild->Append(vals.data(), vals.size());

    NumericDictionaryBuilder<uint32_t> dict;
    ASSERT_EQ(1, dict.Encode(cbuild->columns[1], cbuild->columns[0], true));
    ASSERT_EQ(1, dict.NumberRecords());
    ASSERT_EQ(4, dict.NumberElements());
    ASSERT_EQ(0, dict.Contains<uint32_t>(241));
}

TEST(DictionaryBuilderTests, FindSingleIntTensorExact) {
    std::shared_ptr< ColumnSetBuilderTensor<uint32_t> > cbuild = std::make_shared< ColumnSetBuilderTensor<uint32_t> >();

    std::vector<uint32_t> vals = {10,12,14,21};
    cbuild->Append(vals.data(), vals.size());

    NumericDictionaryBuilder<uint32_t> dict;
    ASSERT_EQ(1, dict.Encode(cbuild->columns[1], cbuild->columns[0], true));
    ASSERT_EQ(1, dict.NumberRecords());
    ASSERT_EQ(4, dict.NumberElements());
    ASSERT_EQ(1, dict.Contains<uint32_t>(&vals[0], vals.size()));
}

TEST(DictionaryBuilderTests, FindSingleIntTensorExactNone) {
    std::shared_ptr< ColumnSetBuilderTensor<uint32_t> > cbuild = std::make_shared< ColumnSetBuilderTensor<uint32_t> >();

    std::vector<uint32_t> vals = {10,12,14,21};
    cbuild->Append(vals.data(), vals.size());

    NumericDictionaryBuilder<uint32_t> dict;
    ASSERT_EQ(1, dict.Encode(cbuild->columns[1], cbuild->columns[0], true));
    ASSERT_EQ(1, dict.NumberRecords());
    ASSERT_EQ(4, dict.NumberElements());
    std::vector<uint32_t> similar = {10,12,14,12}; // 21 changed to 12
    ASSERT_EQ(0, dict.Contains<uint32_t>(&similar[0], similar.size()));
}


TEST(DictionaryBuilderTests, FindSingleIntTensorExactLarge) {
    std::shared_ptr< ColumnSetBuilderTensor<uint32_t> > cbuild = std::make_shared< ColumnSetBuilderTensor<uint32_t> >();

    std::vector<uint32_t> vals = {10,12,14,21};
    cbuild->Append(vals.data(), vals.size());
    vals = {12,14,16};
    cbuild->Append(vals.data(), vals.size());
    vals = {21,49};
    cbuild->Append(vals.data(), vals.size());
    vals = {10,12,14,21};
    cbuild->Append(vals.data(), vals.size());

    NumericDictionaryBuilder<uint32_t> dict;
    ASSERT_EQ(1, dict.Encode(cbuild->columns[1], cbuild->columns[0], true));
    ASSERT_EQ(3, dict.NumberRecords());
    ASSERT_EQ(9, dict.NumberElements());
    vals = {21,49};
    ASSERT_EQ(1, dict.Contains<uint32_t>(&vals[0], vals.size()));
}

TEST(DictionaryBuilderTests, FindSingleIntTensorExactLargeNone) {
    std::shared_ptr< ColumnSetBuilderTensor<uint32_t> > cbuild = std::make_shared< ColumnSetBuilderTensor<uint32_t> >();

    std::vector<uint32_t> vals = {10,12,14,21};
    cbuild->Append(vals.data(), vals.size());
    vals = {12,14,16};
    cbuild->Append(vals.data(), vals.size());
    vals = {21,49};
    cbuild->Append(vals.data(), vals.size());
    vals = {10,12,14,21};
    cbuild->Append(vals.data(), vals.size());

    NumericDictionaryBuilder<uint32_t> dict;
    ASSERT_EQ(1, dict.Encode(cbuild->columns[1], cbuild->columns[0], true));
    ASSERT_EQ(3, dict.NumberRecords());
    ASSERT_EQ(9, dict.NumberElements());
    vals = {1451,12,15,1,7,85,21,12};
    ASSERT_EQ(0, dict.Contains<uint32_t>(&vals[0], vals.size()));
}

}


#endif /* DICTIONARY_BUILDER_TEST_H_ */
