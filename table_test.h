#ifndef TABLE_TEST_H_
#define TABLE_TEST_H_

#include "table.h"
#include <gtest/gtest.h>

namespace pil {

TEST(TableInsertion, SingleValue) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    std::vector<float> vecvals2 = {1};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets.size());
    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas
    ASSERT_EQ(1, table.schema_dict.dict[0].ids.size());
    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());

    ASSERT_EQ(1, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0

    ASSERT_EQ(1, table.FinalizeBatch(0));
}

TEST(TableInsertion, TwoValues) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    std::vector<float> vecvals2 = {1};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    vecvals2 = {2};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets.size());
    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(2, table.build_csets[0]->columns[0]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(1));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas
    ASSERT_EQ(1, table.schema_dict.dict[0].ids.size());
    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());

    ASSERT_EQ(2, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0

    ASSERT_EQ(1, table.FinalizeBatch(0));
}

TEST(TableInsertion, TwoUnbalancedValues) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    std::vector<float> vecvals2 = {1};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));

    vecvals2 = {2, 3};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets.size());
    ASSERT_EQ(2, table.build_csets[0]->columns.size());
    ASSERT_EQ(2, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(2, table.build_csets[0]->columns[1]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(1));
    ASSERT_EQ(false, table.build_csets[0]->columns[1]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[1]->IsValid(1));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas
    ASSERT_EQ(1, table.schema_dict.dict[0].ids.size());
    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());

    ASSERT_EQ(2, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0

    ASSERT_EQ(1, table.FinalizeBatch(0));
}

TEST(TableInsertion, UnbalancedValuesTriangular) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    std::vector<float> vecvals2 = {1};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));

    vecvals2 = {2, 3};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets.size());
    ASSERT_EQ(2, table.build_csets[0]->columns.size());
    ASSERT_EQ(2, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(2, table.build_csets[0]->columns[1]->n);
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(1));
    ASSERT_EQ(false, table.build_csets[0]->columns[1]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[1]->IsValid(1));

    vecvals2 = {3};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(2, table.build_csets[0]->columns.size());
    ASSERT_EQ(3, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(3, table.build_csets[0]->columns[1]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(2));
    ASSERT_EQ(false, table.build_csets[0]->columns[1]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[1]->IsValid(1));
    ASSERT_EQ(false, table.build_csets[0]->columns[1]->IsValid(2));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas
    ASSERT_EQ(1, table.schema_dict.dict[0].ids.size());
    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());

    ASSERT_EQ(3, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0

    ASSERT_EQ(1, table.FinalizeBatch(0));
}

TEST(TableInsertion, UnbalancedDecreasing) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    std::vector<float> vecvals2 = {1, 2, 3};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(3, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[1]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[2]->IsValid(0));

    vecvals2 = {1, 2};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(3, table.build_csets[0]->columns.size());
    ASSERT_EQ(2, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[0]->columns[1]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[1]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[0]->columns[2]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[0]->columns[2]->IsValid(1));

    vecvals2 = {1};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(3, table.build_csets[0]->columns.size());
    ASSERT_EQ(3, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(2));
    ASSERT_EQ(true, table.build_csets[0]->columns[1]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[1]->IsValid(1));
    ASSERT_EQ(false, table.build_csets[0]->columns[1]->IsValid(2));
    ASSERT_EQ(true, table.build_csets[0]->columns[2]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[0]->columns[2]->IsValid(1));
    ASSERT_EQ(false, table.build_csets[0]->columns[2]->IsValid(2));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas
    ASSERT_EQ(1, table.schema_dict.dict[0].ids.size());
    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());

    ASSERT_EQ(3, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0

    ASSERT_EQ(1, table.FinalizeBatch(0));
}

// tensor
TEST(TableInsertion, TensorSingleValue) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    std::vector<float> vecvals2 = {1};
    rbuild.AddArray<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets.size());
    ASSERT_EQ(2, table.build_csets[0]->columns.size()); // This will always be 2: The offset array and the data array
    ASSERT_EQ(2, table.build_csets[0]->columns[0]->n); //  This will always be n+1
    ASSERT_EQ(1, table.build_csets[0]->columns[1]->n); //  This will always be sum total n ELEMENTS
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas
    ASSERT_EQ(1, table.schema_dict.dict[0].ids.size());
    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());

    ASSERT_EQ(1, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0
    ASSERT_EQ(1, table.FinalizeBatch(0));
}

TEST(TableInsertion, TensorTwoUnbalancedValues) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    std::vector<float> vecvals2 = {1};
    rbuild.AddArray<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(2, table.build_csets[0]->columns.size()); // This will always be 2: The offset array and the data array
    ASSERT_EQ(2, table.build_csets[0]->columns[0]->n); //  This will always be n+1
    ASSERT_EQ(1, table.build_csets[0]->columns[1]->n); //  This will always be sum total n ELEMENTS
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));

    vecvals2 = {2, 3};
    rbuild.AddArray<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets.size());
    ASSERT_EQ(2, table.build_csets[0]->columns.size());
    ASSERT_EQ(3, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(3, table.build_csets[0]->columns[1]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(2)); // this is the +1 position since the root {0,1} has cardinality 2

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas
    ASSERT_EQ(1, table.schema_dict.dict[0].ids.size());
    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());

    ASSERT_EQ(2, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0
    ASSERT_EQ(1, table.FinalizeBatch(0));
}

TEST(TableInsertion, TensorTwoUnbalancedIncreasing) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    std::vector<float> vecvals2 = {1};
    rbuild.AddArray<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(2, table.build_csets[0]->columns.size()); // This will always be 2: The offset array and the data array
    ASSERT_EQ(2, table.build_csets[0]->columns[0]->n); //  This will always be n+1
    ASSERT_EQ(1, table.build_csets[0]->columns[1]->n); //  This will always be sum total n ELEMENTS
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));

    vecvals2 = {2, 3};
    rbuild.AddArray<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets.size());
    ASSERT_EQ(2, table.build_csets[0]->columns.size());
    ASSERT_EQ(3, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(3, table.build_csets[0]->columns[1]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(2)); // this is the +1 position since the root {0,1} has cardinality 2

    vecvals2 = {3, 4, 5};
    rbuild.AddArray<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets.size());
    ASSERT_EQ(2, table.build_csets[0]->columns.size());
    ASSERT_EQ(4, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(6, table.build_csets[0]->columns[1]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(2));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(3));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas
    ASSERT_EQ(1, table.schema_dict.dict[0].ids.size());
    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());

    ASSERT_EQ(3, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0
    ASSERT_EQ(1, table.FinalizeBatch(0));
}

TEST(TableInsertion, TensorTwoUnbalancedDecreasing) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    std::vector<float> vecvals2 = {3, 4, 5};
    rbuild.AddArray<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets.size());
    ASSERT_EQ(2, table.build_csets[0]->columns.size()); // This will always be 2: The offset array and the data array
    ASSERT_EQ(2, table.build_csets[0]->columns[0]->n); //  This will always be n+1
    ASSERT_EQ(3, table.build_csets[0]->columns[1]->n); //  This will always be sum total n ELEMENTS
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));

    vecvals2 = {1, 2};
    rbuild.AddArray<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets.size());
    ASSERT_EQ(2, table.build_csets[0]->columns.size());
    ASSERT_EQ(3, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(5, table.build_csets[0]->columns[1]->n);
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(2));

    vecvals2 = {1};
    rbuild.AddArray<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));
    ASSERT_EQ(1, table.build_csets.size());
    ASSERT_EQ(2, table.build_csets[0]->columns.size());
    ASSERT_EQ(4, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(6, table.build_csets[0]->columns[1]->n);
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(2));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas
    ASSERT_EQ(1, table.schema_dict.dict[0].ids.size());

    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());
    ASSERT_EQ(3, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0
    ASSERT_EQ(1, table.FinalizeBatch(0));
}

// Add simple schema with two values
TEST(TableInsertion, SingleValueTwoColumns) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    std::vector<float> vecvals = {1};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals);
    std::vector<uint32_t> vecvals2 = {1};
    rbuild.Add<uint32_t>("FIELD2", pil::PIL_TYPE_UINT32, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));

    ASSERT_EQ(2, table.build_csets.size());

    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[1]->columns.size());

    ASSERT_EQ(1, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(1, table.build_csets[1]->columns[0]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(0));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas
    ASSERT_EQ(2, table.schema_dict.dict[0].ids.size());

    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());
    ASSERT_STREQ("FIELD2", table.field_dict.dict[table.schema_dict.dict[0].ids[1]].field_name.c_str());

    ASSERT_EQ(1, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0
    ASSERT_EQ(1, table.FinalizeBatch(0));
}

// Add two completely non-overlapping schemas
TEST(TableInsertion, MixedSchemas) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    // First schema
    std::vector<float> vecvals = {1};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals);
    std::vector<uint32_t> vecvals2 = {1};
    rbuild.Add<uint32_t>("FIELD2", pil::PIL_TYPE_UINT32, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));

    ASSERT_EQ(2, table.build_csets.size());

    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[1]->columns.size());

    ASSERT_EQ(1, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(1, table.build_csets[1]->columns[0]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(0));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas

    // Second schema
    vecvals = {1};
    rbuild.Add<float>("FIELD1-second", pil::PIL_TYPE_FLOAT, vecvals);
    vecvals2 = {1};
    rbuild.Add<uint32_t>("FIELD2-second", pil::PIL_TYPE_UINT32, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));

    ASSERT_EQ(4, table.build_csets.size());

    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[1]->columns.size());
    ASSERT_EQ(1, table.build_csets[2]->columns.size());
    ASSERT_EQ(1, table.build_csets[3]->columns.size());

    ASSERT_EQ(2, table.build_csets[0]->columns[0]->n); // These should all equal 2 as we pad all non-overlapping columns
    ASSERT_EQ(2, table.build_csets[1]->columns[0]->n);
    ASSERT_EQ(2, table.build_csets[2]->columns[0]->n);
    ASSERT_EQ(2, table.build_csets[3]->columns[0]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[2]->columns[0]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[3]->columns[0]->IsValid(0));

    ASSERT_EQ(false, table.build_csets[0]->columns[0]->IsValid(1));
    ASSERT_EQ(false, table.build_csets[1]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[2]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[3]->columns[0]->IsValid(1));

    ASSERT_EQ(2, table.schema_dict.dict.size());
    ASSERT_EQ(2, table.schema_dict.dict[0].ids.size());
    ASSERT_EQ(2, table.schema_dict.dict[1].ids.size());

    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());
    ASSERT_STREQ("FIELD2", table.field_dict.dict[table.schema_dict.dict[0].ids[1]].field_name.c_str());
    ASSERT_STREQ("FIELD1-second", table.field_dict.dict[table.schema_dict.dict[1].ids[0]].field_name.c_str());
    ASSERT_STREQ("FIELD2-second", table.field_dict.dict[table.schema_dict.dict[1].ids[1]].field_name.c_str());

    ASSERT_EQ(2, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0
    ASSERT_EQ(1, schemas[1]);
    ASSERT_EQ(1, table.FinalizeBatch(0));
}

// Add three completely non-overlapping schemas
TEST(TableInsertion, MixedSchemasThree) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    // First schema
    std::vector<float> vecvals = {1};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals);
    std::vector<uint32_t> vecvals2 = {1};
    rbuild.Add<uint32_t>("FIELD2", pil::PIL_TYPE_UINT32, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));

    ASSERT_EQ(2, table.build_csets.size());

    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[1]->columns.size());

    ASSERT_EQ(1, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(1, table.build_csets[1]->columns[0]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(0));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas

    // Second schema
    vecvals = {1};
    rbuild.Add<float>("FIELD1-second", pil::PIL_TYPE_FLOAT, vecvals);
    vecvals2 = {1};
    rbuild.Add<uint32_t>("FIELD2-second", pil::PIL_TYPE_UINT32, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));

    ASSERT_EQ(4, table.build_csets.size());

    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[1]->columns.size());
    ASSERT_EQ(1, table.build_csets[2]->columns.size());
    ASSERT_EQ(1, table.build_csets[3]->columns.size());

    ASSERT_EQ(2, table.build_csets[0]->columns[0]->n); // These should all equal 2 as we pad all non-overlapping columns
    ASSERT_EQ(2, table.build_csets[1]->columns[0]->n);
    ASSERT_EQ(2, table.build_csets[2]->columns[0]->n);
    ASSERT_EQ(2, table.build_csets[3]->columns[0]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[2]->columns[0]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[3]->columns[0]->IsValid(0));

    ASSERT_EQ(false, table.build_csets[0]->columns[0]->IsValid(1));
    ASSERT_EQ(false, table.build_csets[1]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[2]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[3]->columns[0]->IsValid(1));

    ASSERT_EQ(2, table.schema_dict.dict.size());

    // Third schema
    vecvals = {1};
    rbuild.Add<float>("FIELD1-third", pil::PIL_TYPE_FLOAT, vecvals);
    vecvals2 = {1};
    rbuild.Add<uint32_t>("FIELD2-third", pil::PIL_TYPE_UINT32, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));

    ASSERT_EQ(6, table.build_csets.size());

    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[1]->columns.size());
    ASSERT_EQ(1, table.build_csets[2]->columns.size());
    ASSERT_EQ(1, table.build_csets[3]->columns.size());
    ASSERT_EQ(1, table.build_csets[4]->columns.size());
    ASSERT_EQ(1, table.build_csets[5]->columns.size());

    ASSERT_EQ(3, table.build_csets[0]->columns[0]->n); // These should all equal 2 as we pad all non-overlapping columns
    ASSERT_EQ(3, table.build_csets[1]->columns[0]->n);
    ASSERT_EQ(3, table.build_csets[2]->columns[0]->n);
    ASSERT_EQ(3, table.build_csets[3]->columns[0]->n);
    ASSERT_EQ(3, table.build_csets[4]->columns[0]->n);
    ASSERT_EQ(3, table.build_csets[5]->columns[0]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[2]->columns[0]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[3]->columns[0]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[4]->columns[0]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[5]->columns[0]->IsValid(0));

    ASSERT_EQ(false, table.build_csets[0]->columns[0]->IsValid(1));
    ASSERT_EQ(false, table.build_csets[1]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[2]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[3]->columns[0]->IsValid(1));
    ASSERT_EQ(false, table.build_csets[4]->columns[0]->IsValid(1));
    ASSERT_EQ(false, table.build_csets[5]->columns[0]->IsValid(1));

    ASSERT_EQ(false, table.build_csets[0]->columns[0]->IsValid(2));
    ASSERT_EQ(false, table.build_csets[1]->columns[0]->IsValid(2));
    ASSERT_EQ(false, table.build_csets[2]->columns[0]->IsValid(2));
    ASSERT_EQ(false, table.build_csets[3]->columns[0]->IsValid(2));
    ASSERT_EQ(true, table.build_csets[4]->columns[0]->IsValid(2));
    ASSERT_EQ(true, table.build_csets[5]->columns[0]->IsValid(2));

    ASSERT_EQ(3, table.schema_dict.dict.size());
    ASSERT_EQ(2, table.schema_dict.dict[0].ids.size());
    ASSERT_EQ(2, table.schema_dict.dict[1].ids.size());
    ASSERT_EQ(2, table.schema_dict.dict[2].ids.size());

    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());
    ASSERT_STREQ("FIELD2", table.field_dict.dict[table.schema_dict.dict[0].ids[1]].field_name.c_str());
    ASSERT_STREQ("FIELD1-second", table.field_dict.dict[table.schema_dict.dict[1].ids[0]].field_name.c_str());
    ASSERT_STREQ("FIELD2-second", table.field_dict.dict[table.schema_dict.dict[1].ids[1]].field_name.c_str());
    ASSERT_STREQ("FIELD1-third", table.field_dict.dict[table.schema_dict.dict[2].ids[0]].field_name.c_str());
    ASSERT_STREQ("FIELD2-third", table.field_dict.dict[table.schema_dict.dict[2].ids[1]].field_name.c_str());

    ASSERT_EQ(3, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0
    ASSERT_EQ(1, schemas[1]);
    ASSERT_EQ(2, schemas[2]);
    ASSERT_EQ(1, table.FinalizeBatch(0));
}

// Add two completely non-overlapping schemas
TEST(TableInsertion, MixedSchemasPartialOverlap) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    // First schema
    std::vector<float> vecvals = {1};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals);
    std::vector<uint32_t> vecvals2 = {1};
    rbuild.Add<uint32_t>("FIELD2", pil::PIL_TYPE_UINT32, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));

    ASSERT_EQ(2, table.build_csets.size());

    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[1]->columns.size());

    ASSERT_EQ(1, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(1, table.build_csets[1]->columns[0]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(0));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas

    // Second schema
    vecvals = {1};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals);
    vecvals2 = {1};
    rbuild.Add<uint32_t>("FIELD2-second", pil::PIL_TYPE_UINT32, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));

    ASSERT_EQ(3, table.build_csets.size()); // Only 1 new novel column

    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[1]->columns.size());
    ASSERT_EQ(1, table.build_csets[2]->columns.size());

    ASSERT_EQ(2, table.build_csets[0]->columns[0]->n); // These should all equal 2 as we pad all non-overlapping columns
    ASSERT_EQ(2, table.build_csets[1]->columns[0]->n);
    ASSERT_EQ(2, table.build_csets[2]->columns[0]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[2]->columns[0]->IsValid(0));

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(1));
    ASSERT_EQ(false, table.build_csets[1]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[2]->columns[0]->IsValid(1));

    ASSERT_EQ(2, table.schema_dict.dict.size()); // These are still distinct schemas
    ASSERT_EQ(2, table.schema_dict.dict[0].ids.size());
    ASSERT_EQ(2, table.schema_dict.dict[1].ids.size());

    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());
    ASSERT_STREQ("FIELD2", table.field_dict.dict[table.schema_dict.dict[0].ids[1]].field_name.c_str());
    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[1].ids[0]].field_name.c_str());
    ASSERT_STREQ("FIELD2-second", table.field_dict.dict[table.schema_dict.dict[1].ids[1]].field_name.c_str());

    ASSERT_EQ(2, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0
    ASSERT_EQ(1, schemas[1]);
    ASSERT_EQ(1, table.FinalizeBatch(0));
}

// Add two completely non-overlapping schemas
TEST(TableInsertion, MixedSchemasPartialOverlapThreeway) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    // First schema
    std::vector<float> vecvals = {1};
    rbuild.Add<float>("FIELD1-odd", pil::PIL_TYPE_FLOAT, vecvals);
    std::vector<uint32_t> vecvals2 = {1};
    rbuild.Add<uint32_t>("FIELD2", pil::PIL_TYPE_UINT32, vecvals2);
    ASSERT_EQ(1, table.Append(rbuild));

    ASSERT_EQ(2, table.build_csets.size());

    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[1]->columns.size());

    ASSERT_EQ(1, table.build_csets[0]->columns[0]->n);
    ASSERT_EQ(1, table.build_csets[1]->columns[0]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(0));

    ASSERT_EQ(1, table.schema_dict.dict.size()); // number of schemas

    // Second schema
    vecvals = {1};
    rbuild.Add<float>("FIELD1", pil::PIL_TYPE_FLOAT, vecvals);
    vecvals2 = {1};
    rbuild.Add<uint32_t>("FIELD2", pil::PIL_TYPE_UINT32, vecvals2); // shared with schema 1
    ASSERT_EQ(1, table.Append(rbuild));

    ASSERT_EQ(3, table.build_csets.size()); // Only 1 new novel column

    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[1]->columns.size());
    ASSERT_EQ(1, table.build_csets[2]->columns.size());

    ASSERT_EQ(2, table.build_csets[0]->columns[0]->n); // These should all equal 2 as we pad all non-overlapping columns
    ASSERT_EQ(2, table.build_csets[1]->columns[0]->n);
    ASSERT_EQ(2, table.build_csets[2]->columns[0]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[2]->columns[0]->IsValid(0));

    ASSERT_EQ(false, table.build_csets[0]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[2]->columns[0]->IsValid(1));

    ASSERT_EQ(2, table.schema_dict.dict.size()); // These are still distinct schemas

    // Third schema
    vecvals2 = {1};
    rbuild.Add<uint32_t>("FIELD2", pil::PIL_TYPE_UINT32, vecvals2); // shared with schema 1
    vecvals = {1};
    rbuild.Add<float>("FIELD1-odd-again", pil::PIL_TYPE_FLOAT, vecvals);
    ASSERT_EQ(1, table.Append(rbuild));

    ASSERT_EQ(4, table.build_csets.size()); // Only 1 new novel column

    ASSERT_EQ(1, table.build_csets[0]->columns.size());
    ASSERT_EQ(1, table.build_csets[1]->columns.size());
    ASSERT_EQ(1, table.build_csets[2]->columns.size());
    ASSERT_EQ(1, table.build_csets[3]->columns.size());

    ASSERT_EQ(3, table.build_csets[0]->columns[0]->n); // These should all equal 3 as we pad all non-overlapping columns
    ASSERT_EQ(3, table.build_csets[1]->columns[0]->n);
    ASSERT_EQ(3, table.build_csets[2]->columns[0]->n);
    ASSERT_EQ(3, table.build_csets[3]->columns[0]->n);

    ASSERT_EQ(true, table.build_csets[0]->columns[0]->IsValid(0));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[2]->columns[0]->IsValid(0));
    ASSERT_EQ(false, table.build_csets[3]->columns[0]->IsValid(0));

    ASSERT_EQ(false, table.build_csets[0]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(1));
    ASSERT_EQ(true, table.build_csets[2]->columns[0]->IsValid(1));
    ASSERT_EQ(false, table.build_csets[3]->columns[0]->IsValid(1));

    ASSERT_EQ(false, table.build_csets[0]->columns[0]->IsValid(2));
    ASSERT_EQ(true, table.build_csets[1]->columns[0]->IsValid(2));
    ASSERT_EQ(false, table.build_csets[2]->columns[0]->IsValid(2));
    ASSERT_EQ(true, table.build_csets[3]->columns[0]->IsValid(2));

    ASSERT_EQ(3, table.schema_dict.dict.size()); // These are still distinct schemas
    ASSERT_EQ(2, table.schema_dict.dict[0].ids.size());
    ASSERT_EQ(2, table.schema_dict.dict[1].ids.size());
    ASSERT_EQ(2, table.schema_dict.dict[2].ids.size());

    ASSERT_STREQ("FIELD1-odd", table.field_dict.dict[table.schema_dict.dict[0].ids[0]].field_name.c_str());
    ASSERT_STREQ("FIELD2", table.field_dict.dict[table.schema_dict.dict[0].ids[1]].field_name.c_str());

    ASSERT_STREQ("FIELD1", table.field_dict.dict[table.schema_dict.dict[1].ids[0]].field_name.c_str());
    ASSERT_STREQ("FIELD2", table.field_dict.dict[table.schema_dict.dict[1].ids[1]].field_name.c_str());

    ASSERT_STREQ("FIELD2", table.field_dict.dict[table.schema_dict.dict[2].ids[0]].field_name.c_str());
    ASSERT_STREQ("FIELD1-odd-again", table.field_dict.dict[table.schema_dict.dict[2].ids[1]].field_name.c_str());

    ASSERT_EQ(3, table.meta_data.batches.back()->n_rec); // number of records in this batch
    const uint32_t* schemas = reinterpret_cast<uint32_t*>(table.meta_data.batches.back()->schemas->columns[0]->mutable_data());
    ASSERT_EQ(0, schemas[0]); // first schema should be id 0
    ASSERT_EQ(1, schemas[1]);
    ASSERT_EQ(2, schemas[2]);
    ASSERT_EQ(1, table.FinalizeBatch(0));
}

TEST(TableInsertion, MixedSchemasExtreme) {
    TableConstructor table;
    RecordBuilder rbuild;

    ASSERT_EQ(0, table.build_csets.size());

    // Insert 5000 different single-schema tuples into the table.
    std::vector<float> vecvals;
    for(int i = 0; i < 5000; ++i) {
        vecvals = {i};
        rbuild.Add<float>("FIELD1-" + std::to_string(i), pil::PIL_TYPE_FLOAT, vecvals);
        ASSERT_EQ(1, table.Append(rbuild));
        ASSERT_EQ(i+1, table.build_csets.size());
        ASSERT_EQ(true, table.build_csets[i]->columns[0]->IsValid(i));
        std::cerr << "FIELD1-" + std::to_string(i) << " at=" << i << "/" << 5000 << " cap=" << table.build_csets.capacity() << std::endl;
        ASSERT_EQ(true, table.build_csets[i]->columns[0]->IsValid(i));
    }

    ASSERT_EQ(5000, table.build_csets.size()); // number of columns in the current batch

    // Every column should have 5000 elements.
    for(int i = 0; i < 5000; ++i) {
        ASSERT_EQ(5000, table.build_csets[i]->columns[0]->n); // this should be 5000
        std::cerr << "validity=" << i << "/" << 5000 << std::endl;
        ASSERT_EQ(true, table.build_csets[i]->columns[0]->IsValid(i));
    }

    ASSERT_EQ(5000, table.meta_data.batches.back()->n_rec); // number of records in this batch

    ASSERT_EQ(1, table.FinalizeBatch(0));
}

}



#endif /* TABLE_TEST_H_ */
