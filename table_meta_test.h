#ifndef TABLE_META_TEST_H_
#define TABLE_META_TEST_H_

#include "table_meta.h"
#include <gtest/gtest.h>
#include "column_store.h"

namespace pil {

TEST(SegmentalStatisticsTests, MatchRangeInt8) {
    ColumnStoreMetaData meta;

    std::shared_ptr< ColumnSetBuilder<int8_t> > builder = std::make_shared< ColumnSetBuilder<int8_t> >();
    builder->Append(-100);
    builder->Append(-50);
    builder->Append(25);
    builder->Append(1);

    meta.Set(builder->columns[0]);
    ASSERT_EQ(1, meta.ComputeSegmentStats<int8_t>(builder->columns[0]));
    ASSERT_EQ(-100, meta.GetSegmentMin<int8_t>());
    ASSERT_EQ(25, meta.GetSegmentMax<int8_t>());
}

TEST(SegmentalStatisticsTests, MatchSegmentInt8) {
    ColumnStoreMetaData meta;

    std::shared_ptr< ColumnSetBuilder<int8_t> > builder = std::make_shared< ColumnSetBuilder<int8_t> >();
    builder->Append(-100);
    builder->Append(-50);
    builder->Append(25);
    builder->Append(1);

    meta.Set(builder->columns[0]);
    ASSERT_EQ(1, meta.ComputeSegmentStats<int8_t>(builder->columns[0]));
    ASSERT_EQ(true, meta.OverlapSegment<int8_t>(-25, 10)); // fully contained
    ASSERT_EQ(true, meta.OverlapSegment<int8_t>(-120, -90)); // left contained
    ASSERT_EQ(true, meta.OverlapSegment<int8_t>(20, 50)); // right contained
    ASSERT_EQ(true, meta.OverlapSegment<int8_t>(-120, 50)); // super
    ASSERT_EQ(false, meta.OverlapSegment<int8_t>(50, 100)); // outside range
    ASSERT_EQ(false, meta.OverlapSegment<int8_t>(-120, -110)); // outside range
}

TEST(SegmentalStatisticsTests, MatchRangeInt32) {
    ColumnStoreMetaData meta;

    std::shared_ptr< ColumnSetBuilder<int32_t> > builder = std::make_shared< ColumnSetBuilder<int32_t> >();
    builder->Append(-100);
    builder->Append(-50);
    builder->Append(25);
    builder->Append(1);

    meta.Set(builder->columns[0]);
    ASSERT_EQ(1, meta.ComputeSegmentStats<int32_t>(builder->columns[0]));
    ASSERT_EQ(-100, meta.GetSegmentMin<int32_t>());
    ASSERT_EQ(25, meta.GetSegmentMax<int32_t>());
}

TEST(SegmentalStatisticsTests, MatchSegmentInt32) {
    ColumnStoreMetaData meta;

    std::shared_ptr< ColumnSetBuilder<int32_t> > builder = std::make_shared< ColumnSetBuilder<int32_t> >();
    builder->Append(-100);
    builder->Append(-50);
    builder->Append(25);
    builder->Append(1);

    meta.Set(builder->columns[0]);
    ASSERT_EQ(1, meta.ComputeSegmentStats<int32_t>(builder->columns[0]));
    ASSERT_EQ(true, meta.OverlapSegment<int32_t>(-90, 10)); // fully contained
    ASSERT_EQ(true, meta.OverlapSegment<int32_t>(-120, -90)); // left contained
    ASSERT_EQ(true, meta.OverlapSegment<int32_t>(20, 50)); // right contained
    ASSERT_EQ(true, meta.OverlapSegment<int32_t>(-120, 50)); // super
    ASSERT_EQ(false, meta.OverlapSegment<int32_t>(50, 100)); // outside range
    ASSERT_EQ(false, meta.OverlapSegment<int32_t>(-120, -110)); // outside range
}

TEST(SegmentalStatisticsTests, MatchRangeUInt32) {
    ColumnStoreMetaData meta;

    std::shared_ptr< ColumnSetBuilder<uint32_t> > builder = std::make_shared< ColumnSetBuilder<uint32_t> >();
    builder->Append(2501);
    builder->Append(1409);
    builder->Append(249181);
    builder->Append(1312);

    meta.Set(builder->columns[0]);
    ASSERT_EQ(1, meta.ComputeSegmentStats<int32_t>(builder->columns[0]));
    ASSERT_EQ(1312, meta.GetSegmentMin<uint32_t>());
    ASSERT_EQ(249181, meta.GetSegmentMax<uint32_t>());
}

TEST(SegmentalStatisticsTests, MatchRangeFloat) {
    ColumnStoreMetaData meta;

    std::shared_ptr< ColumnSetBuilder<float> > builder = std::make_shared< ColumnSetBuilder<float> >();
    builder->Append(2501.121);
    builder->Append(1409.51);
    builder->Append(-249181.812);
    builder->Append(1312.12);

    meta.Set(builder->columns[0]);
    ASSERT_EQ(1, meta.ComputeSegmentStats<float>(builder->columns[0]));
    ASSERT_FLOAT_EQ(-249181.812, meta.GetSegmentMin<float>());
    ASSERT_FLOAT_EQ(2501.121, meta.GetSegmentMax<float>());
}

TEST(SegmentalStatisticsTests, MatchSegmentFloat) {
    ColumnStoreMetaData meta;

    std::shared_ptr< ColumnSetBuilder<float> > builder = std::make_shared< ColumnSetBuilder<float> >();
    builder->Append(2501.121);
    builder->Append(1409.51);
    builder->Append(-249181.812);
    builder->Append(1312.12);

    meta.Set(builder->columns[0]);
    ASSERT_EQ(1, meta.ComputeSegmentStats<float>(builder->columns[0]));
    ASSERT_EQ(true, meta.OverlapSegment<float>(-24000.5, 1200.51)); // fully contained
    ASSERT_EQ(true, meta.OverlapSegment<float>(-401231.2, 0)); // left contained
    ASSERT_EQ(true, meta.OverlapSegment<float>(1212.5, 5500.21)); // right contained
    ASSERT_EQ(true, meta.OverlapSegment<float>(-35000.21, 10000.8)); // super
    ASSERT_EQ(false, meta.OverlapSegment<float>(-500000.8123, -400000.89)); // outside range left
    ASSERT_EQ(false, meta.OverlapSegment<float>(5500.21, 123123.2)); // outside range right
}

TEST(SegmentalStatisticsTests, MatchRangeDouble) {
    ColumnStoreMetaData meta;

    std::shared_ptr< ColumnSetBuilder<double> > builder = std::make_shared< ColumnSetBuilder<double> >();
    builder->Append(2501.121);
    builder->Append(1409.51);
    builder->Append(-24918112.812);
    builder->Append(1312.12);

    meta.Set(builder->columns[0]);
    ASSERT_EQ(1, meta.ComputeSegmentStats<double>(builder->columns[0]));
    ASSERT_DOUBLE_EQ(-24918112.812, meta.GetSegmentMin<double>());
    ASSERT_DOUBLE_EQ(2501.121, meta.GetSegmentMax<double>());
}

TEST(SegmentalStatisticsTests, MatchSegmentDouble) {
    ColumnStoreMetaData meta;

    std::shared_ptr< ColumnSetBuilder<double> > builder = std::make_shared< ColumnSetBuilder<double> >();
    builder->Append(2501.121);
    builder->Append(1409.51);
    builder->Append(-24918112.812);
    builder->Append(1312.12);

    meta.Set(builder->columns[0]);
    ASSERT_EQ(1, meta.ComputeSegmentStats<double>(builder->columns[0]));
    ASSERT_EQ(true, meta.OverlapSegment<double>(-20918112.5, 1200.51)); // fully contained
    ASSERT_EQ(true, meta.OverlapSegment<double>(-500000000.2148, 0)); // left contained
    ASSERT_EQ(true, meta.OverlapSegment<double>(1212.5, 5500.21)); // right contained
    ASSERT_EQ(true, meta.OverlapSegment<double>(-500000000, 10000.8)); // super
    ASSERT_EQ(false, meta.OverlapSegment<double>(-500000000, -400000000)); // outside range left
    ASSERT_EQ(false, meta.OverlapSegment<double>(10000, 50000.2841)); // outside range right
}


}

#endif /* TABLE_META_TEST_H_ */
