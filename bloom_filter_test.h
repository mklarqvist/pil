#ifndef BLOOM_FILTER_TEST_H_
#define BLOOM_FILTER_TEST_H_

#include "bloom_filter.h"
#include <gtest/gtest.h>


namespace pil {

TEST(BloomFilterTests, InsertFind) {
    BlockSplitBloomFilter b;
    uint32_t optimal = b.OptimalNumOfBits(1000, 0.05);
    b.Init(optimal);

    std::vector<uint32_t> vals = {12, 21, 191, 1024};
    for(int i = 0; i < vals.size(); ++i) {
        uint64_t hash = b.Hash(vals[i]);
        b.InsertHash(hash);
    }

    for(int i = 0; i < vals.size(); ++i) {
        ASSERT_EQ(true, b.FindHash(b.Hash(vals[i])));
    }

}

TEST(BloomFilterTests, InsertFindString) {
    BlockSplitBloomFilter b;
    uint32_t optimal = b.OptimalNumOfBits(1000, 0.05);
    b.Init(optimal);

    std::vector<std::string> vals = {"john doe", "bananman", "superman", "johnsson", "dawn", "sun"};
    for(int i = 0; i < vals.size(); ++i) {
        uint64_t hash = b.Hash(&vals[i][0], vals[i].size());
        b.InsertHash(hash);
    }

    for(int i = 0; i < vals.size(); ++i) {
        ASSERT_EQ(true, b.FindHash(b.Hash(&vals[i][0], vals[i].size())));
    }

    std::string fake = "i should not be found on average even by pure chance";
    ASSERT_EQ(false, b.FindHash(b.Hash(&fake[0], fake.size())));

}

}


#endif /* BLOOM_FILTER_TEST_H_ */
