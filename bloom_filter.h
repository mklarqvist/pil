/*
* Copyright (c) 2019 Marcus D. R. Klarqvist
* Author(s): Marcus D. R. Klarqvist
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#ifndef BLOOM_FILTER_H_
#define BLOOM_FILTER_H_

#include <cstdint>
#include <cmath>

#include "buffer.h"
#include "bit_utils.h"

namespace pil {

// A Bloom filter is a compact structure to indicate whether an item is not in a set or
// probably in a set. The Bloom filter usually consists of a bit set that represents a
// set of elements, a hash strategy and a Bloom filter algorithm.
class BloomFilter {
public:
    // Maximum Bloom filter size, it sets to HDFS default block size 128MB
    // This value will be reconsidered when implementing Bloom filter producer.
    static constexpr uint32_t kMaximumBloomFilterBytes = 128 * 1024 * 1024;

    /// Determine whether an element exist in set or not.
    ///
    /// @param hash the element to contain.
    /// @return false if value is definitely not in set, and true means PROBABLY
    /// in set.
    virtual bool FindHash(uint64_t hash) const = 0;

    /// Insert element to set represented by Bloom filter bitset.
    /// @param hash the hash of value to insert into Bloom filter.
    virtual void InsertHash(uint64_t hash) = 0;

    /// Get the number of bytes of bitset
    virtual uint32_t GetBitsetSize() const = 0;

    /// Compute hash for N bits value by using its plain encoding result.
    ///
    /// @param value the value to hash.
    /// @return hash result.
    virtual uint64_t Hash(int8_t value) const = 0;
    virtual uint64_t Hash(int16_t value) const = 0;
    virtual uint64_t Hash(int32_t value) const = 0;
    virtual uint64_t Hash(int64_t value) const = 0;
    virtual uint64_t Hash(uint8_t value) const = 0;
    virtual uint64_t Hash(uint16_t value) const = 0;
    virtual uint64_t Hash(uint32_t value) const = 0;
    virtual uint64_t Hash(uint64_t value) const = 0;
    virtual uint64_t Hash(float value) const = 0;
    virtual uint64_t Hash(double value) const = 0;

    virtual ~BloomFilter() {}
};

//
// The BlockSplitBloomFilter is implemented using block-based Bloom filters from
// Putze et al.'s "Cache-,Hash- and Space-Efficient Bloom filters". The basic idea is to
// hash the item to a tiny Bloom filter which size fit a single cache line or smaller.
//
// This implementation sets 8 bits in each tiny Bloom filter. Each tiny Bloom
// filter is 32 bytes to take advantage of 32-byte SIMD instructions.
class BlockSplitBloomFilter : public BloomFilter {
public:
    /// The constructor of BlockSplitBloomFilter. It uses XXHASH64 as hash function.
    BlockSplitBloomFilter();

    /// Initialize the BlockSplitBloomFilter. The range of num_bytes should be within
    /// [kMinimumBloomFilterBytes, kMaximumBloomFilterBytes], it will be
    /// rounded up/down to lower/upper bound if num_bytes is out of range and also
    /// will be rounded up to a power of 2.
    ///
    /// @param num_bytes The number of bytes to store Bloom filter bitset.
    void Init(uint32_t num_bytes);

    /// Initialize the BlockSplitBloomFilter. It copies the bitset as underlying
    /// bitset because the given bitset may not satisfy the 32-byte alignment requirement
    /// which may lead to segfault when performing SIMD instructions. It is the caller's
    /// responsibility to free the bitset passed in. This is used when reconstructing
    /// a Bloom filter from a parquet file.
    ///
    /// @param bitset The given bitset to initialize the Bloom filter.
    /// @param num_bytes  The number of bytes of given bitset.
    void Init(const uint8_t* bitset, uint32_t num_bytes);

    // Minimum Bloom filter size, it sets to 32 bytes to fit a tiny Bloom filter.
    static constexpr uint32_t kMinimumBloomFilterBytes = 32;

    /// Calculate optimal size according to the number of distinct values and false
    /// positive probability.
    ///
    /// @param ndv The number of distinct values.
    /// @param fpp The false positive probability.
    /// @return it always return a value between kMinimumBloomFilterBytes and
    /// kMaximumBloomFilterBytes, and the return value is always a power of 2
    static uint32_t OptimalNumOfBits(uint32_t ndv, double fpp) {
        assert(fpp > 0.0 && fpp < 1.0);
        const double m = -8.0 * ndv / log(1 - pow(fpp, 1.0 / 8));
        uint32_t num_bits = static_cast<uint32_t>(m);

        // Handle overflow.
        if (m < 0 || m > kMaximumBloomFilterBytes << 3) {
            num_bits = static_cast<uint32_t>(kMaximumBloomFilterBytes << 3);
        }

        // Round up to lower bound
        if (num_bits < kMinimumBloomFilterBytes << 3) {
            num_bits = kMinimumBloomFilterBytes << 3;
        }

        // Get next power of 2 if bits is not power of 2.
        if ((num_bits & (num_bits - 1)) != 0) {
            num_bits = static_cast<uint32_t>(BitUtils::NextPower2(num_bits));
        }

        // Round down to upper bound
        if (num_bits > kMaximumBloomFilterBytes << 3) {
            num_bits = kMaximumBloomFilterBytes << 3;
        }

        return num_bits;
    }

    bool FindHash(uint64_t hash) const override;
    void InsertHash(uint64_t hash) override;

    uint32_t GetBitsetSize() const override { return num_bytes_; }

    uint64_t Hash(int8_t value) const override;
    uint64_t Hash(int16_t value) const override;
    uint64_t Hash(int32_t value) const override;
    uint64_t Hash(int64_t value) const override;
    uint64_t Hash(uint8_t value) const override;
    uint64_t Hash(uint16_t value) const override;
    uint64_t Hash(uint32_t value) const override;
    uint64_t Hash(uint64_t value) const override;
    uint64_t Hash(float value) const override;
    uint64_t Hash(double value) const override;

private:
    // Bytes in a tiny Bloom filter block.
    static constexpr int kBytesPerFilterBlock = 32;

    // The number of bits to be set in each tiny Bloom filter
    static constexpr int kBitsSetPerBlock = 8;

    // A mask structure used to set bits in each tiny Bloom filter.
    struct BlockMask {
        uint32_t item[kBitsSetPerBlock];
    };

    // The block-based algorithm needs eight odd SALT values to calculate eight indexes
    // of bit to set, one bit in each 32-bit word.
    static constexpr uint32_t SALT[kBitsSetPerBlock] = {
        0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
        0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U
    };

    /// Set bits in mask array according to input key.
    /// @param key the value to calculate mask values.
    /// @param mask the mask array is used to set inside a block
    void SetMask(uint32_t key, BlockMask& mask) const;

    // Memory pool to allocate aligned buffer for bitset
    MemoryPool* pool_;

    // The underlying buffer of bitset.
    std::shared_ptr<Buffer> data_;

    // The number of bytes of Bloom filter bitset.
    uint32_t num_bytes_;
};

}

#endif /* BLOOM_FILTER_H_ */
