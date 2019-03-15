#include "third_party/xxhash/xxhash.h"
#include "bloom_filter.h"

namespace pil {

constexpr uint32_t BlockSplitBloomFilter::SALT[kBitsSetPerBlock];

BlockSplitBloomFilter::BlockSplitBloomFilter()
    : pool_(default_memory_pool()), num_bytes_(0)
{}

void BlockSplitBloomFilter::Init(uint32_t num_bytes) {
    if (num_bytes < kMinimumBloomFilterBytes) {
        num_bytes = kMinimumBloomFilterBytes;
    }

    // Get next power of 2 if it is not power of 2.
    if ((num_bytes & (num_bytes - 1)) != 0) {
        num_bytes = static_cast<uint32_t>(BitUtils::NextPower2(num_bytes));
    }

    num_bytes_ = num_bytes;
    assert(AllocateBuffer(pool_, num_bytes_, &data_) == 1);
    memset(data_->mutable_data(), 0, num_bytes_);
}

void BlockSplitBloomFilter::Init(const uint8_t* bitset, uint32_t num_bytes) {
    assert(bitset != nullptr);

    if (num_bytes < kMinimumBloomFilterBytes || (num_bytes & (num_bytes - 1)) != 0) {
        //throw ParquetException("Given length of bitset is illegal");
        exit(1);
    }

    num_bytes_ = num_bytes;
    assert(AllocateBuffer(pool_, num_bytes_, &data_) == 1);
    memcpy(data_->mutable_data(), bitset, num_bytes_);
}

void BlockSplitBloomFilter::SetMask(uint32_t key, BlockMask& block_mask) const {
    for (int i = 0; i < kBitsSetPerBlock; ++i) {
        block_mask.item[i] = key * SALT[i];
    }

    for (int i = 0; i < kBitsSetPerBlock; ++i) {
        block_mask.item[i] = block_mask.item[i] >> 27;
    }

    for (int i = 0; i < kBitsSetPerBlock; ++i) {
        block_mask.item[i] = UINT32_C(0x1) << block_mask.item[i];
    }
}

bool BlockSplitBloomFilter::FindHash(uint64_t hash) const {
    const uint32_t bucket_index = static_cast<uint32_t>((hash >> 32) & (num_bytes_ / kBytesPerFilterBlock - 1));
    uint32_t key = static_cast<uint32_t>(hash);
    uint32_t* bitset32 = reinterpret_cast<uint32_t*>(data_->mutable_data());

    // Calculate mask for bucket.
    BlockMask block_mask;
    SetMask(key, block_mask);

    for (int i = 0; i < kBitsSetPerBlock; ++i) {
        if (0 == (bitset32[kBitsSetPerBlock * bucket_index + i] & block_mask.item[i])) {
            return false;
        }
    }
    return true;
}

void BlockSplitBloomFilter::InsertHash(uint64_t hash) {
    const uint32_t bucket_index = static_cast<uint32_t>(hash >> 32) & (num_bytes_ / kBytesPerFilterBlock - 1);
    uint32_t key = static_cast<uint32_t>(hash);
    uint32_t* bitset32 = reinterpret_cast<uint32_t*>(data_->mutable_data());

    // Calculate mask for bucket.
    BlockMask block_mask;
    SetMask(key, block_mask);

    for (int i = 0; i < kBitsSetPerBlock; i++) {
        bitset32[bucket_index * kBitsSetPerBlock + i] |= block_mask.item[i];
    }
}

uint64_t BlockSplitBloomFilter::Hash(int8_t value) const   { return(XXH64(&value, sizeof(int8_t),   912732)); }
uint64_t BlockSplitBloomFilter::Hash(int16_t value) const  { return(XXH64(&value, sizeof(int16_t),  912732)); }
uint64_t BlockSplitBloomFilter::Hash(int32_t value) const  { return(XXH64(&value, sizeof(int32_t),  912732)); }
uint64_t BlockSplitBloomFilter::Hash(int64_t value) const  { return(XXH64(&value, sizeof(int64_t),  912732)); }
uint64_t BlockSplitBloomFilter::Hash(uint8_t value) const  { return(XXH64(&value, sizeof(uint8_t),  912732)); }
uint64_t BlockSplitBloomFilter::Hash(uint16_t value) const { return(XXH64(&value, sizeof(uint16_t), 912732)); }
uint64_t BlockSplitBloomFilter::Hash(uint32_t value) const { return(XXH64(&value, sizeof(uint32_t), 912732)); }
uint64_t BlockSplitBloomFilter::Hash(uint64_t value) const { return(XXH64(&value, sizeof(uint64_t), 912732)); }
uint64_t BlockSplitBloomFilter::Hash(float value) const    { return(XXH64(&value, sizeof(float),    912732)); }
uint64_t BlockSplitBloomFilter::Hash(double value) const   { return(XXH64(&value, sizeof(double),   912732)); }

}
