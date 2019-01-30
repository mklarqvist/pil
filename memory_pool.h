#ifndef MEMORY_POOL_H_
#define MEMORY_POOL_H_

#include <cstdint>
#include <algorithm>
#include <memory>
#include <atomic>

#include "status.h"

namespace pil {

namespace internal {

// Helper tracking memory statistics

class MemoryPoolStats {
public:
    MemoryPoolStats() : bytes_allocated_(0), max_memory_(0) {}

    int64_t max_memory() const { return max_memory_.load(); }

    int64_t bytes_allocated() const { return bytes_allocated_.load(); }

    inline void UpdateAllocatedBytes(int64_t diff) {
        auto allocated = bytes_allocated_.fetch_add(diff) + diff;
        // "maximum" allocated memory is ill-defined in multi-threaded code,
        // so don't try to be too rigorous here
        if (diff > 0 && allocated > max_memory_) {
          max_memory_ = allocated;
        }
    }

protected:
    std::atomic<int64_t> bytes_allocated_;
    std::atomic<int64_t> max_memory_;
};

}  // namespace internal

class MemoryPool {
public:
    virtual ~MemoryPool();

    /// \brief EXPERIMENTAL. Create a new instance of the default MemoryPool
    static std::unique_ptr<MemoryPool> CreateDefault();

    /// Allocate a new memory region of at least size bytes.
    ///
    /// The allocated region shall be 64-byte aligned.
    virtual int Allocate(int64_t size, uint8_t** out) = 0;

    /// Resize an already allocated memory section.
    ///
    /// As by default most default allocators on a platform don't support aligned
    /// reallocation, this function can involve a copy of the underlying data.
    virtual int Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) = 0;

    /// Free an allocated region.
    ///
    /// @param buffer Pointer to the start of the allocated memory region
    /// @param size Allocated size located at buffer. An allocator implementation
    ///   may use this for tracking the amount of allocated bytes as well as for
    ///   faster deallocation if supported by its backend.
    virtual void Free(uint8_t* buffer, int64_t size) = 0;

    /// The number of bytes that were allocated and not yet free'd through
    /// this allocator.
    virtual int64_t bytes_allocated() const = 0;

    /// Return peak memory allocation in this memory pool
    ///
    /// \return Maximum bytes allocated. If not known (or not implemented),
    /// returns -1
    virtual int64_t max_memory() const;

protected:
    MemoryPool();
};

/// Return the process-wide default memory pool.
MemoryPool* default_memory_pool();

}


#endif /* MEMORY_POOL_H_ */
