#include "memory_pool.h"

#include <cassert>
#include <cstring>

namespace pil {

constexpr size_t kAlignment = 64;

namespace {

// A static piece of memory for 0-size allocations, so as to return
// an aligned non-null pointer.
alignas(kAlignment) static uint8_t zero_size_area[1];

// Allocate memory according to the alignment requirements for Arrow
// (as of May 2016 64 bytes)
int AllocateAligned(int64_t size, uint8_t** out) {
    if (size < 0) {
        return(-1);
        //return Status::Invalid("negative malloc size");
    }

    if (size == 0) {
        *out = zero_size_area;
        return(1);
        //return Status::OK();
    }

    if (static_cast<uint64_t>(size) >= std::numeric_limits<size_t>::max()) {
        //return Status::CapacityError("malloc size overflows size_t");
        return(-1);
    }
#ifdef _WIN32
    // Special code path for Windows
    *out = reinterpret_cast<uint8_t*>(_aligned_malloc(static_cast<size_t>(size), kAlignment));
    if (!*out) {
        //return Status::OutOfMemory("malloc of size ", size, " failed");
        return(-1);
    }
#else
    const int result = posix_memalign(reinterpret_cast<void**>(out), kAlignment,
                                      static_cast<size_t>(size));
    if (result == ENOMEM) {
        return(-1);
        //return Status::OutOfMemory("malloc of size ", size, " failed");
    }

    if (result == EINVAL) {
        return(-1);
        //return Status::Invalid("invalid alignment parameter: ", kAlignment);
    }
#endif
    return(1);
    //return Status::OK();
}

void DeallocateAligned(uint8_t* ptr, int64_t size) {
    if (ptr == zero_size_area) {
      assert(size == 0);
      //DCHECK_EQ(size, 0);
    } else {
#ifdef _WIN32
    _aligned_free(ptr);
#elif defined(ARROW_JEMALLOC)
    dallocx(ptr, MALLOCX_ALIGN(kAlignment));
#else
    std::free(ptr);
#endif
    }
}

int ReallocateAligned(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    uint8_t* previous_ptr = *ptr;
    if (previous_ptr == zero_size_area) {
      //DCHECK_EQ(old_size, 0);
      assert(old_size == 0);
      return AllocateAligned(new_size, ptr);
    }

    if (new_size == 0) {
    DeallocateAligned(previous_ptr, old_size);
    *ptr = zero_size_area;
    return(1);
    //return Status::OK();
    }

    // Note: We cannot use realloc() here as it doesn't guarantee alignment.

    // Allocate new chunk
    uint8_t* out = nullptr;
    int ret = AllocateAligned(new_size, &out);
    if(ret != 1) return(-1);
    //DCHECK(out);
    // Copy contents and release old memory chunk
    memcpy(out, *ptr, static_cast<size_t>(std::min(new_size, old_size)));
#ifdef _WIN32
    _aligned_free(*ptr);
#else
    std::free(*ptr);
#endif // defined(_MSC_VER)
    *ptr = out;

    return(1);
    //return Status::OK();
}

}  // namespace

MemoryPool::MemoryPool() {}

MemoryPool::~MemoryPool() {}

int64_t MemoryPool::max_memory() const { return -1; }


// Default MemoryPool implementation

class DefaultMemoryPool : public MemoryPool {
public:
    ~DefaultMemoryPool() override {}

    int Allocate(int64_t size, uint8_t** out) override {
        int status = AllocateAligned(size, out);
        if(status != 1) return -1;

        stats_.UpdateAllocatedBytes(size);
        return 1;
    }

    int Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
        int status = ReallocateAligned(old_size, new_size, ptr);
        if(status != 1) return -1;

        stats_.UpdateAllocatedBytes(new_size - old_size);
        return 1;
    }

    int64_t bytes_allocated() const override { return stats_.bytes_allocated(); }

    void Free(uint8_t* buffer, int64_t size) override {
        DeallocateAligned(buffer, size);

        stats_.UpdateAllocatedBytes(-size);
    }

    int64_t max_memory() const override { return stats_.max_memory(); }

private:
    internal::MemoryPoolStats stats_;
};

std::unique_ptr<MemoryPool> MemoryPool::CreateDefault() {
    return std::unique_ptr<MemoryPool>(new DefaultMemoryPool);
}

MemoryPool* default_memory_pool() {
    static DefaultMemoryPool default_memory_pool_;
    return &default_memory_pool_;
}


}
