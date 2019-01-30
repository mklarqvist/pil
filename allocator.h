#ifndef ALLOCATOR_H_
#define ALLOCATOR_H_

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>

#include "memory_pool.h"

namespace pil {

/// \brief A STL allocator delegating allocations to a Arrow MemoryPool
template <class T>
class stl_allocator {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    template <class U>
    struct rebind {
        using other = stl_allocator<U>;
    };

    /// \brief Construct an allocator from the default MemoryPool
    //stl_allocator() noexcept : pool_(default_memory_pool()) {}
    stl_allocator() = delete;

    /// \brief Construct an allocator from the given MemoryPool
    explicit stl_allocator(MemoryPool* pool) noexcept : pool_(pool) {}

    template <class U>
    stl_allocator(const stl_allocator<U>& rhs) noexcept : pool_(rhs.pool_) {}

    ~stl_allocator() { pool_ = nullptr; }

    pointer address(reference r) const noexcept { return std::addressof(r); }

    const_pointer address(const_reference r) const noexcept { return std::addressof(r); }

    pointer allocate(size_type n, const void* /*hint*/ = nullptr) {
        uint8_t* data;
        int s = pool_->Allocate(n * sizeof(T), &data);
        if (s != 1) throw std::bad_alloc();
        return reinterpret_cast<pointer>(data);
    }

    void deallocate(pointer p, size_type n) {
        pool_->Free(reinterpret_cast<uint8_t*>(p), n * sizeof(T));
    }

    size_type size_max() const noexcept { return size_type(-1) / sizeof(T); }

    template <class U, class... Args>
    void construct(U* p, Args&&... args) {
        new (reinterpret_cast<void*>(p)) U(std::forward<Args>(args)...);
    }

    template <class U>
    void destroy(U* p) {
        p->~U();
    }

    MemoryPool* pool() const noexcept { return pool_; }

// Todo: make private
public:
    MemoryPool* pool_;
};

}



#endif /* ALLOCATOR_H_ */
