#ifndef BUFFER_BUILDER_H_
#define BUFFER_BUILDER_H_

#include "bit_utils.h"
#include "buffer.h"

namespace pil {

class BufferBuilder {
public:
    explicit BufferBuilder(MemoryPool* pool = default_memory_pool()) :
        pool_(pool), data_(nullptr), capacity_(0), size_(0)
    {}

    void Reset() {
        buffer_ = nullptr;
        capacity_ = size_ = 0;
    }

    /// \brief Resize the buffer to the nearest multiple of 64 bytes
    ///
    /// \param new_capacity the new capacity of the of the builder. Will be
    /// rounded up to a multiple of 64 bytes for padding
    /// \param shrink_to_fit if
    /// new capacity is smaller than the existing size, reallocate internal
    /// buffer. Set to false to avoid reallocations when shrinking the builder.
    /// \return Status
    int Resize(const int64_t new_capacity, bool shrink_to_fit = true) {
        // Resize(0) is a no-op
        if (new_capacity == 0) {
            //return Status::OK();
            return(1);
        }

        int64_t old_capacity = capacity_;
        if (buffer_ == nullptr) {
            int ret = AllocateResizableBuffer(pool_, new_capacity, &buffer_);
            if(ret != 1) return(-1);
        } else {
            int ret = buffer_->Resize(new_capacity, shrink_to_fit);
            if(ret != 1) return(-1);
        }
        capacity_ = buffer_->capacity();
        data_ = buffer_->mutable_data();
        if (capacity_ > old_capacity) {
            memset(data_ + old_capacity, 0, capacity_ - old_capacity);
        }
        //return Status::OK();
        return(1);
    }

    /// \brief Ensure that builder can accommodate the additional number of bytes
    /// without the need to perform allocations
    ///
    /// \param[in] additional_bytes number of additional bytes to make space for
    /// \param[in] grow_by_factor if true, round up allocations using the
    /// strategy in BufferBuilder::GrowByFactor
    /// \return Status
    int Reserve(const int64_t additional_bytes, bool grow_by_factor = false) {
        auto min_capacity = size_ + additional_bytes;
        if (min_capacity <= capacity_) return 1;
        if (grow_by_factor) {
            min_capacity = GrowByFactor(min_capacity);
        }
        return Resize(min_capacity, false);
    }

    /// \brief Return a capacity expanded by a growth factor of 2
    static int64_t GrowByFactor(const int64_t min_capacity) {
        // If the capacity was not already a multiple of 2, do so here
        // TODO(emkornfield) doubling isn't great default allocation practice
        // see https://github.com/facebook/folly/blob/master/folly/docs/FBVector.md
        // for discussion
        return BitUtils::NextPower2(min_capacity);
    }

    /// \brief Append the given data to the buffer
    ///
    /// The buffer is automatically expanded if necessary.
    int Append(const void* data, const int64_t length) {
        if (PIL_PREDICT_FALSE(size_ + length > capacity_)) {
            int ret = Resize(GrowByFactor(size_ + length), false);
            if(ret != 1) return -1;
        }
        UnsafeAppend(data, length);
        return(1);
        //return Status::OK();
    }

    /// \brief Append copies of a value to the buffer
    ///
    /// The buffer is automatically expanded if necessary.
    int Append(const int64_t num_copies, uint8_t value) {
        int ret = Reserve(num_copies, true);
        if(ret != 1) return(-1);
        UnsafeAppend(num_copies, value);
        //return Status::OK();
        return(1);
    }

    // Advance pointer and zero out memory
    int Advance(const int64_t length) { return Append(length, 0); }

    // Advance pointer, but don't allocate or zero memory
    void UnsafeAdvance(const int64_t length) { size_ += length; }

    // Unsafe methods don't check existing size
    void UnsafeAppend(const void* data, const int64_t length) {
        memcpy(data_ + size_, data, static_cast<size_t>(length));
        size_ += length;
    }

    void UnsafeAppend(const int64_t num_copies, uint8_t value) {
        memset(data_ + size_, value, static_cast<size_t>(num_copies));
        size_ += num_copies;
    }

    void UnsafeSetLength(const int64_t length){ size_ = length; }

    int64_t capacity() const { return capacity_; }
    int64_t length() const { return size_; }
    const uint8_t* data() const { return data_; }
    uint8_t* mutable_data() { return data_; }

private:
    std::shared_ptr<ResizableBuffer> buffer_;
    MemoryPool* pool_;
    uint8_t* data_;
    int64_t capacity_;
    int64_t size_;
};

}


#endif /* BUFFER_BUILDER_H_ */
