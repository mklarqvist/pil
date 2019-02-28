#ifndef COLUMN_DICTIONARY_H_
#define COLUMN_DICTIONARY_H_

#include "../buffer.h"

namespace pil {

class ColumnDictionary {
public:
    explicit ColumnDictionary(MemoryPool* mpool = default_memory_pool()) : have_lengths(false), n_records(0), n_elements(0), sz_u(0), sz_c(0), sz_lu(0), sz_lc(0), pool(mpool){}
    virtual ~ColumnDictionary(){}

    int64_t Get(const int64_t p) const;
    int64_t Find(const int64_t p) const;
    template <class T>
    bool Contains(const T p) const;
    bool Contains(const uint8_t* in, const uint32_t length) const;

    uint8_t* mutable_data() { return buffer->mutable_data(); }
    uint8_t* mutable_length_data() { return lengths->mutable_data(); }
    int64_t GetCompressedSize() const { return(sz_c); }
    int64_t GetUncompressedSize() const { return(sz_u); }
    int64_t GetCompressedLengthSize() const { return(sz_lc); }
    int64_t GetUncompressedLengthSize() const { return(sz_lu); }
    void UnsafeSetCompressedSize(const int64_t size) { sz_c = size; }
    void UnsafeSetCompressedLengthSize(const int64_t size) { sz_lc = size; }

protected:
    bool have_lengths;
    int64_t n_records, n_elements;
    int64_t sz_u, sz_c, sz_lu, sz_lc;
    MemoryPool* pool;
    std::shared_ptr<ResizableBuffer> buffer;
    std::shared_ptr<ResizableBuffer> lengths;
};

}



#endif /* COLUMN_DICTIONARY_H_ */
