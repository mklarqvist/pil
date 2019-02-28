#ifndef COLUMN_DICTIONARY_H_
#define COLUMN_DICTIONARY_H_

#include "../buffer.h"

namespace pil {

class ColumnDictionary {
public:
    explicit ColumnDictionary(MemoryPool* mpool = default_memory_pool()) : n_records(0), n_elements(0), sz_u(0), sz_c(0), pool(mpool){}
    virtual ~ColumnDictionary(){}

    int64_t Get(const int64_t p) const;
    int64_t Find(const int64_t p) const;
    bool Contains(const int64_t p) const;

    uint8_t* mutable_data() { return buffer->mutable_data(); }
    int64_t GetCompressedSize() const { return(sz_c); }
    int64_t GetUncompressedSize() const { return(sz_u); }
    void UnsafeSetCompressedSize(const int64_t size) { sz_c = size; }

protected:
    int64_t n_records, n_elements;
    int64_t sz_u, sz_c;
    MemoryPool* pool;
    std::shared_ptr<ResizableBuffer> buffer;
};

}



#endif /* COLUMN_DICTIONARY_H_ */
