#ifndef COLUMN_DICTIONARY_H_
#define COLUMN_DICTIONARY_H_

#include <iostream>
#include "buffer.h"

namespace pil {

typedef enum {
    PIL_DICT_SET_NULLPTR = -1,
    PIL_DICT_STORE_NULLPTR = -2,
    PIL_DICT_ILLEGAL_CSTORE = -3,
    PIL_DICT_MISSING_NULLITY = -4,
    PIL_DICT_MALFORMED = -5,
    PIL_DICT_NO_DATA = -6
} PIL_DICTIONARY_ERRORS;

class ColumnDictionary {
public:
    explicit ColumnDictionary(MemoryPool* mpool = default_memory_pool()) :
        have_lengths(false), n_records(0), n_elements(0), sz_u(0),
        sz_c(0), sz_lu(0), sz_lc(0), pool(mpool)
    {}

    virtual ~ColumnDictionary(){}

    template <class T>
    int Get(const int64_t p, T& dst) const {
        if(buffer.get() == nullptr) return(-1);
        if(sz_u % sizeof(T) != 0) {
           std::cerr << sz_u << "%" << sizeof(T) << "=" << (sz_u%sizeof(T)) << std::endl;
           return(-2);
       }

        if(p >= n_records) return(-4);

       if(have_lengths) {
           return(-5);
       }

       // retrieve data at pos p
       const T* data = reinterpret_cast<T*>(buffer->mutable_data());
       dst = data[p];
       return(1);
    }

    template <class T>
    int Get(const int64_t p, const T* dst, int64_t& dst_len) const {

    }

    //int64_t Find(const int64_t p) const;
    template <class T>
    int Contains(const T p) const {
        if(buffer.get() == nullptr) return(-1);
        if(sz_u % sizeof(T) != 0) {
            std::cerr << sz_u << "%" << sizeof(T) << "=" << (sz_u%sizeof(T)) << std::endl;
            return(-2);
        }

        const T* data = reinterpret_cast<T*>(buffer->mutable_data());

        if(have_lengths) {
            int64_t matches = 0;
            // Match ANY element to the value in question
            for(int i = 0; i < n_elements; ++i) {
                matches += (data[i] == p); // branchless
            }
            return(matches);
        }

        int64_t matches = 0;
        for(int i = 0; i < n_records; ++i) {
            matches += (data[i] == p); // branchless
        }
        return(matches);
    }

    template <class T>
    int Contains(const T* in, const uint32_t length) const {
        if(buffer.get() == nullptr) return(-1);
        if(sz_u % sizeof(T) != 0) {
            std::cerr << sz_u << "%" << sizeof(T) << "=" << (sz_u%sizeof(T)) << std::endl;
            return(-2);
        }
        if(have_lengths == false) return(-3);

        const T* data = reinterpret_cast<T*>(buffer->mutable_data());
        const uint32_t* l = reinterpret_cast<uint32_t*>(lengths->mutable_data());
        // Assumes data has been encoded as cumulative offsets during loading;
        int64_t matches = 0;
        int64_t cumsum = 0;
        for(int i = 0; i < n_records; ++i) {
            //std::cerr << "comparing"
            if(cumsum + length > n_elements) {
                //std::cerr << "breaking" << std::endl;
                break;
            }
            matches += (memcmp(in, &data[cumsum], length*sizeof(T)) == 0);
            //std::cerr << "striding=" << l[i] << std::endl;
            cumsum += l[i];
        }
        return(matches);
    }

    uint8_t* mutable_data() { return buffer->mutable_data(); }
    uint8_t* mutable_length_data() { return lengths->mutable_data(); }
    int64_t GetCompressedSize() const { return(sz_c); }
    int64_t GetUncompressedSize() const { return(sz_u); }
    int64_t GetCompressedLengthSize() const { return(sz_lc); }
    int64_t GetUncompressedLengthSize() const { return(sz_lu); }
    void UnsafeSetCompressedSize(const int64_t size) { sz_c = size; }
    void UnsafeSetCompressedLengthSize(const int64_t size) { sz_lc = size; }

    int64_t NumberRecords() const { return n_records; }
    int64_t NumberElements() const { return n_elements; }

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
