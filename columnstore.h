#ifndef COLUMNSTORE_H_
#define COLUMNSTORE_H_

#include <cstdint>
#include <memory>
#include <algorithm>
#include <cassert>
#include <iostream>

#include "buffer.h"

namespace pil {

// ColumnStore can store ANY primitive type: e.g. CHROM, POS
// Initiate the ColumnStore with the largest word size of a primitive family type.
// For example, if you are working with uint8_t values then initiate the ptype
// to uint32_t. Similarly, for precision ptypes, start out with a double. Shrink
// the primitive type in the Segment of a ColumnStore.
//
// Default length of a ColumnStore should be 4096 elements.
//
// A ColumnStore is **MUTABLE** and should be used during importing/constructing
// procedures only. Retrieving data should take place through Array structs and
// downcast to one of its concrete types.
struct ColumnStore {
public:
    ColumnStore(MemoryPool* pool) :
        sorted(false), n(0), m(0), ptype(0), checksum(0), pool_(pool)
    {
    }

    //int64_t GetLength() const { return buffer.data->n; }
    //int64_t GetOffset() const { return buffer.data->offset; }

    int GetType() const { return ptype; }

    // Pointer to data.
    //std::shared_ptr<ResizableBuffer> data() { return buffer; }
    //std::shared_ptr<ResizableBuffer> bitmap() { return bitmap; }
    //std::shared_ptr<ResizableBuffer> stats() { return segment_statistics_buffer; }

    // PrettyPrint representation of array suitable for debugging.
    std::string ToString() const;

public:
    bool sorted; // is this ColumnStore sorted (relative itself)
    uint32_t n, m; // number of elements -> check validity such that n*sizeof(primitive_type)==buffer.size()
    uint32_t ptype; // primtive type encoding, possible bit use for signedness
    uint32_t checksum; // checksum for buffer
    std::vector<uint32_t> transformations; // order of transformations:
                                           // most usually simply PIL_COMPRESS_ZSTD or more advanced use-cases like
                                           // PIL_TRANSFORM_SORT, PIL_ENCODE_DICTIONARY, or PIL_COMPRESS_ZSTD

    // Any memory is owned by the respective Buffer instance (or its parents).
    MemoryPool* pool_;
    std::shared_ptr<ResizableBuffer> buffer; // Actual data BLOB
    std::shared_ptr<ResizableBuffer> transformation_args; // BLOB storing the parameters for the transformation operations.
    std::shared_ptr<ResizableBuffer> segment_statistics_buffer; // when searching for overlap this MUST be cast to the appropriate primitive type};
};

template <class T>
struct ColumnStoreBuilder : public ColumnStore {
public:
    ColumnStoreBuilder(MemoryPool* pool) : ColumnStore(pool){}

    int Append(const T& value) {
        if(buffer.get() == nullptr){
            std::cerr << "first allocation" << std::endl;
            assert(AllocateResizableBuffer(pool_, 4096*sizeof(T), &buffer) == 1);
            m = 4096;
            std::cerr << "buffer=" << buffer->capacity() << std::endl;
        }

        //std::cerr << n << "/" << m << ":" << buffer->capacity() << std::endl;

        if(n == m){
            std::cerr << "here in limit=" << n*sizeof(T) << "/" << buffer->capacity() << std::endl;
            assert(buffer->Resize(n*sizeof(T) + 4096*sizeof(T)) == 1);
            m = n + 4096;
        }

        reinterpret_cast<T*>(buffer->mutable_data())[n++] = value;
        return(1);
    }

    const T* data() const { return reinterpret_cast<const T*>(buffer->mutable_data()); }
    T front() const { return data()[0]; }
    T back() const { return data()[n - 1]; }

public:

};


// ColumnSet groups any number of ColumnStores into a joint set of
// columns. This is useful when there are variable length vectors
// of values associated with a single field. This can potentially occur in all
// valid fields such as ALT and in INFO and FORMAT fields.
//
// When ANY of the ColumnStore objects reach the upper limit (batch size) they
// will ALL be processed en-mass and the encapsulaing Segment will be
// processed and flushed.
struct ColumnSet {
public:
    ColumnSet(MemoryPool* pool) :
        global_id(-1), n(0), m(0), checksum(0), pool_(pool), stride(std::make_shared<ColumnStore>(pool))
    {
    }

public:
    int32_t global_id;
    uint32_t n, m; // batch size of vectors are set to 4096 by default.
    uint32_t checksum; // checksum of the checksum vector -> md5(&checksums, n); this check is to guarantee there is no accidental reordering of the set
    // Any memory is owned by the respective Buffer instance (or its parents).
    MemoryPool* pool_;
    std::shared_ptr<ColumnStore> stride; // "Stride" size of columns for each record.
    std::vector< std::shared_ptr<ColumnStore> > columns;
};

template <class T>
struct ColumnSetBuilder : public ColumnSet {
public:
    ColumnSetBuilder(MemoryPool* pool) : ColumnSet(pool){}

    int Append(const T& value) {
        // Check if columns[0] is set
        //std::cerr << "appending single value: " << value << std::endl;
        if(columns.size() == 0){
            std::cerr << "pushing back first column" << std::endl;
            columns.push_back( std::make_shared<ColumnStore>(pool_) );
            ++n;
        }
        std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[0])->Append(value);
        UpdateDictionary(1);
        return(1);
    }

    int Append(const std::vector<T>& values) {
        // Add values up until N
        if(n < values.size()){
            std::cerr << "adding more columns: " << columns.size() << "->" << values.size() << std::endl;
            const int start_size = columns.size();
            for(int i = start_size; i < values.size(); ++i, ++n)
                columns.push_back( std::make_shared<ColumnStore>(pool_) );

            assert(n >= values.size());
        }
        //std::cerr << columns.size() << "/" << values.size() << "/" << n << std::endl;

        for(int i = 0; i < values.size(); ++i){
            std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(values[i]);
        }
        UpdateDictionary(values.size());

        //std::cerr << "addition done" << std::endl;

        return(1);
    }

    int Append(T* value, int n_values) {
        //std::cerr << "in append" << std::endl;
        //std::cerr << "addding=" << n_values << " values" << std::endl;
        // Add values up until N
        if((int)n < n_values){
            std::cerr << "adding more columns: " << columns.size() << "->" << n_values << std::endl;
            const int start_size = columns.size();
            for(int i = start_size; i < n_values; ++i, ++n)
                columns.push_back( std::make_shared<ColumnStore>(pool_) );

            assert((int)n >= n_values);
        }
        //std::cerr << columns.size() << "/" << n_values << "/" << n << std::endl;

        for(int i = 0; i < n_values; ++i){
            std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(value[i]);
        }
        UpdateDictionary(n_values);

        //std::cerr << "addition done" << std::endl;

        return(1);
    }

    std::vector<int64_t> ColumnLengths() const{
        std::vector<int64_t> lengths;
        for(int i = 0; i < n; ++i)
            lengths.push_back(columns[i]->n);

        return(lengths);
    }

private:
    int UpdateDictionary(int32_t value) {
        std::static_pointer_cast< ColumnStoreBuilder<int32_t> >(stride)->Append(value);
        //std::cerr << "dict size=" << std::static_pointer_cast< ColumnStoreBuilder<int32_t> >(dict)->n << std::endl;
        return(1);
    }

public:

};

}



#endif /* COLUMNSTORE_H_ */
