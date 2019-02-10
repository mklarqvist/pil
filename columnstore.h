#ifndef COLUMNSTORE_H_
#define COLUMNSTORE_H_

#include <cstdint>
#include <memory>
#include <algorithm>
#include <cassert>
#include <iostream>

#include "pil.h"
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
        sorted(false), n(0), m(0), mem_use(0), checksum(0),
        stats_surrogate_min(std::numeric_limits<int64_t>::min()), stats_surrogate_max(std::numeric_limits<int64_t>::max()),
        pool_(pool)
    {
    }

    uint32_t size() const { return n; }
    uint32_t capacity() const { return m; }
    uint32_t GetMemoryUsage() const { return mem_use; }

    // Pointer to data.
    std::shared_ptr<ResizableBuffer> data() { return buffer; }
    std::shared_ptr<ResizableBuffer> transforms() { return transformation_args; }

    // PrettyPrint representation of array suitable for debugging.
    std::string ToString() const;

public:
    bool sorted; // is this ColumnStore sorted (relative itself)
    uint32_t n, m, mem_use; // number of elements -> check validity such that n*sizeof(primitive_type)==buffer.size()
    //uint32_t ptype, ptype_array; // primtive type encoding, possible bit use for signedness
    uint32_t checksum; // checksum for buffer
    int64_t stats_surrogate_min, stats_surrogate_max;
    std::vector<uint32_t> transformations; // order of transformations:
                                           // most usually simply PIL_COMPRESS_ZSTD or more advanced use-cases like
                                           // PIL_TRANSFORM_SORT, PIL_ENCODE_DICTIONARY, or PIL_COMPRESS_ZSTD

    // Any memory is owned by the respective Buffer instance (or its parents).
    MemoryPool* pool_;
    std::shared_ptr<ResizableBuffer> buffer; // Actual data BLOB
    std::shared_ptr<ResizableBuffer> transformation_args; // BLOB storing the parameters for the transformation operations.
};

template <class T>
struct ColumnStoreBuilder : public ColumnStore {
public:
    ColumnStoreBuilder(MemoryPool* pool) : ColumnStore(pool){}

    int Append(const T& value) {
        if(buffer.get() == nullptr){
            //std::cerr << "first allocation" << std::endl;
            assert(AllocateResizableBuffer(pool_, 4096*sizeof(T), &buffer) == 1);
            m = 4096;
            //std::cerr << "buffer=" << buffer->capacity() << std::endl;
        }

        //std::cerr << n << "/" << m << ":" << buffer->capacity() << std::endl;

        if(n == m){
            //std::cerr << "here in limit=" << n*sizeof(T) << "/" << buffer->capacity() << std::endl;
            assert(buffer->Reserve(n*sizeof(T) + 4096*sizeof(T)) == 1);
            m = n + 4096;
        }

        reinterpret_cast<T*>(buffer->mutable_data())[n++] = value;
        mem_use += sizeof(T);
        return(1);
    }

    const T* data() const { return reinterpret_cast<const T*>(buffer->mutable_data()); }
    T front() const { return data()[0]; }
    T back() const { return data()[n - 1]; }
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
    ColumnSet() :
        n(0), m(0), checksum(0)
    {
    }

    size_t size() const { return(columns.size()); }
    uint32_t GetMemoryUsage() const {
        uint32_t total = 0;
        for(int i = 0; i < size(); ++i)
            total += columns[i]->GetMemoryUsage();

        return(total);
    }

    void clear() {
        n = 0;
        checksum = 0;
        columns.clear();
    }

public:
    uint32_t n, m; // batch size of vectors are set to 4096 by default.
    uint32_t checksum; // checksum of the checksum vector -> md5(&checksums, n); this check is to guarantee there is no accidental reordering of the set
    std::vector< std::shared_ptr<ColumnStore> > columns;
};

template <class T>
struct ColumnSetBuilder : public ColumnSet {
public:
    ColumnSetBuilder(MemoryPool* pool) : ColumnSet(){}

    int Append(const T& value) {
        // Check if columns[0] is set
        //std::cerr << "appending single value: " << value << std::endl;
        if(columns.size() == 0) {
            //std::cerr << "pushing back first column" << std::endl;
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            ++n;
        }
        int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[0])->Append(value);
        assert(ret == 1);

        for(int i = 1; i < columns.size(); ++i){
            int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(0);
            assert(ret == 1);
        }

        return(1);
    }

    int Append(const std::vector<T>& values) {
        // Add values up until N
        if(n < values.size()) {
            //std::cerr << "adding more columns: " << columns.size() << "->" << values.size() << std::endl;
            const int start_size = columns.size();
            for(int i = start_size; i < values.size(); ++i, ++n)
                columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );

            assert(n >= values.size());

            // todo: insert padding from 0 to current offset in record batch
            const uint32_t padding_to = columns[0]->n;
            // Pad every column added this way.
            for(int i = start_size; i < values.size(); ++i){
                //std::cerr << i << "/" << values.size() << " -> (WIDTH) padding up to: " << padding_to << std::endl;
                for(int j = 0; j < padding_to; ++j){
                   int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(0);
                   assert(ret == 1);
                }
            }
        }
        //std::cerr << columns.size() << "/" << values.size() << "/" << n << std::endl;

        for(int i = 0; i < values.size(); ++i){
            int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(values[i]);
            assert(ret == 1);
        }

        //std::cerr << "add null from: " << n_values << "-" << columns.size() << std::endl;
        for(int i = values.size(); i < columns.size(); ++i){
            int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(0);
            assert(ret == 1);
        }

        //std::cerr << "addition done" << std::endl;

        return(1);
    }

    int Append(T* value, int n_values) {
        //std::cerr << "in append" << std::endl;
        //std::cerr << "adding=" << n_values << " values @ " << columns.size() << std::endl;
        // Add values up until N
        if((int)n < n_values){
            //std::cerr << "adding more columns: " << columns.size() << "->" << n_values << std::endl;
            const int start_size = columns.size();
            for(int i = start_size; i < n_values; ++i, ++n)
                columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );

            assert((int)n >= n_values);

            // todo: insert padding from 0 to current offset in record batch
            const uint32_t padding_to = columns[0]->n;
            // Pad every column added this way.
            for(int i = start_size; i < n_values; ++i){
                //std::cerr << i << "/" << n_values << " -> (WIDTH) padding up to: " << padding_to << std::endl;
                for(int j = 0; j < padding_to; ++j){
                   int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(0);
                   assert(ret == 1);
                }
            }
        }
        //std::cerr << columns.size() << "/" << n_values << "/" << n << std::endl;

        for(int i = 0; i < n_values; ++i){
            int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(value[i]);
            assert(ret == 1);
        }

        //std::cerr << "add null from: " << n_values << "-" << columns.size() << std::endl;
        for(int i = n_values; i < columns.size(); ++i){
            int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(0);
            assert(ret == 1);
        }

        //std::cerr << "addition done" << std::endl;

        return(1);
    }

    /**<
     * Pad all the ColumnStores in this ColumnSet with NULL values.
     * @return
     */
    int PadNull() {
        for(int i = 0; i < columns.size(); ++i){
            int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(0);
            assert(ret == 1);
        }
        return(1);
    }

    std::vector<int64_t> ColumnLengths() const{
        std::vector<int64_t> lengths;
        for(int i = 0; i < n; ++i)
            lengths.push_back(columns[i]->n);

        return(lengths);
    }

public:

};

}



#endif /* COLUMNSTORE_H_ */
