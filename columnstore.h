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

// The ColumnMetaData is stored separate from the ColumnSet and ColumnStores
// themselves and is used in an Indexing capacity. For example, to perform
// Segmental Elimination for predicate pushdown in selection queries.
struct ColumnMetaData {
    bool sorted; // is this ColumnStore sorted (relative itself)
    uint32_t n, m, uncompressed_size, compressed_size; // number of elements
    uint8_t md5_checksum[16]; // checksum for buffer
    int64_t stats_surrogate_min, stats_surrogate_max; // cast to actual ptype, any possible remainder is 0
    uint64_t file_offset; // file offset on disk to seek to the start of this ColumnStore
};

struct ColumnSetMetaData {
    std::vector< std::shared_ptr<ColumnMetaData> > column_meta_data;
};

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
        sorted(false), n(0), m(0), uncompressed_size(0), compressed_size(0),
        stats_surrogate_min(std::numeric_limits<int64_t>::min()), stats_surrogate_max(std::numeric_limits<int64_t>::max()),
        pool_(pool)
    {
    }

    uint32_t size() const { return n; }
    uint32_t capacity() const { return m; }
    uint32_t GetMemoryUsage() const { return uncompressed_size; }

    // Pointer to data.
    std::shared_ptr<ResizableBuffer> data() { return buffer; }
    uint8_t* mutable_data() { return buffer->mutable_data(); }
    std::shared_ptr<ResizableBuffer> transforms() { return transformation_args; }

    // PrettyPrint representation of array suitable for debugging.
    std::string ToString() const;

public:
    bool sorted; // is this ColumnStore sorted (relative itself)
    uint32_t n, m, uncompressed_size, compressed_size; // number of elements -> check validity such that n*sizeof(primitive_type)==buffer.size()
    uint8_t md5_checksum[16]; // checksum for buffer
    int64_t stats_surrogate_min, stats_surrogate_max; // cast to actual ptype, any possible remainder is 0
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

    int ComputeSegmentStats() {
        T* values = reinterpret_cast<T*>(mutable_data());
        if(uncompressed_size / sizeof(T) != n){
            std::cerr << "ERROR: " << uncompressed_size/sizeof(T) << "!=" << n << std::endl;
        }
        T min = std::numeric_limits<T>::max();
        T max = std::numeric_limits<T>::min();

        for(int i = 0; i < n; ++i) {
            min = std::min(min, values[i]);
            max = std::max(max, values[i]);
        }

        // Cast the placeholder bytes to the target type and assign
        // the minimum and maximum values to it.
        *reinterpret_cast<T*>(&stats_surrogate_min) = min;
        *reinterpret_cast<T*>(&stats_surrogate_max) = max;

        std::cerr << "min-max: " << (int)min << "-" << (int)max << std::endl;
        return(1);
    }

    int Append(const T& value) {
        if(buffer.get() == nullptr){
            //std::cerr << "first allocation" << std::endl;
            assert(AllocateResizableBuffer(pool_, 16384*sizeof(T), &buffer) == 1);
            m = 16384;
            //std::cerr << "buffer=" << buffer->capacity() << std::endl;
        }

        //std::cerr << n << "/" << m << ":" << buffer->capacity() << std::endl;

        if(n == m){
            //std::cerr << "here in limit=" << n*sizeof(T) << "/" << buffer->capacity() << std::endl;
            assert(buffer->Reserve(n*sizeof(T) + 16384*sizeof(T)) == 1);
            m = n + 16384;
            //std::cerr << "now=" << buffer->capacity() << std::endl;
        }

        reinterpret_cast<T*>(mutable_data())[n++] = value;
        uncompressed_size += sizeof(T);
        return(1);
    }

    int Append(const T* value, uint32_t n_values) {
        if(buffer.get() == nullptr){
            //std::cerr << "first allocation" << std::endl;
            assert(AllocateResizableBuffer(pool_, 16384*sizeof(T), &buffer) == 1);
            m = 16384;
            //std::cerr << "buffer=" << buffer->capacity() << std::endl;
        }

        //std::cerr << n << "/" << m << ":" << buffer->capacity() << std::endl;

        if(n == m || n + n_values >= m){
            //std::cerr << "here in limit=" << n*sizeof(T) << "/" << buffer->capacity() << std::endl;
            assert(buffer->Reserve(n*sizeof(T) + 16384*sizeof(T)) == 1);
            m = n + 16384;
        }

        T* dat = reinterpret_cast<T*>(mutable_data());
        memcpy(&dat[n], value, n_values * sizeof(T));
        n += n_values;
        uncompressed_size += n_values * sizeof(T);
        return(1);
    }

    const T* data() const { return reinterpret_cast<const T*>(mutable_data()); }
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
        n(0), m(0)
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
        n = 0, m = 0;
        memset(md5_checksum, 0, 16);
        columns.clear();
    }

public:
    uint32_t n, m;
    uint8_t md5_checksum[16]; // checksum of the checksum vector -> md5(&checksums, n); this check is to guarantee there is no accidental reordering of the set
    std::vector< std::shared_ptr<ColumnStore> > columns;
};

template <class T>
struct ColumnSetBuilder : public ColumnSet {
public:
    ColumnSetBuilder(MemoryPool* pool) : ColumnSet(){}

    int ComputeSegmentStats() {
        for(int i = 0; i < columns.size(); ++i) {
            int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->ComputeSegmentStats();
            assert(ret != -1);
        }
        return(1);
    }

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

template <class T>
struct ColumnSetBuilderTensor : public ColumnSet {
public:
    ColumnSetBuilderTensor(MemoryPool* pool) : ColumnSet(){}

    int ComputeSegmentStats() {
        for(int i = 0; i < columns.size(); ++i) {
            int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->ComputeSegmentStats();
            assert(ret != -1);
        }
        return(1);
    }

    int Append(const T& value) {
        // Check if columns[0] is set
        //std::cerr << "appending single value: " << value << std::endl;
        if(columns.size() == 0) {
            //std::cerr << "pushing back first column" << std::endl;
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            n += 2;
        }
        assert(n == 2);

        if(columns[0]->n == 0) {
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
            ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(1);
            assert(ret == 1);
        } else {
            const uint32_t n_recs = columns[0]->n;
            const uint32_t cum = reinterpret_cast<uint32_t*>(columns[0]->mutable_data())[n_recs - 1];
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(cum + 1);
            assert(ret == 1);
        }

        int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[1])->Append(value);
        assert(ret == 1);

        return(1);
    }

    int Append(const std::vector<T>& values) {
        if(columns.size() == 0) {
            //std::cerr << "pushing back first column" << std::endl;
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            n += 2;
        }
        assert(n == 2);

        for(int i = 0; i < values.size(); ++i){
            int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[1])->Append(values[i]);
            assert(ret == 1);
        }

        if(columns[0]->n == 0) {
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
            ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(values.size());
            assert(ret == 1);
        } else {
            const uint32_t n_recs = columns[0]->n;
            const uint32_t cum = reinterpret_cast<uint32_t*>(columns[0]->mutable_data())[n_recs - 1];
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(cum + values.size());
            assert(ret == 1);
        }

        return(1);
    }

    int Append(T* value, int n_values) {
        if(columns.size() == 0) {
            //std::cerr << "pushing back first column" << std::endl;
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            n += 2;
        }
        assert(n == 2);

        int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[1])->Append(value, n_values);
        assert(ret == 1);

        if(columns[0]->n == 0) {
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
            ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(n_values);
            assert(ret == 1);
        } else {
            const uint32_t n_recs = columns[0]->n;
            const uint32_t cum = reinterpret_cast<uint32_t*>(columns[0]->mutable_data())[n_recs - 1];
            //std::cerr << "appending: " << n_recs << " for " << cum + n_values << " as " << cum << "+" << n_values << std::endl;
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(cum + n_values);
            assert(ret == 1);
        }

        return(1);
    }

    /**<
     * Padding a Tensor-style ColumnStore simply involves adding an offset of 0 and
     * setting the appropriate Null-vector bit.
     * @return
     */
    int PadNull() {
        if(columns[0]->n == 0) {
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
            ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
        } else {
            const uint32_t n_recs = columns[0]->n;
            const uint32_t cum = reinterpret_cast<uint32_t*>(columns[0]->mutable_data())[n_recs];
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(cum + 0);
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
