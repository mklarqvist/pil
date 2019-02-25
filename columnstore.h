#ifndef COLUMNSTORE_H_
#define COLUMNSTORE_H_

#include <cstdint>
#include <memory>
#include <algorithm>
#include <cassert>
#include <iostream>
#include <cmath>

#include "pil.h"
#include "buffer.h"
#include "bit_utils.h"

namespace pil {

// ColumnStore can store ANY primitive type: e.g. CHROM, POS
//
// A ColumnStore is **MUTABLE** and should be used during importing/constructing
// procedures **ONLY**. Retrieving data should take place through Array structs and
// downcast to one of its concrete types.
struct ColumnStore {
public:
    ColumnStore(MemoryPool* pool) :
        have_dictionary(false),
        n(0), m(0), uncompressed_size(0), compressed_size(0),
        m_nullity(0), nullity_u(0), nullity_c(0),
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

    // Serialize/deserialize to/from disk
    int Serialize(std::ostream& stream) {
        stream.write(reinterpret_cast<char*>(&n), sizeof(uint32_t));
        stream.write(reinterpret_cast<char*>(&m), sizeof(uint32_t));
        stream.write(reinterpret_cast<char*>(&uncompressed_size), sizeof(uint32_t));
        stream.write(reinterpret_cast<char*>(&compressed_size),   sizeof(uint32_t));
        stream.write(reinterpret_cast<char*>(&nullity_u), sizeof(uint32_t));
        stream.write(reinterpret_cast<char*>(&nullity_c), sizeof(uint32_t));

        uint32_t n_transforms = transformations.size();
        stream.write(reinterpret_cast<char*>(&n_transforms), sizeof(uint32_t));

        //assert(transformations.size() != 0);
        for(size_t i = 0; i < transformations.size(); ++i) {
            uint32_t t_type = transformations[i];
            stream.write(reinterpret_cast<char*>(&t_type), sizeof(uint32_t));
        }

        if(buffer.get() != nullptr) {
            // If the data has been transformed we write out the compressed data
            // otherwise we write out the uncompressed data.
            if(n_transforms != 0) {
                std::cerr << "writing transformed n= " << size() << " c=" << compressed_size << std::endl;
                stream.write(reinterpret_cast<char*>(buffer->mutable_data()), compressed_size);
            } else {
                std::cerr << "writing untransformed data= " << size() << " u=" << uncompressed_size << std::endl;
                stream.write(reinterpret_cast<char*>(buffer->mutable_data()), uncompressed_size);
            }
        }

        // Nullity vector
        //const uint32_t n_nullity = std::ceil((float)n / 32);
        if(nullity.get() != nullptr) {
            const uint32_t* nulls = reinterpret_cast<const uint32_t*>(nullity->mutable_data());
            stream.write(reinterpret_cast<const char*>(nulls), nullity_c);
        }

        // Dictionary encoding
        stream.write(reinterpret_cast<char*>(&have_dictionary), sizeof(bool));
        if(have_dictionary) {
            //stream.write()
        }

        return(1);
    }

    // Deserialization is for DEBUG use only. Otherwise, use the
    // correct concrete Array types.
    int Deserialize(std::ostream& stream);

    // Check if the given element is valid by looking up that bit in the bitmap.
    bool IsValid(const uint32_t p) { return(reinterpret_cast<uint32_t*>(nullity->mutable_data())[n / 32] & (1 << (p % 32))); }

public:
    bool have_dictionary;
    // Todo: address (https://app.asana.com/0/1111151872856814/1111157143617759)
    // int32_t num_values;
    // int32_t num_rows;
    // int32_t num_nulls;
    // bool is_compressed;
    uint32_t n, m, uncompressed_size, compressed_size; // number of elements -> check validity such that n*sizeof(primitive_type)==buffer.size()
    uint32_t m_nullity, nullity_u, nullity_c; // nullity_u is not required as we can compute it. but is nice to have during deserialization

    // It is disallowed to call Dictionary encoding as a non-final step
    // excluding compression. It is also disallowed to call Dictionary
    // encoding more than once (1).
    //
    // Allowed: transform 1, transform 2, dictionary encoding, compression
    // Allowed: transform, dictionary encoding, compression1, compression2
    // Disallowed: transform 1, compression, dictionary encoding
    // Disallowed: transform 1, dictionary encoding, transform 2, compression
    // Disallowed: dictionary encoding, transform 1
    std::vector<PIL_COMPRESSION_TYPE> transformations; // order of transformations:
                                                       // most usually simply PIL_COMPRESS_ZSTD or more advanced use-cases like
                                                       // PIL_TRANSFORM_SORT, PIL_ENCODE_DICTIONARY, or PIL_COMPRESS_ZSTD

    // Any memory is owned by the respective Buffer instance (or its parents).
    MemoryPool* pool_;
    std::shared_ptr<ResizableBuffer> buffer; // Actual data BLOB
    std::shared_ptr<ResizableBuffer> nullity; // NULLity vector Todo: make into structure
    std::shared_ptr<ResizableBuffer> dictionary; // Dictionary used for predicate pushdown Todo: make into structure
    std::shared_ptr<ResizableBuffer> transformation_args; // BLOB storing the parameters for the transformation operations.
                                                          // Every transform MUST store a value in the arguments.
};

template <class T>
struct ColumnStoreBuilder : public ColumnStore {
public:
    ColumnStoreBuilder(MemoryPool* pool) : ColumnStore(pool){}

    /**<
     * Set the validity of the current object in the Nullity/Validity bitmap.
     * This function MUST be called BEFORE the appending data to make sure that
     * the correct position is set.
     * @param yes Logical flag set to TRUE if the data is VALID or FALSE otherwise.
     * @return
     */
    int AppendValidity(const bool yes) {
        if(nullity.get() == nullptr) {
            assert(AllocateResizableBuffer(pool_, 16384*sizeof(uint32_t), &nullity) == 1);
            //nullity->ZeroPadding();
            memset(nullity->mutable_data(), 0, sizeof(uint32_t)*16384);
            m_nullity = 16384 * 32; // 32 bits for every integer used in the Nullity bitmap
        }

        if(n == m_nullity) {
            //std::cerr << "should never happen" << std::endl;
            //exit(1);
            assert(nullity->Reserve(n*(uint32_t) + 16384*sizeof(uint32_t)) == 1);
            //nullity->ZeroPadding();
            m_nullity = n*32 + 16384 * 32;
            //memset(&nullity->mutable_data()[n*sizeof(uint32_t)], 0, );
            std::cerr << "TODO" << std::endl;
            exit(1);
        }

        reinterpret_cast<uint32_t*>(nullity->mutable_data())[n / 32] |= (yes << (n % 32));
        return 1;
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
        n(0), m(0)
    {
    }

    size_t size() const { return(columns.size()); }

    uint32_t GetMemoryUsage() const {
        uint32_t total = 0;
        for(size_t i = 0; i < size(); ++i)
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

    int Append(const T& value) {
        // Check if columns[0] is set
        //std::cerr << "appending single value: " << value << std::endl;
        if(columns.size() == 0) {
            //std::cerr << "pushing back first column" << std::endl;
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            ++n;
        }

        std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[0])->AppendValidity(true);
        int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[0])->Append(value);
        assert(ret == 1);

        for(uint32_t i = 1; i < columns.size(); ++i){
            std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->AppendValidity(false);
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
            for(int i = start_size; i < values.size(); ++i) {
                //std::cerr << i << "/" << values.size() << " -> (WIDTH) padding up to: " << padding_to << std::endl;
                for(int j = 0; j < padding_to; ++j){
                   std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->AppendValidity(false);
                   int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(0);
                   assert(ret == 1);
                }
            }
        }
        //std::cerr << columns.size() << "/" << values.size() << "/" << n << std::endl;

        for(int i = 0; i < values.size(); ++i){
            std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->AppendValidity(true);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(values[i]);
            assert(ret == 1);
        }

        //std::cerr << "add null from: " << n_values << "-" << columns.size() << std::endl;
        for(int i = values.size(); i < columns.size(); ++i){
            std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->AppendValidity(false);
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
            for(int i = start_size; i < n_values; ++i) {
                //std::cerr << i << "/" << n_values << " -> (WIDTH) padding up to: " << padding_to << std::endl;
                for(uint32_t j = 0; j < padding_to; ++j) {
                   std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->AppendValidity(false);
                   int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(0);
                   assert(ret == 1);
                }
            }
        }
        //std::cerr << columns.size() << "/" << n_values << "/" << n << std::endl;

        for(int i = 0; i < n_values; ++i) {
            std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->AppendValidity(true);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->Append(value[i]);
            assert(ret == 1);
        }

        //std::cerr << "add null from: " << n_values << "-" << columns.size() << std::endl;
        for(uint32_t i = n_values; i < columns.size(); ++i){
            std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->AppendValidity(false);
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
        if(columns.size() == 0) {
            //std::cerr << "pushing back first column" << std::endl;
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            ++n;
        }

        //std::cerr << "in padnull: csize = " << columns.size() << std::endl;
        for(uint32_t i = 0; i < columns.size(); ++i) {
            std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[i])->AppendValidity(false);
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

        // If this is the first value added to the ColumnSet then we add an addition
        // 0 to the start such to support constant time lookup. This will resulting
        // in this column being n + 1 length.
        // We do NOT care about the Nullity vector for the data column as it has no meaning.
        if(columns[0]->n == 0) {
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(true);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
            ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(1);
            assert(ret == 1);
        } else {
            const uint32_t n_recs = columns[0]->n;
            const uint32_t cum = reinterpret_cast<uint32_t*>(columns[0]->mutable_data())[n_recs - 1];
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(true);
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

        if(columns[0]->n == 0) {
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(true);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
            ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(values.size());
            assert(ret == 1);
        } else {
            const uint32_t n_recs = columns[0]->n;
            const uint32_t cum = reinterpret_cast<uint32_t*>(columns[0]->mutable_data())[n_recs - 1];
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(true);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(cum + values.size());
            assert(ret == 1);
        }

        for(int i = 0; i < values.size(); ++i){
            int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[1])->Append(values[i]);
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

        if(columns[0]->n == 0) {
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(true);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
            ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(n_values);
            assert(ret == 1);
        } else {
            const uint32_t n_recs = columns[0]->n;
            const uint32_t cum = reinterpret_cast<uint32_t*>(columns[0]->mutable_data())[n_recs - 1];
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(true);
            //std::cerr << "appending: " << n_recs << " for " << cum + n_values << " as " << cum << "+" << n_values << std::endl;
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(cum + n_values);
            assert(ret == 1);
        }

        int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[1])->Append(value, n_values);
        assert(ret == 1);

        return(1);
    }

    /**<
     * Padding a Tensor-style ColumnStore simply involves adding an offset of 0 and
     * setting the appropriate Null-vector bit.
     * @return
     */
    int PadNull() {
        if(columns.size() == 0) {
            //std::cerr << "pushing back first column" << std::endl;
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            n += 2;
        }
        assert(n == 2);

        if(columns[0]->n == 0) {
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(false);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
            ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
        } else {
            const uint32_t n_recs = columns[0]->n;
            const uint32_t cum = reinterpret_cast<uint32_t*>(columns[0]->mutable_data())[n_recs];
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(false);
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
