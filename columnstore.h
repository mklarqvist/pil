#ifndef COLUMNSTORE_H_
#define COLUMNSTORE_H_

#include <cstdint>
#include <memory>
#include <algorithm>
#include <cassert>
#include <iostream>
#include <cmath>
#include <vector>

#include "pil.h"
#include "buffer_builder.h"

// Todo: fix -> this is for encoder meta
#include "transform/transform_meta.h"
#include "column_dictionary.h"
//#include "bit_utils.h"

namespace pil {

// ColumnStore can store ANY primitive type: e.g. CHROM, POS
//
// A ColumnStore is **MUTABLE** and should be used during importing/constructing
// procedures **ONLY**. Retrieving data should take place through Array structs and
// downcast to one of its concrete types.
class ColumnStore {
public:
    explicit ColumnStore(MemoryPool* pool = default_memory_pool()) :
        have_dictionary(false),
        n_records(0), n_elements(0), n_null(0), uncompressed_size(0), compressed_size(0),
        m_nullity(0), nullity_u(0), nullity_c(0),
        pool_(pool)
    {
    }

    uint32_t size() const { return n_records; }

    uint32_t GetMemoryUsage() const {
        uint32_t ret = uncompressed_size + nullity_u;
        if(have_dictionary) ret += dictionary->GetUncompressedSize();
        return(ret);
    }

    // Pointer to data.
    //std::shared_ptr<ResizableBuffer> data() { return buffer; }
    uint8_t* mutable_data() { return buffer.mutable_data(); }
    //std::shared_ptr<ResizableBuffer> transforms() { return transformation_args; }

    // PrettyPrint representation of array suitable for debugging.
    std::string ToString() const;

    // Serialize/deserialize to/from disk
    int Serialize(std::ostream& stream);

    // Deserialization is for DEBUG use only. Otherwise, use the
    // correct concrete Array types.
    int Deserialize(std::ostream& stream);

    // Check if the given element is valid by looking up that bit in the bitmap.
    bool IsValid(const uint32_t p) { return(reinterpret_cast<uint32_t*>(nullity->mutable_data())[p / 32] & (1 << (p % 32))); }

public:
    bool have_dictionary;
    uint32_t n_records, n_elements, n_null;
    uint32_t uncompressed_size, compressed_size; // number of elements -> check validity such that n*sizeof(primitive_type)==buffer.size()
    uint32_t m_nullity, nullity_u, nullity_c; // nullity_u is not required as we can compute it. but is convenient to have during deserialization

    // Any memory is owned by the respective Buffer instance (or its parents).
    MemoryPool* pool_;
    BufferBuilder buffer;
    std::shared_ptr<ResizableBuffer> nullity; // NULLity vector Todo: make into structure
    std::shared_ptr<ColumnDictionary> dictionary; // Dictionary used for predicate pushdown Todo: make into structure
    std::vector< std::shared_ptr<TransformMeta> > transformation_args; // Every transform MUST store a value.
};

template <class T>
class ColumnStoreBuilder : public ColumnStore {
public:
    explicit ColumnStoreBuilder(MemoryPool* pool = default_memory_pool()) : ColumnStore(pool){}

    /**<
     * Set the validity of the current object in the Nullity/Validity bitmap.
     * This function MUST be called BEFORE the appending data to make sure that
     * the correct position is set.
     * @param yes Logical flag set to TRUE if the data is VALID or FALSE otherwise.
     * @return
     */
    int AppendValidity(const bool yes, const int32_t adjust = 0) {
        if(nullity.get() == nullptr) {
            assert(AllocateResizableBuffer(pool_, 16384*sizeof(uint32_t), &nullity) == 1);
            //nullity->ZeroPadding();
            memset(nullity->mutable_data(), 0, sizeof(uint32_t)*16384);
            m_nullity = 16384 * 32; // 32 bits for every integer used in the Nullity bitmap
        }

        if(n_records == m_nullity) {
            //std::cerr << "should never happen" << std::endl;
            //exit(1);
            assert(nullity->Reserve(n_records*(uint32_t) + 16384*sizeof(uint32_t)) == 1);
            //nullity->ZeroPadding();
            m_nullity = n_records*32 + 16384 * 32;
            //memset(&nullity->mutable_data()[n*sizeof(uint32_t)], 0, );
            std::cerr << "TODO" << std::endl;
            exit(1);
        }

        n_null += (yes == false);
        reinterpret_cast<uint32_t*>(nullity->mutable_data())[(n_records - adjust) / 32] |= (yes << ((n_records - adjust) % 32));
        return 1;
    }

    int Append(const T value) {
        buffer.Append(reinterpret_cast<const uint8_t*>(&value), sizeof(T));
        ++n_records;
        ++n_elements;
        uncompressed_size += sizeof(T);
        return(1);
    }

    int Append(const T* value, uint32_t n_values) {
        buffer.Append(reinterpret_cast<const uint8_t*>(value), sizeof(T)*n_values);
        ++n_records;
        n_elements += n_values;
        uncompressed_size += n_values * sizeof(T);
        return(1);
    }

    const T* data() const { return reinterpret_cast<const T*>(buffer.data()); }
};


// ColumnSet groups any number of ColumnStores into a joint set of
// columns. This is useful when there are variable length vectors
// of values associated with a single field. This can potentially occur in all
// valid fields such as ALT and in INFO and FORMAT fields.
//
// When ANY of the ColumnStore objects reach the upper limit (batch size) they
// will ALL be processed en-mass and the encapsulaing Segment will be
// processed and flushed.
class ColumnSet {
public:
    explicit ColumnSet(MemoryPool* pool = default_memory_pool()) :
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
class ColumnSetBuilder : public ColumnSet {
public:
    explicit ColumnSetBuilder(MemoryPool* pool = default_memory_pool()) : ColumnSet(pool){}

    int Append(const T value) {
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
            const uint32_t padding_to = columns[0]->n_records;
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

    int Append(const T* value, int n_values) {
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
            const uint32_t padding_to = columns[0]->n_records;
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
            lengths.push_back(columns[i]->n_records);

        return(lengths);
    }

public:

};

template <class T>
class ColumnSetBuilderTensor : public ColumnSet {
public:
    explicit ColumnSetBuilderTensor(MemoryPool* pool = default_memory_pool()) : ColumnSet(pool){}

    int Append(const T value) {
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
        if(columns[0]->n_records == 0) {
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(true);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
            ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(1);
            assert(ret == 1);
        } else {
            const uint32_t n_recs = columns[0]->n_records;
            assert(n_recs != 0);
            const uint32_t cum = reinterpret_cast<uint32_t*>(columns[0]->mutable_data())[n_recs - 1];
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(true, 1);
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

        if(columns[0]->n_records == 0) {
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(true);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
            ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(values.size());
            assert(ret == 1);
        } else {
            const uint32_t n_recs = columns[0]->n_records;
            assert(n_recs != 0);
            const uint32_t cum = reinterpret_cast<uint32_t*>(columns[0]->mutable_data())[n_recs - 1];
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(true, 1);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(cum + values.size());
            assert(ret == 1);
        }

        int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(columns[1])->Append(values.data(), values.size());
        assert(ret == 1);

        return(1);
    }

    int Append(const T* value, int n_values) {
        if(columns.size() == 0) {
            //std::cerr << "pushing back first column" << std::endl;
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            columns.push_back( std::make_shared<ColumnStore>(pil::default_memory_pool()) );
            n += 2;
        }
        assert(n == 2);

        if(columns[0]->n_records == 0) {
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(true);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
            ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(n_values);
            assert(ret == 1);
        } else {
            const uint32_t n_recs = columns[0]->n_records;
            assert(n_recs != 0);
            const uint32_t cum = reinterpret_cast<uint32_t*>(columns[0]->mutable_data())[n_recs - 1];
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(true, 1);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(cum + n_values);
            assert(ret == 1);
        }

        //std::cerr << "appending=" << n_values << std::endl;
        assert(n_values > 0);
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

        if(columns[0]->n_records == 0) {
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(false);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
            ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(0);
            assert(ret == 1);
        } else {
            const uint32_t n_recs = columns[0]->n_records;
            assert(n_recs != 0);
            const uint32_t cum = reinterpret_cast<uint32_t*>(columns[0]->mutable_data())[n_recs - 1];
            std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->AppendValidity(false);
            int ret = std::static_pointer_cast< ColumnStoreBuilder<uint32_t> >(columns[0])->Append(cum + 0);
            assert(ret == 1);
        }
        return(1);
    }

    std::vector<int64_t> ColumnLengths() const{
        std::vector<int64_t> lengths;
        for(int i = 0; i < n; ++i)
            lengths.push_back(columns[i]->n_records);

        return(lengths);
    }
};

}



#endif /* COLUMNSTORE_H_ */
