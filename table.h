#ifndef TABLE_H_
#define TABLE_H_

#include <algorithm>
#include <unordered_map>

#include "table_dict.h"
#include "memory_pool.h"
#include "columnstore.h"
#include "compression/compressor.h"
#include "record_builder.h"

#include "encoder.h"
#include "compression/variant_digest_manager.h"

#include <fstream>

namespace pil {

// The ColumnMetaData is stored separate from the ColumnSet and ColumnStores
// themselves and is used in an Indexing capacity. For example, to perform
// Segmental Elimination for predicate pushdown in selection queries.
struct ColumnStoreMetaData {
public:
    template <class T>
    int ComputeSegmentStats(std::shared_ptr<ColumnStore> cstore) {
        T* values = reinterpret_cast<T*>(cstore->buffer->mutable_data());
        T min = std::numeric_limits<T>::max();
        T max = std::numeric_limits<T>::min();

        for(int i = 0; i < n; ++i) {
            min = std::min(min, values[i]);
            max = std::max(max, values[i]);
        }

        // Cast the placeholder bytes to the target type and assign
        // the minimum and maximum values to it.
        stats_surrogate_min = 0;
        stats_surrogate_max = 0;
        *reinterpret_cast<T*>(&stats_surrogate_min) = min;
        *reinterpret_cast<T*>(&stats_surrogate_max) = max;

        std::cerr << "min-max: " << (int)min << "-" << (int)max << std::endl;
        return(1);
    }

    int ComputeChecksum(std::shared_ptr<ColumnStore> cstore) {
        Digest::GenerateMd5(cstore->mutable_data(), cstore->uncompressed_size, md5_checksum);
        return(1);
    }

    void Set(std::shared_ptr<ColumnStore> cstore) {
        sorted = cstore->sorted;
        n = cstore->n;
        m = cstore->m;
        uncompressed_size = cstore->uncompressed_size;
        compressed_size = cstore->compressed_size;
    }

    int Serialize(std::ostream& stream);
    int Deserialize(std::ostream& stream);

public:
    uint64_t file_offset; // file offset on disk to seek to the start of this ColumnStore
    bool sorted; // is this ColumnStore sorted (relative itself)
    uint32_t n, m, uncompressed_size, compressed_size; // number of elements
    int64_t stats_surrogate_min, stats_surrogate_max; // cast to actual ptype, any possible remainder is 0
    uint8_t md5_checksum[16]; // checksum for buffer
};

struct ColumnSetMetaData {
public:
    ColumnSetMetaData(std::shared_ptr<ColumnSet> cset, const uint32_t batch_id) : record_batch_id(batch_id)
    {
        for(int i = 0; i < cset->size(); ++i) {
            column_meta_data.push_back(std::make_shared<ColumnStoreMetaData>());
            column_meta_data.back()->Set(cset->columns[i]);
            column_meta_data.back()->ComputeChecksum(cset->columns[i]);

            std::cerr << "MD5: ";
            for(int j = 0; j < 12; ++j)
                std::cerr << (int)column_meta_data.back()->md5_checksum[j] << " ";
            std::cerr << std::endl;
        }
    }

    template <class T>
    int ComputeSegmentStats(std::shared_ptr<ColumnSet> cset) {
        for(int i = 0; i < cset->size(); ++i) {
            int ret = column_meta_data[i]->ComputeSegmentStats<T>(cset->columns[i]);
            //int ret = std::static_pointer_cast< ColumnStoreBuilder<T> >(cset->columns[i])->ComputeSegmentStats();
            assert(ret != -1);
        }
        return(1);
    }

    int Serialize(std::ostream& stream);
    int Deserialize(std::ostream& stream);

public:
    uint32_t record_batch_id; // What RecordBatch does this ColumnSet belong to.
    std::vector< std::shared_ptr<ColumnStoreMetaData> > column_meta_data;
};

// Whenever a Field is added we must add one of these meta structures.
// Then every time a RecordBatch is flushed we add the ColumnSet data
// statistics.
struct FieldMetaData {
public:
    int AddBatch(std::shared_ptr<ColumnSet> cset, const uint32_t batch_id) {
        if(cset.get() == nullptr) return(-1);

        std::cerr << "adding field data=" << cset->columns.size() << std::endl;
        cset_meta.push_back(std::make_shared<ColumnSetMetaData>(cset, batch_id));

        return(1);
    }

    int Serialize(std::ostream& stream);
    int Deserialize(std::ostream& stream);

public:
    std::string file_name;
    std::vector< std::shared_ptr<ColumnSetMetaData> > cset_meta;
};

/**<
 * The RecordBatch is a contiguous, non-overlapping, block of records (tuples).
 * The relationship between a Schema and its target ColumnSets are guaranteed
 * by keeping track of the Schema identifier for each record. This layout of
 * vectorized Schema identifiers permits for fast predicate evaluation of the
 * schemas in a vectorized fashion. This also allows us to correctly materialize
 * tuples according to their Schema without returning NULLs for non-relevant
 * columns.
 *
 * Each RecordBatch additionally maps the GLOBAL field identifiers into LOCAL
 * field identifiers such that only the used Fields in the local Schemas map
 * to [0, n). For example: the GLOBAL FieldDictionary have 100 fields defined
 * but a LOCAL RecordBatch have only 3 field available: F1 -> 0, F2 -> 1,
 * F3 -> 3. Lets say F1, F2, and F3 have the GLOBAL identifiers 21, 26, 51 then
 * the local map is 21 -> 0, 26 -> 1, and 51 -> 2.
 */
struct RecordBatch {
public:
    RecordBatch() : file_offset(0), n_rec(0){}

    // Insert a GLOBAL Schema id into the record batch
    int AddSchema(uint32_t pid) {
        if(schemas.get() == nullptr) schemas = std::make_shared<ColumnSet>();
        int insert_status = std::static_pointer_cast< ColumnSetBuilder<uint32_t> >(schemas)->Append(pid);
        ++n_rec;
        return(insert_status);
    }

    int AddGlobalField(const std::vector<uint32_t> global_ids) {
        for(int i = 0; i < global_ids.size(); ++i){
            int32_t local = FindLocalField(global_ids[i]);
            if(local == -1) {
                std::cerr << "inserting field: " << global_ids[i] << " into local map" << std::endl;
                global_local_field_map[global_ids[i]] = local_dict.size();
                local = local_dict.size();
                local_dict.push_back(global_ids[i]);
            }
        }
        return(1);
    }

    int AddGlobalField(const uint32_t global_id) {
        int32_t local = FindLocalField(global_id);
        if(local == -1) {
            std::cerr << "inserting field: " << global_id << " into local map" << std::endl;
            global_local_field_map[global_id] = local_dict.size();
            local = local_dict.size();
            local_dict.push_back(global_id);
        }
        return(local);
    }

    int FindLocalField(const uint32_t global_id) const {
        std::unordered_map<uint32_t, uint32_t>::const_iterator ret = global_local_field_map.find(global_id);
        if(ret != global_local_field_map.end()){
            return(ret->second);
        } else return(-1);
    }

    int Serialize(std::ofstream& stream) {
        assert(schemas->columns[0].get() != nullptr);

        uint64_t curpos = stream.tellp();
        file_offset = curpos;
        std::cerr << "SERIALIZE offset=" << curpos << std::endl;

        stream.write(reinterpret_cast<char*>(&file_offset), sizeof(uint64_t));
        stream.write(reinterpret_cast<char*>(&n_rec), sizeof(uint32_t));
        uint32_t n_dict = local_dict.size();
        stream.write(reinterpret_cast<char*>(&n_dict), sizeof(uint32_t));
        for(int i = 0; i < n_dict; ++i)
            stream.write(reinterpret_cast<char*>(&local_dict[i]), sizeof(uint32_t));

        // Compress with Zstd
        Compressor c;
        int ret = static_cast<ZstdCompressor*>(&c)->Compress(schemas->columns[0]->mutable_data(), schemas->columns[0]->uncompressed_size, 1);
        std::cerr << "SERIALIZE zstd: " << schemas->columns[0]->uncompressed_size << "->" << ret << std::endl;
        assert(ret != -1);
        stream.write(reinterpret_cast<char*>(&schemas->columns[0]->uncompressed_size), sizeof(uint32_t)); // uncompressed size
        stream.write(reinterpret_cast<char*>(&ret), sizeof(int)); // compressed size
        stream.write(reinterpret_cast<char*>(c.data()->mutable_data()), ret); // compressed data

        // Explicit release of shared_ptr.
        schemas = nullptr;

        return(1);
    }

public:
    uint64_t file_offset; // Disk virtual offset to the Schemas offsets
    uint32_t n_rec; // Number of rows in this RecordBatch
    std::vector<uint32_t> local_dict; // local field ID vector
    std::unordered_map<uint32_t, uint32_t> global_local_field_map; // Map from global field id -> local field id
    // ColumnStore for Schemas. ALWAYS cast as uint32_t
    std::shared_ptr<ColumnSet> schemas; // Array storage of the local dictionary-encoded BatchPatterns.
};

struct FileMetaData {
public:
    FileMetaData() : n_rows(0){}

    inline void AddRowCounts(const uint32_t c) { n_rows += c; }

public:
    uint64_t n_rows;
    // Efficient map of a RecordBatch to the ColumnSets it contains:
    // we can do this by the querying the local Schemas.
    std::vector< std::shared_ptr<RecordBatch> > batches;
    // Todo: columnstore offsets and summary data
    // Want to:
    //    efficiently map a ColumnSet to a RecordBatch
    //    efficiently map a ColumnSet to its disk offset
    //    efficiently lookup the segmental range

    // Meta data for each Field
    std::vector< std::shared_ptr<FieldMetaData> > field_meta;
};

struct Table {
public:
    Table() : single_archive(false), c_in(0), c_out(0){}

	/**<
	 * Convert a tuple into ColumnStore representation. This function will accept
	 * a RecordBuilder reference with properly overloaded data and described slots
	 * and will attempt to insert it into the current RecordBatch.
	 * @param builder Reference to a RecordBuilder.
	 * @return
	 */
    int Append(RecordBuilder& builder);

    /**<
     * Add a new ColumnSet to the current RecordBatch and null-pad up to the
     * current record count.
     * @param builder     Reference RecordBuilder holding the data to be added.
     * @param slot_offset Target Slot in the RecordBuilder to add.
     * @param global_id   Global identifier offset.
     * @return
     */
    int BatchAddColumn(PIL_PRIMITIVE_TYPE ptype, PIL_PRIMITIVE_TYPE ptype_arr, uint32_t global_id);

    /**<
     * Append the data from the provided RecordBuilder into the Table. This
     * subroutine adds data from a source Slot to a target ColumnSet.
     * @param builder     Reference RecordBuilder holding the data to be added.
     * @param slot_offset Target Slot in the RecordBuilder to add.
     * @param dst_column  Target ColumnSet to add the target Slot data into the Table.
     * @return            Returns a non-negative number if succesful or -1 otherwise.
     */
    int AppendData(const RecordBuilder& builder, const uint32_t slot_offset, std::shared_ptr<ColumnSet> dst_column);

    /**<
     * Finalise the RecordBatch by encoding and compressing the ColumnSets.
     * @return
     */
    int FinalizeBatch();

    /**<
     * Helper function that writes out the ColumnSet information and their
     * header descriptions.
     * @param stream Destination ostream.
     */
    void Describe(std::ostream& stream);

    /**<
     * Pre-hint the existence of a field with the given parameters and set the
     * preferred compression/encoding pattern.
     * Example usage:
     *
     * std::vector<PIL_COMPRESSION_TYPE> ctypes;
     * ctypes.push_back(PIL_ENCODE_BASES_2BIT);
     * ctypes.push_back(PIL_COMPRESS_RC_QUAL);
     * table.SetField("QUAL", PIL_TYPE_UINT8, ctypes); // Column-store of type UINT8 with 2-bit encoding followed by RangeCoding using CRAM Quality algorithm.
     *
     * @param field_name
     * @param ptype
     * @param ctype
     * @return
     */
    int SetField(const std::string& field_name, PIL_PRIMITIVE_TYPE ptype, const std::vector<PIL_COMPRESSION_TYPE>& ctype) {
        if(field_dict.Find(field_name) == - 1) {
            meta_data.field_meta.push_back(std::make_shared<FieldMetaData>());
            int ret = field_dict.FindOrAdd(field_name, ptype, PIL_TYPE_UNKNOWN);
            std::cerr << "returned=" << ret << std::endl;
            int _segid = BatchAddColumn(ptype, PIL_TYPE_UNKNOWN, ret);
            std::cerr << "_segid=" << _segid << std::endl;
            field_dict.dict[ret].transforms = ctype;
        } else {
            std::cerr << "already exists" << std::endl;

        }
        return(1);
    }

    /**<
    * Pre-hint the existence of a field with the given parameters and set the
    * preferred compression/encoding pattern.
    * Example usage:
    *
    * std::vector<PIL_COMPRESSION_TYPE> ctypes;
    * ctypes.push_back(PIL_ENCODE_BASES_2BIT);
    * ctypes.push_back(PIL_COMPRESS_RC_QUAL);
    * table.SetField("QUAL", PIL_TYPE_BYTE_ARRAY, PIL_TYPE_UINT8, ctypes); // Tensor-store of type UINT8 with 2-bit encoding followed by RangeCoding using CRAM Quality algorithm.
    *
    * @param field_name
    * @param ptype
    * @param ctype
    * @return
    */
    int SetField(const std::string& field_name,
                 PIL_PRIMITIVE_TYPE ptype,
                 PIL_PRIMITIVE_TYPE ptype_array,
                 const std::vector<PIL_COMPRESSION_TYPE>& ctype)
    {
        if(field_dict.Find(field_name) == - 1) {
            meta_data.field_meta.push_back(std::make_shared<FieldMetaData>());
            int ret = field_dict.FindOrAdd(field_name, ptype, ptype_array);
            std::cerr << "returned=" << ret << std::endl;
            int _segid = BatchAddColumn(ptype, ptype_array, ret);
            std::cerr << "_segid=" << _segid << std::endl;
            field_dict.dict[ret].transforms = ctype;
        } else {
            std::cerr << "already exists" << std::endl;

        }
        return(1);
    }

public:
    int32_t format_version; // 4 bytes
    FieldDictionary field_dict;
    SchemaDictionary schema_dict;
    FileMetaData meta_data;

public: // private:
    bool single_archive; // Write a single archive or mutiple output files in a directory.
    // Construction helpers
    uint64_t c_in, c_out;
    //std::shared_ptr<RecordBatch> record_batch; // temporary instance of a RecordBatch
    std::vector< std::shared_ptr<ColumnSet> > _seg_stack; // temporary Segments used during construction.
    std::ofstream out_stream;
};


}



#endif /* TABLE_H_ */
