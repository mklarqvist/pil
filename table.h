#ifndef TABLE_H_
#define TABLE_H_

#include <random>

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
#include <ctime>

namespace pil {

// The ColumnMetaData is stored separate from the ColumnSet and ColumnStores
// themselves and is used in an Indexing capacity. For example, to perform
// Segmental Elimination for predicate pushdown in selection queries.
struct ColumnStoreMetaData {
public:
    /**<
     * Compute Segmental statistics for use in predicate pushdown. Currently,
     * we store minimum and maximum values for a ColumnStore. These statistics
     * are used in Segmental Elimination during queries.
     * @param cstore Source ColumnStore
     * @return
     */
    template <class T>
    int ComputeSegmentStats(std::shared_ptr<ColumnStore> cstore) {
        if(cstore.get() == nullptr) return(-1);

        const T* values = reinterpret_cast<const T*>(cstore->buffer->mutable_data());
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
        // use memcpy to properly handle punning
        std::memcpy(&stats_surrogate_min, &min, sizeof(T));
        std::memcpy(&stats_surrogate_max, &max, sizeof(T));
        //*reinterpret_cast<T*>(&stats_surrogate_min) = min;
        //*reinterpret_cast<T*>(&stats_surrogate_max) = max;

        std::cerr << "Segmental statistics (as ints): min-max: " << (int)min << "-" << (int)max << std::endl;
        return(1);
    }

    /**<
     * Compute the MD5 checksum of the given ColumnStore.
     * @param cstore Source ColumnStore.
     * @return
     */
    int ComputeChecksum(std::shared_ptr<ColumnStore> cstore) {
        if(cstore.get() == nullptr) return(-1);
        Digest::GenerateMd5(cstore->mutable_data(), cstore->uncompressed_size, md5_checksum);
        return(1);
    }

    /**<
     * Store the meta-data from a ColumnStore in this ColumnStoreMetaData object.
     * @param cstore Source ColumnStore.
     */
    void Set(std::shared_ptr<ColumnStore> cstore) {
        if(cstore.get() == nullptr) return;

        n = cstore->n;
        m = cstore->m;
        uncompressed_size = cstore->uncompressed_size;
        compressed_size   = cstore->compressed_size;
    }

    // Synonym
    inline void operator=(std::shared_ptr<ColumnStore> cstore) { this->Set(cstore); }

    int Serialize(std::ostream& stream) {
        stream.write(reinterpret_cast<char*>(&file_offset), sizeof(uint64_t));
        stream.write(reinterpret_cast<char*>(&last_modified), sizeof(uint64_t));
        stream.write(reinterpret_cast<char*>(&n), sizeof(uint32_t));
        stream.write(reinterpret_cast<char*>(&m), sizeof(uint32_t));
        stream.write(reinterpret_cast<char*>(&uncompressed_size), sizeof(uint32_t));
        stream.write(reinterpret_cast<char*>(&compressed_size), sizeof(uint32_t));
        stream.write(reinterpret_cast<char*>(&stats_surrogate_min), sizeof(uint64_t));
        stream.write(reinterpret_cast<char*>(&stats_surrogate_max), sizeof(uint64_t));
        stream.write(reinterpret_cast<char*>(md5_checksum), 16);
        return(stream.good());
    }

    int Deserialize(std::ostream& stream);

public:
    uint64_t file_offset; // file offset on disk to seek to the start of this ColumnStore
    uint64_t last_modified; // unix timestamp when last modified
    uint32_t n, m, uncompressed_size, compressed_size; // number of elements
    uint64_t stats_surrogate_min, stats_surrogate_max; // cast to actual ptype, any possible remainder is 0
    uint8_t md5_checksum[16]; // checksum for buffer
};

// MetaData for a ColumnSet
struct ColumnSetMetaData {
public:
    ColumnSetMetaData() : record_batch_id(0){}

    ColumnSetMetaData(std::shared_ptr<ColumnSet> cset, const uint32_t batch_id) :
        record_batch_id(batch_id)
    {
        for(int i = 0; i < cset->size(); ++i) {
            column_meta_data.push_back(std::make_shared<ColumnStoreMetaData>());
            column_meta_data.back()->Set(cset->columns[i]);
            column_meta_data.back()->ComputeChecksum(cset->columns[i]);

            //std::cerr << "MD5: ";
            //for(int j = 0; j < 12; ++j)
            //    std::cerr << (int)column_meta_data.back()->md5_checksum[j] << " ";
            //std::cerr << std::endl;
        }
    }

    /**<
     * Compute Segmental summary statistics for every ColumnStore in the provided
     * ColumnSet.
     * @param cset Source ColumnSet.
     * @return
     */
    template <class T>
    int ComputeSegmentStats(std::shared_ptr<ColumnSet> cset) {
        if(cset.get() == nullptr) return(-1);

        for(size_t i = 0; i < cset->size(); ++i) {
            int ret = column_meta_data[i]->ComputeSegmentStats<T>(cset->columns[i]);
            assert(ret != -1);
        }
        return(1);
    }

    /**<
     * Overload the meta-data for every ColumnStore in the provided ColumnSet
     * in the index.
     * @param cset Source ColumnSet.
     * @return
     */
    int AddColumnStore(std::shared_ptr<ColumnStore> cstore) {
        if(cstore.get() == nullptr) return(-1);

        column_meta_data.push_back(std::make_shared<ColumnStoreMetaData>());
        column_meta_data.back()->Set(cstore);

        return 1;
    }

    int AddColumnSet(std::shared_ptr<ColumnSet> cset) {
        if(cset.get() == nullptr) return(-1);

        for(size_t i = 0; i < cset->size(); ++i) {
            AddColumnStore(cset->columns[i]);
        }

        return 1;
    }

    int UpdateColumnStore(std::shared_ptr<ColumnStore> cstore, const uint32_t offset) {
        if(cstore.get() == nullptr) return(-1);

        column_meta_data[offset]->Set(cstore);
        column_meta_data[offset]->ComputeChecksum(cstore);

        return 1;
    }

    int UpdateColumnSet(std::shared_ptr<ColumnSet> cstore) {
        if(cstore.get() == nullptr) return(-1);

        for(size_t i = 0; i < cstore->size(); ++i) {
            if(i == column_meta_data.size()) {
                AddColumnStore(cstore->columns[i]);
            }

            UpdateColumnStore(cstore->columns[i], i);
        }

        return 1;
    }

    /**<
     * Serialize to disk. Will correctly output this ColumnSetMetaData in the
     * appropriate format.
     * @param cset   Source ColumnSet.
     * @param stream Destination output stream.
     * @return
     */
    int SerializeColumnSet(std::shared_ptr<ColumnSet> cset, std::ostream& stream) {
        if(cset.get() == nullptr) return(-1);

        // Iterate over ColumnStores in the ColumnSet and serialize out to disk
        // and keep track of the virtual offset (disk offset) and the time this
        // object was last modified (now).
        for(size_t i = 0; i < cset->size(); ++i) {
            column_meta_data[i]->file_offset = stream.tellp();
            // Todo: this is incorrect. Should not be used like this.
            int ret = cset->columns[i]->Serialize(stream);
            column_meta_data[i]->last_modified = static_cast<uint64_t>(std::time(0));
        }
        stream.flush();
        return(stream.good());
    }

    int Serialize(std::ostream& stream) {
        stream.write(reinterpret_cast<char*>(&record_batch_id), sizeof(uint32_t));
        uint32_t n_cmeta = column_meta_data.size();
        stream.write(reinterpret_cast<char*>(&n_cmeta), sizeof(uint32_t));
        for(size_t i = 0; i < n_cmeta; ++i) {
            column_meta_data[i]->Serialize(stream);
        }
        return(stream.good());
    }

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
    FieldMetaData() : open_writer(false), open_reader(false){}

    int AddBatch(std::shared_ptr<ColumnSet> cset) {
        if(cset.get() == nullptr) return(-1);

        //std::cerr << "adding field data=" << cset->columns.size() << std::endl;
        int ret_target = cset_meta.size();
        //cset_meta.push_back(std::make_shared<ColumnSetMetaData>(cset, batch_id));
        cset_meta.push_back(std::make_shared<ColumnSetMetaData>());

        return(ret_target);
    }

    double AverageColumns() const {
        if(cset_meta.size() == 0) return 0;
        double tot = 0;

        for(size_t i = 0; i < cset_meta.size(); ++i) {
            tot += cset_meta[i]->column_meta_data.size();
        }

        return(tot / cset_meta.size());
    }

    double AverageCompressionFold() const {
        if(cset_meta.size() == 0) return 0;
        double uc = 0, c = 0;

        for(size_t i = 0; i < cset_meta.size(); ++i) {
            for(size_t j = 0; j < cset_meta[i]->column_meta_data.size(); ++j) {
                uc += cset_meta[i]->column_meta_data[j]->uncompressed_size;
                c += cset_meta[i]->column_meta_data[j]->compressed_size;
            }
        }

        //std::cerr << "uc = " << uc << " and c = " << c << std::endl;
        if(c == 0) return 0;
        return(uc / c);
    }

    uint64_t TotalOccurences() const {
        if(cset_meta.size() == 0) return 0;
        uint64_t n = 0;

        for(size_t i = 0; i < cset_meta.size(); ++i) {
            for(size_t j = 0; j < cset_meta[i]->column_meta_data.size(); ++j) {
                n += cset_meta[i]->column_meta_data[j]->n;
            }
        }

        return(n);
    }

    inline size_t TotalCount() const { return(cset_meta.size()); }

    uint64_t TotalUncompressed() const {
        if(cset_meta.size() == 0) return 0;
        uint64_t n = 0;

        for(size_t i = 0; i < cset_meta.size(); ++i) {
            for(size_t j = 0; j < cset_meta[i]->column_meta_data.size(); ++j) {
                n += cset_meta[i]->column_meta_data[j]->uncompressed_size;
            }
        }

        return(n);
    }

    uint64_t TotalCompressed() const {
        if(cset_meta.size() == 0) return 0;
        uint64_t n = 0;

        for(size_t i = 0; i < cset_meta.size(); ++i) {
            for(size_t j = 0; j < cset_meta[i]->column_meta_data.size(); ++j) {
                n += cset_meta[i]->column_meta_data[j]->compressed_size;
            }
        }

        return(n);
    }

    std::string RandomString(std::string::size_type length) {
        static auto& chrs = "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        thread_local static std::mt19937 rg{std::random_device{}()};
        thread_local static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);

        std::string s;

        s.reserve(length);

        while(length--)
            s += chrs[pick(rg)];

        return s;
    }

    int OpenWriter(const std::string& prefix) {
        if(writer.get() != nullptr) return(0);

        std::string random = RandomString(46);
        std::string out = prefix + "_" + random + ".pil";
        file_name = out;
        writer = std::unique_ptr<std::ofstream>(new std::ofstream());
        std::cerr << "opening: " << file_name << std::endl;
        writer->open(file_name, std::ios::binary | std::ios::out);

        if(writer->good() == false) {
            return(-1);
        }

        open_writer = true;

        return(1);
    }

    // Todo: pass cstore to serialize to dist writer
    // Assumes we are writing blocks *IN ORDER* as we use the latest MetaData
    // unit to store the information in.
    int SerializeColumnSet(std::shared_ptr<ColumnSet> cset) {
        if(cset.get() == nullptr) return(-1);
        if(open_writer == false) return(-1);

        bool good = cset_meta.back()->SerializeColumnSet(cset, *writer.get());
        return(good);
    }

    int SerializeColumnSet(std::shared_ptr<ColumnSet> cset, std::ostream& stream) {
        if(cset.get() == nullptr) return(-1);

        // This subroutine will append the Serialized data from the ColumnSet into
        // the destination ColumnSetMetaData.
        // Todo: this is quiet ugly and should be rewritten in the future.
        // Todo: ugly appending the data to the back rather than at a given offset.
        bool good = cset_meta.back()->SerializeColumnSet(cset, stream);
        return(good);
    }

    int Serialize(std::ostream& stream) {
        uint32_t n_file_name = file_name.size();
        stream.write(reinterpret_cast<char*>(&n_file_name), sizeof(uint32_t));
        uint32_t n_cset = cset_meta.size();
        stream.write(reinterpret_cast<char*>(&n_cset), sizeof(uint32_t));
        for(size_t i = 0; i < n_cset; ++i) {
            cset_meta[i]->Serialize(stream);
        }
        return(stream.good());
    }

    int Deserialize(std::ostream& stream);

public:
    bool open_writer, open_reader;
    std::string file_name;
    std::unique_ptr<std::ofstream> writer;
    std::unique_ptr<std::ifstream> reader;
    std::vector< std::shared_ptr<ColumnSetMetaData> > cset_meta;
};

/**<
 * The RecordBatch is a contiguous, non-overlapping, block of records (tuples).
 * The relationship between a Schema and its target ColumnSets are guaranteed
 * by keeping track of the Schema identifier for each record. This layout of
 * vectorized Schema identifiers permits for fast predicate evaluation of the
 * schemas using vectorized instructions. This also allows us to correctly mature
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
    RecordBatch() : n_rec(0){}

    // Insert a GLOBAL Schema id into the record batch
    int AddSchema(uint32_t pid) {
        if(schemas.get() == nullptr) schemas = std::make_shared<ColumnSet>();
        int insert_status = std::static_pointer_cast< ColumnSetBuilder<uint32_t> >(schemas)->Append(pid);
        ++n_rec;
        return(insert_status);
    }

    int AddGlobalField(const std::vector<uint32_t> global_ids) {
        for(size_t i = 0; i < global_ids.size(); ++i){
            int32_t local = FindLocalField(global_ids[i]);
            if(local == -1) {
                //std::cerr << "inserting field: " << global_ids[i] << " into local map" << std::endl;
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
            //std::cerr << "inserting field: " << global_id << " into local map @ " << local_dict.size() << std::endl;
            global_local_field_map[global_id] = local_dict.size();
            local = local_dict.size();
            local_dict.push_back(global_id);
        }
        return(local);
    }

    int FindLocalField(const uint32_t global_id) const {
        std::unordered_map<uint32_t, uint32_t>::const_iterator ret = global_local_field_map.find(global_id);
        if(ret != global_local_field_map.end()) {
            return(ret->second);
        } else return(-1);
    }

    int Serialize(std::ostream& stream) {
        //uint64_t curpos = stream.tellp();
        //file_offset = curpos;
        //std::cerr << "SERIALIZE offset=" << curpos << std::endl;

        //stream.write(reinterpret_cast<char*>(&file_offset), sizeof(uint64_t));
        stream.write(reinterpret_cast<char*>(&n_rec), sizeof(uint32_t));
        uint32_t n_dict = local_dict.size();
        stream.write(reinterpret_cast<char*>(&n_dict), sizeof(uint32_t));
        for(size_t i = 0; i < n_dict; ++i)
            stream.write(reinterpret_cast<char*>(&local_dict[i]), sizeof(uint32_t));

        // Todo: move out
        if(0) {
            assert(schemas->columns[0].get() != nullptr);
            // Compress with Zstd
            Compressor c;
            int ret = static_cast<ZstdCompressor*>(&c)->Compress(schemas->columns[0]->mutable_data(),
                                                                 schemas->columns[0]->uncompressed_size,
                                                                 PIL_ZSTD_DEFAULT_LEVEL);

            //std::cerr << "SERIALIZE zstd: " << schemas->columns[0]->uncompressed_size << "->" << ret << std::endl;
            assert(ret != -1);
            stream.write(reinterpret_cast<char*>(&schemas->columns[0]->uncompressed_size), sizeof(uint32_t)); // uncompressed size
            stream.write(reinterpret_cast<char*>(&ret), sizeof(int)); // compressed size
            stream.write(reinterpret_cast<char*>(c.data()->mutable_data()), ret); // compressed data

        }

        // Explicit release of shared_ptr.
        schemas = nullptr;

        return(1);
    }

public:
    //uint64_t file_offset; // Disk virtual offset to the Schemas offsets
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

    /**<
     * Add a ColumnSet to the MetaData indices and compute Segmental statistics for
     * predicate pushdown.
     * @param cset       Source ColumnSet to add.
     * @param global_id  Global Field identifier.
     * @param field_dict FieldDictionary reference.
     * @return           Returns the destination Batch identifier.
     */
    int AddColumnSet(std::shared_ptr<ColumnSet> cset,
                     const uint32_t global_id,
                     FieldDictionary& field_dict)
    {
        // Update the FileMetaData with information about ColumnSets
        //
        // FieldMetaData: the target FieldMetaData can be found by mapping the local
        // dictionary of global values.
        std::shared_ptr<FieldMetaData> tgt_field_meta = field_meta[global_id];
        // Prepare to add a ColumnSet for this RecordBatch to the FieldMetaData
        // record. The function AddBatch will return the target offset that will
        // be used.
        uint32_t batch_id = tgt_field_meta->AddBatch(cset);
        // Set the ColumnSetMetaData to the current RecordBatch offset.
        tgt_field_meta->cset_meta[batch_id]->record_batch_id = batches.size() - 1;

        int ret_status = -1;
        std::shared_ptr<ColumnSetMetaData> tgt_cset_meta = tgt_field_meta->cset_meta.back();
        // Add ColumnSet to the ColumnSetMetaData target
        tgt_cset_meta->AddColumnSet(cset);

        // Switch the Primitive type for the target ColumnSet.
        // Compute Segmental summary statistics including min and max.
        switch(field_dict.dict[global_id].ptype) {
        case(PIL_TYPE_INT8):   ret_status = tgt_cset_meta->ComputeSegmentStats<int8_t>(cset);   break;
        case(PIL_TYPE_INT16):  ret_status = tgt_cset_meta->ComputeSegmentStats<int16_t>(cset);  break;
        case(PIL_TYPE_INT32):  ret_status = tgt_cset_meta->ComputeSegmentStats<int32_t>(cset);  break;
        case(PIL_TYPE_INT64):  ret_status = tgt_cset_meta->ComputeSegmentStats<int64_t>(cset);  break;
        case(PIL_TYPE_UINT8):  ret_status = tgt_cset_meta->ComputeSegmentStats<uint8_t>(cset);  break;
        case(PIL_TYPE_UINT16): ret_status = tgt_cset_meta->ComputeSegmentStats<uint16_t>(cset); break;
        case(PIL_TYPE_UINT32): ret_status = tgt_cset_meta->ComputeSegmentStats<uint32_t>(cset); break;
        case(PIL_TYPE_UINT64): ret_status = tgt_cset_meta->ComputeSegmentStats<uint64_t>(cset); break;
        case(PIL_TYPE_FLOAT):  ret_status = tgt_cset_meta->ComputeSegmentStats<float>(cset);    break;
        case(PIL_TYPE_DOUBLE): ret_status = tgt_cset_meta->ComputeSegmentStats<double>(cset);   break;
        default: std::cerr << "no known type: " << field_dict.dict[global_id].ptype << std::endl; ret_status = -1; break;
        }
        assert(ret_status != -1);

        return(batch_id);
    }

    int UpdateColumnSet(std::shared_ptr<ColumnSet> cset,
                     const uint32_t global_id,
                     const uint32_t meta_id)
    {
        return(field_meta[global_id]->cset_meta[meta_id]->UpdateColumnSet(cset));
    }

    int Serialize(std::ostream& ostream) {
        ostream.write(reinterpret_cast<char*>(&n_rows), sizeof(uint64_t));
        uint32_t n_batches = batches.size();
        ostream.write(reinterpret_cast<char*>(&n_batches), sizeof(uint32_t));
        for(uint32_t i = 0; i < n_batches; ++i) {
            batches[i]->Serialize(ostream);
        }

        uint32_t n_fields = field_meta.size();
        ostream.write(reinterpret_cast<char*>(&n_fields), sizeof(uint32_t));
        for(uint32_t i = 0; i < n_fields; ++i) {
            field_meta[i]->Serialize(ostream);
        }

        return(ostream.good());
    }

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


    // First Core FieldMetaData is ALWAYS the Schema encodings in uint32_t format.
    std::vector< std::shared_ptr<FieldMetaData> > core_meta;
    // Meta data for each Field
    std::vector< std::shared_ptr<FieldMetaData> > field_meta;
};

struct Table {
public:
    Table() : format_version(0), single_archive(false), c_in(0), c_out(0){}
    ~Table(){ }

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
     * @param batch_id
     * @return
     */
    int FinalizeBatch(const uint32_t batch_id);


    int AddRecordBatchSchemas(std::shared_ptr<ColumnSet> cset, const uint32_t batch_id);
    // private

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
            //int _segid = BatchAddColumn(ptype, ptype_array, ret);
            //std::cerr << "_segid=" << _segid << std::endl;
            field_dict.dict[ret].transforms = ctype;
        } else {
            std::cerr << "already exists" << std::endl;

        }
        return(1);
    }

    int Finalize();

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
