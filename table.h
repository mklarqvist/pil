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

namespace pil {

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
    RecordBatch() : n_rec(0), schemas(){}

    // Insert a GLOBAL Schema id into the record batch
    int AddSchema(uint32_t pid) {
        int insert_status = static_cast<ColumnSetBuilder<uint32_t>*>(&schemas)->Append(pid);
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

    int32_t FindLocalField(const uint32_t global_id) const {
        std::unordered_map<uint32_t, uint32_t>::const_iterator ret = global_local_field_map.find(global_id);
        if(ret != global_local_field_map.end()){
            return(ret->second);
        } else return(-1);
    }

public:
    uint32_t n_rec; // Number of rows in this RecordBatch
    std::vector<uint32_t> local_dict; // local field ID vector
    std::unordered_map<uint32_t, uint32_t> global_local_field_map; // Map from global field id -> local field id
    // ColumnStore for Schemas. ALWAYS cast as uint32_t
    ColumnSet schemas; // Array storage of the local dictionary-encoded BatchPatterns.
};

struct FileMetaData {
    FileMetaData() : n_rows(0){}

    uint64_t n_rows;
    // Efficient map of a RecordBatch to the ColumnSets it contains:
    // we can do this by the querying the local Schemas.
    std::vector< std::shared_ptr<RecordBatch> > batches;
    // Todo: columnstore offsets and summary data
    // Want to:
    //    efficiently map a ColumnSet to a RecordBatch
    //    efficiently map a ColumnSet to its disk offset
    //    efficiently lookup the segmental range
};

struct Table {
public:
    Table() : c_in(0), c_out(0){}

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
        if(field_dict.Find(field_name) == - 1){
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
        if(field_dict.Find(field_name) == - 1){
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
    // Construction helpers
    uint64_t c_in, c_out;
    std::shared_ptr<RecordBatch> record_batch; // temporary instance of a RecordBatch
    std::vector< std::shared_ptr<ColumnSet> > _seg_stack; // temporary Segments used during construction.
};


}



#endif /* TABLE_H_ */
