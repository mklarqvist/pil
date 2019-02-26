#ifndef TABLE_H_
#define TABLE_H_

#include <random>
#include <algorithm>
#include <unordered_map>
#include <fstream>

#include "table_meta.h"
#include "table_dict.h"
#include "memory_pool.h"
#include "columnstore.h"
#include "record_builder.h"
#include "encoder.h"

namespace pil {

struct Table {
public:
    Table() : format_version(0){}
    //~Table(){ }

public:
    int32_t format_version; // PIL\0 + main record | alt record
    FieldDictionary field_dict;
    SchemaDictionary schema_dict;
    FileMetaData meta_data;
};

// Use during construciton ONLY! This separates out construction and reading
class TableConstructor : public Table {
public:
    TableConstructor() : single_archive(true), batch_size(8192), c_in(0), c_out(0){}
    ~TableConstructor(){}

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
        // Check for validity of transformation order.
        if(Transformer::ValidTransformationOrder(ctype) == false) {
            return(-2);
        }

        if(field_dict.Find(field_name) == - 1) {
            meta_data.field_meta.push_back(std::make_shared<FieldMetaData>());
            int ret = field_dict.FindOrAdd(field_name, ptype, PIL_TYPE_UNKNOWN);
            int _segid = BatchAddColumn(ptype, PIL_TYPE_UNKNOWN, ret);
            field_dict.dict[ret].transforms = ctype;
        } else {
            //std::cerr << "already exists" << std::endl;
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
        // Check for validity of transformation order.
        if(Transformer::ValidTransformationOrder(ctype) == false) {
            return(-2);
        }

        if(field_dict.Find(field_name) == - 1) {
            meta_data.field_meta.push_back(std::make_shared<FieldMetaData>());
            int ret = field_dict.FindOrAdd(field_name, ptype, ptype_array);
            field_dict.dict[ret].transforms = ctype;
        } else {
            //std::cerr << "already exists" << std::endl;
        }

        return(1);
    }

    /**<
     * Finalize import of data.
     * @return
     */
    int Finalize();

public:
    bool single_archive; // Write a single archive or mutiple output files in a directory.
    uint32_t batch_size;
    // Construction helpers
    uint64_t c_in, c_out; // Todo: delete
    //std::shared_ptr<RecordBatch> record_batch; // temporary instance of a RecordBatch
    std::vector< std::shared_ptr<ColumnSet> > build_csets; // temporary ColumnSets used during construction.
    std::ofstream out_stream;
};


}



#endif /* TABLE_H_ */
