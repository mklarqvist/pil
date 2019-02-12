#ifndef TABLE_H_
#define TABLE_H_

#include <algorithm>
#include <unordered_map>

#include "memory_pool.h"
#include "columnstore.h"
#include "record_builder.h"
#include "third_party/xxhash/xxhash.h"
#include "compression/variant_digest_manager.h"

#include "compression/zstd_codec.h"
#include "compression/fastdelta.h"

namespace pil {

//
// A SchemaPattern is the collection of ColumnSet identifiers a Record maps to.
// The `ids` vector of identifiers are indirectly encoded as an integer (indirect
// encoding).
struct SchemaPattern {
	/**<
	 * Compute a 64-bit hash for the vector of field identifiers with XXHASH.
	 * @return Returns a 64-bit hash.
	 */
    uint64_t Hash() const {
        XXH64_state_t* const state = XXH64_createState();
        if (state == NULL) abort();

        XXH_errorcode const resetResult = XXH64_reset(state, 71236251);
        if (resetResult == XXH_ERROR) abort();

        for(uint32_t i = 0; i < ids.size(); ++i){
            XXH_errorcode const addResult = XXH64_update(state, (const void*)&ids[i], sizeof(int));
            if (addResult == XXH_ERROR) abort();
        }

        uint64_t hash = XXH64_digest(state);
        XXH64_freeState(state);

        return hash;
    }

    std::vector<uint32_t> ids;
};

// DictionaryFieldType holds the target field name associated with
// a ColumnSet and the appropriate GLOBAL PIL primitive type.
// This means that all returned values should be compatibile with
// this assigned primitive type.
struct DictionaryFieldType {
	DictionaryFieldType() : cstore(PIL_CSTORE_UNKNOWN), ptype(PIL_TYPE_UNKNOWN){}

    std::string field_name;
    PIL_CSTORE_TYPE cstore;
    PIL_PRIMITIVE_TYPE ptype;
};

/**<
 * Dictionary encoding for Schema Fields. This structure allows us to map
 * from a field name string to a number representing the global identifier.
 */
struct FieldDictionary {
    uint32_t FindOrAdd(const std::string& field_name, PIL_PRIMITIVE_TYPE ptype, PIL_PRIMITIVE_TYPE ptype_arr) {
        int32_t column_id = -1;
        std::unordered_map<std::string, uint32_t>::const_iterator s = map.find(field_name);
        if(s != map.end()){
            //std::cerr << "found: " << s->first << "@" << s->second << std::endl;
            if(ptype == PIL_TYPE_BYTE_ARRAY){
                if(dict[s->second].ptype != ptype_arr) {
                    std::cerr << "ptype mismatch: illegal! -> " << (int)ptype_arr << "!=" << (int)dict[s->second].ptype << std::endl;
                    exit(1);
                }
            } else {
                if(dict[s->second].ptype != ptype) {
                    std::cerr << "ptype mismatch: illegal! -> " << (int)ptype << "!=" << (int)dict[s->second].ptype << std::endl;
                    exit(1);
                }
            }

            if(dict[s->second].cstore == PIL_CSTORE_TENSOR && ptype != PIL_TYPE_BYTE_ARRAY) {
                std::cerr << "cstore mismatch: illegal! -> have tensor store but not given byte array: " << (int)ptype << std::endl;
                exit(1);
            }

            column_id = s->second;
        } else {
            std::cerr << "did not find: " << field_name << std::endl;
            column_id = dict.size();
            map[field_name] = column_id;
            dict.push_back(DictionaryFieldType());
            dict.back().field_name = field_name;
            if(ptype != PIL_TYPE_BYTE_ARRAY){ dict.back().ptype = ptype; dict.back().cstore = PIL_CSTORE_COLUMN; }
            else { dict.back().ptype = ptype_arr; dict.back().cstore = PIL_CSTORE_TENSOR; }
        }
        //std::cerr << "id=" << column_id << std::endl;
        return(column_id);
    }

    // Global dictionary of ColumnSet identifiers
    // Field name string -> global identifier (dictionary encoding)
    std::vector<DictionaryFieldType> dict; // Field typing.
    std::unordered_map<std::string, uint32_t> map; // Field dictonary
};

/**<
 * Dictionary encoding for Schemas. The Schemas comprises of permutations of
 * global Field identifiers (see FieldDictionary).
 */
struct SchemaDictionary {
    uint32_t FindOrAdd(const SchemaPattern& pattern) {
        const uint64_t phash = pattern.Hash();
        std::unordered_map<uint64_t, uint32_t>::const_iterator s = map.find(phash);
        int pid = 0;
        if(s != map.end()){
            //std::cerr << "found: " << s->first << "@" << s->second << std::endl;
            pid = s->second;
        } else {
            std::cerr << "did not find pattern hash: " << phash << std::endl;
            pid = dict.size();
            map[phash] = pid;
            dict.push_back(pattern);
        }

        return pid;
    }

    // Dictionary-encoding of dictionary-encoded of field identifiers as a _Pattern_
    std::vector< SchemaPattern > dict; // number of UNIQUE patterns (multi-sets). Note that different permutations of the same values are considered different patterns.
    std::unordered_map<uint64_t, uint32_t> map; // Reverse lookup of Hash of pattern -> pattern ID.
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
                global_local_field_map[global_ids[i]] = dict.size();
                local = dict.size();
                dict.push_back(global_ids[i]);
            }
        }
        return(1);
    }

    int AddGlobalField(const uint32_t global_id) {
        int32_t local = FindLocalField(global_id);
        if(local == -1) {
            std::cerr << "inserting field: " << global_id << " into local map" << std::endl;
            global_local_field_map[global_id] = dict.size();
            local = dict.size();
            dict.push_back(global_id);
        }
        return(local);
    }

public:
    int32_t FindLocalField(const uint32_t global_id) const {
        std::unordered_map<uint32_t, uint32_t>::const_iterator ret = global_local_field_map.find(global_id);
        if(ret != global_local_field_map.end()){
            return(ret->second);
        } else return(-1);
    }

public:
    uint32_t n_rec; // Guarantee parity with schemas.columns[0]->n
    std::vector<uint32_t> dict; // local field ID vector
    std::unordered_map<uint32_t, uint32_t> global_local_field_map; // Map from global field id -> local field id
    // ColumnStore for Schemas. ALWAYS cast as uint32_t
    ColumnSet schemas; // Array storage of the local dictionary-encoded BatchPatterns.
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
    int Append(RecordBuilder& builder) {
        if(record_batch.get() == nullptr)
            record_batch = std::make_shared<RecordBatch>();

        // Check if the current RecordBatch has reached its Batch limit.
        // If it has then finalize the Batch.
        if(record_batch->n_rec >= 8000) { FinalizeBatch(); }

        // Foreach field name string in the builder record we check if it
        // exists in the dictionary. If it exists we return that value, otherwise
        // we insert it into the dictionary and return the new value.
        SchemaPattern pattern;
        for(int i = 0; i < builder.slots.size(); ++i) {
            int32_t column_id = field_dict.FindOrAdd(builder.slots[i]->field_name, builder.slots[i]->primitive_type, builder.slots[i]->array_primitive_type);
            pattern.ids.push_back(column_id);
        }

        // Store Schema-id with each record.
        uint32_t pid = schema_dict.FindOrAdd(pattern);

        // Check the local stack of ColumnSets if the target identifier is present.
        // If the target identifier is NOT available then we insert a new ColumnSet
        // with that identifier in the current Batch and pad with NULL values up
        // to the current offset.
        for(int i = 0; i < pattern.ids.size(); ++i) {
            //std::cerr << "checking: " << pattern.ids[i] << "..." << std::endl;
            int32_t _segid = record_batch->FindLocalField(pattern.ids[i]);
            if(_segid == -1) {
                std::cerr << "target column does NOT Exist in local stack: insert -> " << pattern.ids[i] << std::endl;
                _segid = BatchAddColumn(builder, i, pattern.ids[i]);
                std::cerr << "_segid=" << _segid << std::endl;
                assert(_segid != -1);
            }

            // Actual addition of data.
            assert(AppendData(builder, i, _seg_stack[_segid].get()) == 1);
        }

        // Map GLOBAL to LOCAL Schema in the current RecordBatch.
        // Note: Adding a pattern automatically increments the record count in a RecordBatch.
        record_batch->AddSchema(pid);
        //record_batch->AddGlobalField(pattern.ids);

        // CRITICAL!
        // Every Field in the current RecordBatch that is NOT in the current
        // Schema MUST be padded with NULLs for the current Schema. This is to
        // maintain the matrix-relationship between tuples and the ColumnSets.
        std::vector<uint32_t> pad_tgts;
        std::set_difference(record_batch->dict.begin(), record_batch->dict.end(),
                              pattern.ids.begin(), pattern.ids.end(),
                              back_inserter(pad_tgts));

        for(int i = 0; i < pad_tgts.size(); ++i) {
            // pad with nulls
            int32_t tgt_id = record_batch->FindLocalField(pad_tgts[i]);

            std::cerr << "padding id: " << i << "->" << tgt_id << std::endl;

            PIL_PRIMITIVE_TYPE ptype = field_dict.dict[pad_tgts[i]].ptype;
            PIL_CSTORE_TYPE ctype = field_dict.dict[pad_tgts[i]].cstore;
            int ret_status = 0;
            if(ctype == PIL_CSTORE_COLUMN){
                switch(ptype) {
                case(PIL_TYPE_INT8):   ret_status = static_cast<ColumnSetBuilder<int8_t>*>(_seg_stack[tgt_id].get())->PadNull();   break;
                case(PIL_TYPE_INT16):  ret_status = static_cast<ColumnSetBuilder<int16_t>*>(_seg_stack[tgt_id].get())->PadNull();  break;
                case(PIL_TYPE_INT32):  ret_status = static_cast<ColumnSetBuilder<int32_t>*>(_seg_stack[tgt_id].get())->PadNull();  break;
                case(PIL_TYPE_INT64):  ret_status = static_cast<ColumnSetBuilder<int64_t>*>(_seg_stack[tgt_id].get())->PadNull();  break;
                case(PIL_TYPE_UINT8):  ret_status = static_cast<ColumnSetBuilder<uint8_t>*>(_seg_stack[tgt_id].get())->PadNull();  break;
                case(PIL_TYPE_UINT16): ret_status = static_cast<ColumnSetBuilder<uint16_t>*>(_seg_stack[tgt_id].get())->PadNull(); break;
                case(PIL_TYPE_UINT32): ret_status = static_cast<ColumnSetBuilder<uint32_t>*>(_seg_stack[tgt_id].get())->PadNull(); break;
                case(PIL_TYPE_UINT64): ret_status = static_cast<ColumnSetBuilder<uint64_t>*>(_seg_stack[tgt_id].get())->PadNull(); break;
                case(PIL_TYPE_FLOAT):  ret_status = static_cast<ColumnSetBuilder<float>*>(_seg_stack[tgt_id].get())->PadNull();    break;
                case(PIL_TYPE_DOUBLE): ret_status = static_cast<ColumnSetBuilder<double>*>(_seg_stack[tgt_id].get())->PadNull();   break;
                default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
                }
            } else {
                switch(ptype) {
                case(PIL_TYPE_INT8):   ret_status = static_cast<ColumnSetBuilderTensor<int8_t>*>(_seg_stack[tgt_id].get())->PadNull();   break;
                case(PIL_TYPE_INT16):  ret_status = static_cast<ColumnSetBuilderTensor<int16_t>*>(_seg_stack[tgt_id].get())->PadNull();  break;
                case(PIL_TYPE_INT32):  ret_status = static_cast<ColumnSetBuilderTensor<int32_t>*>(_seg_stack[tgt_id].get())->PadNull();  break;
                case(PIL_TYPE_INT64):  ret_status = static_cast<ColumnSetBuilderTensor<int64_t>*>(_seg_stack[tgt_id].get())->PadNull();  break;
                case(PIL_TYPE_UINT8):  ret_status = static_cast<ColumnSetBuilderTensor<uint8_t>*>(_seg_stack[tgt_id].get())->PadNull();  break;
                case(PIL_TYPE_UINT16): ret_status = static_cast<ColumnSetBuilderTensor<uint16_t>*>(_seg_stack[tgt_id].get())->PadNull(); break;
                case(PIL_TYPE_UINT32): ret_status = static_cast<ColumnSetBuilderTensor<uint32_t>*>(_seg_stack[tgt_id].get())->PadNull(); break;
                case(PIL_TYPE_UINT64): ret_status = static_cast<ColumnSetBuilderTensor<uint64_t>*>(_seg_stack[tgt_id].get())->PadNull(); break;
                case(PIL_TYPE_FLOAT):  ret_status = static_cast<ColumnSetBuilderTensor<float>*>(_seg_stack[tgt_id].get())->PadNull();    break;
                case(PIL_TYPE_DOUBLE): ret_status = static_cast<ColumnSetBuilderTensor<double>*>(_seg_stack[tgt_id].get())->PadNull();   break;
                default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
                }
            }
            assert(ret_status == 1);
        }

        ++builder.n_added;
        builder.slots.clear();
        return(1); // success
    }

    // private
    int BatchAddColumn(const RecordBuilder& builder, const uint32_t slot_offset, uint32_t global_id) {
        std::cerr << "target column does NOT Exist in local stack: insert -> " << global_id << std::endl;
        int col_id = record_batch->AddGlobalField(global_id);

        _seg_stack.push_back(std::unique_ptr<ColumnSet>(new ColumnSet()));

        PIL_PRIMITIVE_TYPE ptype = builder.slots[slot_offset]->primitive_type;
        PIL_PRIMITIVE_TYPE ptype_arr = builder.slots[slot_offset]->array_primitive_type;
        const uint32_t padding_to = record_batch->n_rec;
        std::cerr << "padding up to: " << padding_to << std::endl;

        int ret_status = 0;
        // Special case when there is no padding we have to force the correct
        // return value.
        if(padding_to == 0) ret_status = 1;

        if(ptype == PIL_TYPE_BYTE_ARRAY){
            for(int j = 0; j < padding_to; ++j) {
               switch(ptype_arr) {
               case(PIL_TYPE_INT8):   ret_status = static_cast<ColumnSetBuilderTensor<int8_t>*>(_seg_stack.back().get())->Append(0);   break;
               case(PIL_TYPE_INT16):  ret_status = static_cast<ColumnSetBuilderTensor<int16_t>*>(_seg_stack.back().get())->Append(0);  break;
               case(PIL_TYPE_INT32):  ret_status = static_cast<ColumnSetBuilderTensor<int32_t>*>(_seg_stack.back().get())->Append(0);  break;
               case(PIL_TYPE_INT64):  ret_status = static_cast<ColumnSetBuilderTensor<int64_t>*>(_seg_stack.back().get())->Append(0);  break;
               case(PIL_TYPE_UINT8):  ret_status = static_cast<ColumnSetBuilderTensor<uint8_t>*>(_seg_stack.back().get())->Append(0);  break;
               case(PIL_TYPE_UINT16): ret_status = static_cast<ColumnSetBuilderTensor<uint16_t>*>(_seg_stack.back().get())->Append(0); break;
               case(PIL_TYPE_UINT32): ret_status = static_cast<ColumnSetBuilderTensor<uint32_t>*>(_seg_stack.back().get())->Append(0); break;
               case(PIL_TYPE_UINT64): ret_status = static_cast<ColumnSetBuilderTensor<uint64_t>*>(_seg_stack.back().get())->Append(0); break;
               case(PIL_TYPE_FLOAT):  ret_status = static_cast<ColumnSetBuilderTensor<float>*>(_seg_stack.back().get())->Append(0);    break;
               case(PIL_TYPE_DOUBLE): ret_status = static_cast<ColumnSetBuilderTensor<double>*>(_seg_stack.back().get())->Append(0);   break;
               default: std::cerr << "no known type: " << ptype_arr << std::endl; ret_status = -1; break;
               }
               assert(ret_status == 1);
            }
        } else {
            for(int j = 0; j < padding_to; ++j) {
               switch(ptype) {
               case(PIL_TYPE_INT8):   ret_status = static_cast<ColumnSetBuilder<int8_t>*>(_seg_stack.back().get())->Append(0);   break;
               case(PIL_TYPE_INT16):  ret_status = static_cast<ColumnSetBuilder<int16_t>*>(_seg_stack.back().get())->Append(0);  break;
               case(PIL_TYPE_INT32):  ret_status = static_cast<ColumnSetBuilder<int32_t>*>(_seg_stack.back().get())->Append(0);  break;
               case(PIL_TYPE_INT64):  ret_status = static_cast<ColumnSetBuilder<int64_t>*>(_seg_stack.back().get())->Append(0);  break;
               case(PIL_TYPE_UINT8):  ret_status = static_cast<ColumnSetBuilder<uint8_t>*>(_seg_stack.back().get())->Append(0);  break;
               case(PIL_TYPE_UINT16): ret_status = static_cast<ColumnSetBuilder<uint16_t>*>(_seg_stack.back().get())->Append(0); break;
               case(PIL_TYPE_UINT32): ret_status = static_cast<ColumnSetBuilder<uint32_t>*>(_seg_stack.back().get())->Append(0); break;
               case(PIL_TYPE_UINT64): ret_status = static_cast<ColumnSetBuilder<uint64_t>*>(_seg_stack.back().get())->Append(0); break;
               case(PIL_TYPE_FLOAT):  ret_status = static_cast<ColumnSetBuilder<float>*>(_seg_stack.back().get())->Append(0);    break;
               case(PIL_TYPE_DOUBLE): ret_status = static_cast<ColumnSetBuilder<double>*>(_seg_stack.back().get())->Append(0);   break;
               default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
               }
               assert(ret_status == 1);
            }
        }

        //std::cerr << "returning: " << col_id << " with ret_status=" << ret_status << std::endl;
        if(ret_status == 1) return(col_id);
        else return(-1);
    }

    // private
    int AppendData(const RecordBuilder& builder, const uint32_t slot_offset, ColumnSet* dst_column) {
        PIL_PRIMITIVE_TYPE ptype = builder.slots[slot_offset]->primitive_type;
        PIL_PRIMITIVE_TYPE ptype_arr = builder.slots[slot_offset]->array_primitive_type;

        int ret_status = 0;

        if(ptype == PIL_TYPE_BYTE_ARRAY) {
            switch(ptype_arr) {
            case(PIL_TYPE_INT8):   ret_status = static_cast<ColumnSetBuilderTensor<int8_t>*>(dst_column)->Append(reinterpret_cast<int8_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_INT16):  ret_status = static_cast<ColumnSetBuilderTensor<int16_t>*>(dst_column)->Append(reinterpret_cast<int16_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_INT32):  ret_status = static_cast<ColumnSetBuilderTensor<int32_t>*>(dst_column)->Append(reinterpret_cast<int32_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_INT64):  ret_status = static_cast<ColumnSetBuilderTensor<int64_t>*>(dst_column)->Append(reinterpret_cast<int64_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_UINT8):  ret_status = static_cast<ColumnSetBuilderTensor<uint8_t>*>(dst_column)->Append(reinterpret_cast<uint8_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_UINT16): ret_status = static_cast<ColumnSetBuilderTensor<uint16_t>*>(dst_column)->Append(reinterpret_cast<uint16_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_UINT32): ret_status = static_cast<ColumnSetBuilderTensor<uint32_t>*>(dst_column)->Append(reinterpret_cast<uint32_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_UINT64): ret_status = static_cast<ColumnSetBuilderTensor<uint64_t>*>(dst_column)->Append(reinterpret_cast<uint64_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_FLOAT):  ret_status = static_cast<ColumnSetBuilderTensor<float>*>(dst_column)->Append(reinterpret_cast<float*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_DOUBLE): ret_status = static_cast<ColumnSetBuilderTensor<double>*>(dst_column)->Append(reinterpret_cast<double*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
            }
        } else {
            switch(ptype) {
            case(PIL_TYPE_INT8):   ret_status = static_cast<ColumnSetBuilder<int8_t>*>(dst_column)->Append(reinterpret_cast<int8_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_INT16):  ret_status = static_cast<ColumnSetBuilder<int16_t>*>(dst_column)->Append(reinterpret_cast<int16_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_INT32):  ret_status = static_cast<ColumnSetBuilder<int32_t>*>(dst_column)->Append(reinterpret_cast<int32_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_INT64):  ret_status = static_cast<ColumnSetBuilder<int64_t>*>(dst_column)->Append(reinterpret_cast<int64_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_UINT8):  ret_status = static_cast<ColumnSetBuilder<uint8_t>*>(dst_column)->Append(reinterpret_cast<uint8_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_UINT16): ret_status = static_cast<ColumnSetBuilder<uint16_t>*>(dst_column)->Append(reinterpret_cast<uint16_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_UINT32): ret_status = static_cast<ColumnSetBuilder<uint32_t>*>(dst_column)->Append(reinterpret_cast<uint32_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_UINT64): ret_status = static_cast<ColumnSetBuilder<uint64_t>*>(dst_column)->Append(reinterpret_cast<uint64_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_FLOAT):  ret_status = static_cast<ColumnSetBuilder<float>*>(dst_column)->Append(reinterpret_cast<float*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            case(PIL_TYPE_DOUBLE): ret_status = static_cast<ColumnSetBuilder<double>*>(dst_column)->Append(reinterpret_cast<double*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
            default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
            }
        }

        return(ret_status);
    }

    int FinalizeBatch() {
        std::cerr << "record limit reached: " << record_batch->schemas.columns[0]->size() << ":" << record_batch->schemas.GetMemoryUsage() << std::endl;

        // Ugly reset of all data.
        uint32_t m_out = 1000000;
        uint8_t* out = new uint8_t[m_out];
        ZSTDCodec zstd;
        uint32_t mem_in = 0, mem_out = 0;

        for(int i = 0; i < _seg_stack.size(); ++i){
            uint32_t col_u = 0, col_c = 0;
            // Todo: How relate local ID to global ID for checks?
            std::cerr << "dict size=" << record_batch->dict.size() << std::endl;
            std::cerr << "checking: " << record_batch->dict[i] << "/" << field_dict.dict.size() << std::endl;

            if(field_dict.dict[record_batch->dict[i]].cstore == PIL_CSTORE_TENSOR) {
                std::cerr << "computing deltas" << std::endl;
                compute_deltas_inplace(reinterpret_cast<uint32_t*>(_seg_stack[i]->columns[0]->buffer->mutable_data()),
                                       _seg_stack[i]->columns[0]->n,
                                       0);
            }


            for(int j = 0; j < _seg_stack[i]->size(); ++j) {
                if(_seg_stack[i]->columns[j]->uncompressed_size > m_out) {
                    delete[] out;
                    m_out = _seg_stack[i]->columns[j]->uncompressed_size + 1000000;
                    out = new uint8_t[m_out];
                }

                int c_ret = zstd.Compress(_seg_stack[i]->columns[j]->buffer->mutable_data(),
                                          _seg_stack[i]->columns[j]->uncompressed_size,
                                          out, m_out, 1);

                mem_in += _seg_stack[i]->columns[j]->uncompressed_size;
                mem_out += c_ret;
                std::cerr << "compressed: " << i << ":" << j << ": n_rec=" <<_seg_stack[i]->columns[j]->n << " -> " << _seg_stack[i]->columns[j]->uncompressed_size << "->" << c_ret << " (" << (float)_seg_stack[i]->columns[j]->uncompressed_size/c_ret << "-fold)" << std::endl;
                col_u += _seg_stack[i]->columns[j]->uncompressed_size;
                col_c += c_ret;

                _seg_stack[i]->columns[j]->n = 0;
                _seg_stack[i]->columns[j]->uncompressed_size = 0;
            }
            //std::cerr << "column: compressed: " << col_u << "->" << col_c << "(" << (float)col_u/col_c << "-fold)" << std::endl;
        }
        _seg_stack.clear();
        std::cerr << "total: compressed: " << mem_in << "->" << mem_out << "(" << (float)mem_in/mem_out << "-fold)" << std::endl;
        c_in += mem_in;
        c_out += mem_out;

        delete[] out;
        // Todo: finalize a record batch
        record_batch = std::make_shared<RecordBatch>();

        return(1);
    }

public:
    int32_t format_version; // 4 bytes
    FieldDictionary field_dict;
    SchemaDictionary schema_dict;

public: // private:
    // Construction helpers
    uint64_t c_in, c_out;
    std::shared_ptr<RecordBatch> record_batch;
    std::vector< std::unique_ptr<ColumnSet> > _seg_stack; // temporary Segments used during construction.
};


}



#endif /* TABLE_H_ */
