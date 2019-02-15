#include "table.h"

namespace pil {

int Table::Append(RecordBuilder& builder) {
    if(record_batch.get() == nullptr)
        record_batch = std::make_shared<RecordBatch>();

    // Check if the current RecordBatch has reached its Batch limit.
    // If it has then finalize the Batch.
    if(record_batch->n_rec >= 4096) { FinalizeBatch(); }

    // Foreach field name string in the builder record we check if it
    // exists in the dictionary. If it exists we return that value, otherwise
    // we insert it into the dictionary and return the new value.
    SchemaPattern pattern;
    std::unordered_map<uint32_t, uint32_t> pattern_map;

    for(int i = 0; i < builder.slots.size(); ++i) {
        int32_t column_id = field_dict.FindOrAdd(builder.slots[i]->field_name, builder.slots[i]->primitive_type, builder.slots[i]->array_primitive_type);
        pattern.ids.push_back(column_id);
        pattern_map[column_id] = 1;
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
            _segid = BatchAddColumn(builder.slots[i]->primitive_type, builder.slots[i]->array_primitive_type, pattern.ids[i]);
            std::cerr << "_segid=" << _segid << std::endl;
            assert(_segid != -1);
        }

        // Actual addition of data.
        assert(AppendData(builder, i, _seg_stack[_segid]) == 1);
    }

    // Map GLOBAL to LOCAL Schema in the current RecordBatch.
    // Note: Adding a pattern automatically increments the record count in a RecordBatch.
    record_batch->AddSchema(pid);

    // CRITICAL!
    // Every Field in the current RecordBatch that is NOT in the current
    // Schema MUST be padded with NULLs for the current Schema. This is to
    // maintain the matrix-relationship between tuples and the ColumnSets.

    std::vector<uint32_t> pad_tgts;

    for(int i = 0; i < record_batch->local_dict.size(); ++i) {
        std::unordered_map<uint32_t, uint32_t>::const_iterator it = pattern_map.find(record_batch->local_dict[i]);
        if(it == pattern_map.end()) pad_tgts.push_back(i); // if not in set append it to vector
    }

    for(int i = 0; i < pad_tgts.size(); ++i) {
        // pad with nulls
        int32_t tgt_id = record_batch->FindLocalField(pad_tgts[i]);
        assert(_seg_stack[tgt_id].get() != nullptr);

        std::cerr << "padding id: " << i << "->" << tgt_id << std::endl;
        exit(1);

        PIL_PRIMITIVE_TYPE ptype = field_dict.dict[pad_tgts[i]].ptype;
        PIL_CSTORE_TYPE ctype = field_dict.dict[pad_tgts[i]].cstore;

        int ret_status = 0;
        if(ctype == PIL_CSTORE_COLUMN){
            switch(ptype) {
            case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< ColumnSetBuilder<int8_t> >(_seg_stack[tgt_id])->PadNull();   break;
            case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int16_t> >(_seg_stack[tgt_id])->PadNull();  break;
            case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int32_t> >(_seg_stack[tgt_id])->PadNull();  break;
            case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int64_t> >(_seg_stack[tgt_id])->PadNull();  break;
            case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< ColumnSetBuilder<uint8_t> >(_seg_stack[tgt_id])->PadNull();  break;
            case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint16_t> >(_seg_stack[tgt_id])->PadNull(); break;
            case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint32_t> >(_seg_stack[tgt_id])->PadNull(); break;
            case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint64_t> >(_seg_stack[tgt_id])->PadNull(); break;
            case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< ColumnSetBuilder<float> >(_seg_stack[tgt_id])->PadNull();    break;
            case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< ColumnSetBuilder<double> >(_seg_stack[tgt_id])->PadNull();   break;
            default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
            }
        } else {
            switch(ptype) {
            case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int8_t> >(_seg_stack[tgt_id])->PadNull();   break;
            case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int16_t> >(_seg_stack[tgt_id])->PadNull();  break;
            case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int32_t> >(_seg_stack[tgt_id])->PadNull();  break;
            case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int64_t> >(_seg_stack[tgt_id])->PadNull();  break;
            case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint8_t> >(_seg_stack[tgt_id])->PadNull();  break;
            case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint16_t> >(_seg_stack[tgt_id])->PadNull(); break;
            case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint32_t> >(_seg_stack[tgt_id])->PadNull(); break;
            case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint64_t> >(_seg_stack[tgt_id])->PadNull(); break;
            case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<float> >(_seg_stack[tgt_id])->PadNull();    break;
            case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<double> >(_seg_stack[tgt_id])->PadNull();   break;
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
int Table::BatchAddColumn(PIL_PRIMITIVE_TYPE ptype, PIL_PRIMITIVE_TYPE ptype_arr, uint32_t global_id) {
    std::cerr << "target column does NOT Exist in local stack: insert -> " << global_id << std::endl;
    if(record_batch.get() == nullptr) {
        std::cerr << "need to allocate record batch" << std::endl;
        record_batch = std::make_shared<RecordBatch>();
    }
    int col_id = record_batch->AddGlobalField(global_id);

    _seg_stack.push_back(std::unique_ptr<ColumnSet>(new ColumnSet()));

    const uint32_t padding_to = record_batch->n_rec;
    std::cerr << "padding up to: " << padding_to << std::endl;

    int ret_status = 0;
    // Special case when there is no padding we have to force the correct
    // return value.
    if(padding_to == 0) ret_status = 1;

    if(ptype == PIL_TYPE_BYTE_ARRAY){
        for(int j = 0; j < padding_to; ++j) {
           switch(ptype_arr) {
           case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int8_t> >(_seg_stack.back())->Append(0);   break;
           case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int16_t> >(_seg_stack.back())->Append(0);  break;
           case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int32_t> >(_seg_stack.back())->Append(0);  break;
           case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int64_t> >(_seg_stack.back())->Append(0);  break;
           case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint8_t> >(_seg_stack.back())->Append(0);  break;
           case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint16_t> >(_seg_stack.back())->Append(0); break;
           case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint32_t> >(_seg_stack.back())->Append(0); break;
           case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint64_t> >(_seg_stack.back())->Append(0); break;
           case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<float> >(_seg_stack.back())->Append(0);    break;
           case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<double> >(_seg_stack.back())->Append(0);   break;
           default: std::cerr << "no known type: " << ptype_arr << std::endl; ret_status = -1; break;
           }
           assert(ret_status == 1);
        }
    } else {
        for(int j = 0; j < padding_to; ++j) {
           switch(ptype) {
           case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< ColumnSetBuilder<int8_t> >(_seg_stack.back())->Append(0);   break;
           case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int16_t> >(_seg_stack.back())->Append(0);  break;
           case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int32_t> >(_seg_stack.back())->Append(0);  break;
           case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int64_t> >(_seg_stack.back())->Append(0);  break;
           case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< ColumnSetBuilder<uint8_t> >(_seg_stack.back())->Append(0);  break;
           case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint16_t> >(_seg_stack.back())->Append(0); break;
           case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint32_t> >(_seg_stack.back())->Append(0); break;
           case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint64_t> >(_seg_stack.back())->Append(0); break;
           case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< ColumnSetBuilder<float> >(_seg_stack.back())->Append(0);    break;
           case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< ColumnSetBuilder<double> >(_seg_stack.back())->Append(0);   break;
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
int Table::AppendData(const RecordBuilder& builder, const uint32_t slot_offset, std::shared_ptr<ColumnSet> dst_column) {
    PIL_PRIMITIVE_TYPE ptype = builder.slots[slot_offset]->primitive_type;
    PIL_PRIMITIVE_TYPE ptype_arr = builder.slots[slot_offset]->array_primitive_type;

    int ret_status = 0;

    if(ptype == PIL_TYPE_BYTE_ARRAY) {
        switch(ptype_arr) {
        case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int8_t> >(dst_column)->Append(reinterpret_cast<int8_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int16_t> >(dst_column)->Append(reinterpret_cast<int16_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int32_t> >(dst_column)->Append(reinterpret_cast<int32_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int64_t> >(dst_column)->Append(reinterpret_cast<int64_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint8_t> >(dst_column)->Append(reinterpret_cast<uint8_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint16_t> >(dst_column)->Append(reinterpret_cast<uint16_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint32_t> >(dst_column)->Append(reinterpret_cast<uint32_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint64_t> >(dst_column)->Append(reinterpret_cast<uint64_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<float> >(dst_column)->Append(reinterpret_cast<float*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<double> >(dst_column)->Append(reinterpret_cast<double*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
        }
    } else {
        switch(ptype) {
        case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< ColumnSetBuilder<int8_t> >(dst_column)->Append(reinterpret_cast<int8_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int16_t> >(dst_column)->Append(reinterpret_cast<int16_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int32_t> >(dst_column)->Append(reinterpret_cast<int32_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int64_t> >(dst_column)->Append(reinterpret_cast<int64_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< ColumnSetBuilder<uint8_t> >(dst_column)->Append(reinterpret_cast<uint8_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint16_t> >(dst_column)->Append(reinterpret_cast<uint16_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint32_t> >(dst_column)->Append(reinterpret_cast<uint32_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint64_t> >(dst_column)->Append(reinterpret_cast<uint64_t*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< ColumnSetBuilder<float> >(dst_column)->Append(reinterpret_cast<float*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< ColumnSetBuilder<double> >(dst_column)->Append(reinterpret_cast<double*>(builder.slots[slot_offset]->data), builder.slots[slot_offset]->stride); break;
        default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
        }
    }

    return(ret_status);
}

int Table::FinalizeBatch() {
    std::cerr << "record limit reached: " << record_batch->schemas.columns[0]->size() << ":" << record_batch->schemas.GetMemoryUsage() << std::endl;

    // Ugly reset of all data.
    Compressor compressor;
    uint32_t mem_in = 0, mem_out = 0;
    DictionaryEncoder enc;

    // which one is qual?
    //int qual_col = record_batch->global_local_field_map[field_dict.Find("QUAL")];
    //field_dict.dict[field_dict.Find("QUAL")].compression_mode = PIL_COMPRESS_RC_QUAL;


    for(int i = 0; i < _seg_stack.size(); ++i) {
        int ret_status = -1;
        switch(field_dict.dict[record_batch->local_dict[i]].ptype) {
        case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< ColumnSetBuilder<int8_t> >(_seg_stack[i])->ComputeSegmentStats(); break;
        case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int16_t> >(_seg_stack[i])->ComputeSegmentStats(); break;
        case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int32_t> >(_seg_stack[i])->ComputeSegmentStats(); break;
        case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int64_t> >(_seg_stack[i])->ComputeSegmentStats(); break;
        case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< ColumnSetBuilder<uint8_t> >(_seg_stack[i])->ComputeSegmentStats(); break;
        case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint16_t> >(_seg_stack[i])->ComputeSegmentStats(); break;
        case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint32_t> >(_seg_stack[i])->ComputeSegmentStats(); break;
        case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint64_t> >(_seg_stack[i])->ComputeSegmentStats(); break;
        case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< ColumnSetBuilder<float> >(_seg_stack[i])->ComputeSegmentStats(); break;
        case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< ColumnSetBuilder<double> >(_seg_stack[i])->ComputeSegmentStats(); break;
        default: std::cerr << "no known type: " << field_dict.dict[record_batch->local_dict[i]].ptype << std::endl; ret_status = -1; break;
        }

        mem_in += _seg_stack[i]->GetMemoryUsage();

        std::cerr << "global->local: " << record_batch->local_dict[i] << "->" << i << " with n=" << _seg_stack[i]->size() << std::endl;
        //std::cerr << "compressing: " << record_batch->dict[i] << std::endl;
        int ret = compressor.Compress(_seg_stack[i], field_dict.dict[record_batch->local_dict[i]]);
        std::cerr << "compressed: n=" << _seg_stack[i]->size() << " size=" << _seg_stack[i]->GetMemoryUsage() << "->" << ret << " (" << (float)_seg_stack[i]->GetMemoryUsage()/ret << "-fold)" << std::endl;
        mem_out += ret;

    }
    _seg_stack.clear();
    std::cerr << "total: compressed: " << mem_in << "->" << mem_out << "(" << (float)mem_in/mem_out << "-fold)" << std::endl;
    c_in += mem_in;
    c_out += mem_out;

    record_batch = std::make_shared<RecordBatch>();

    return(1);
}

void Table::Describe(std::ostream& stream) {
    stream << "---------------------------------" << std::endl;
    for(int i = 0; i < field_dict.dict.size(); ++i) {
        stream << field_dict.dict[i].field_name << ": ptype " << PIL_PRIMITIVE_TYPE_STRING[field_dict.dict[i].ptype] << " cstore: " << PIL_CSTORE_TYPE_STRING[field_dict.dict[i].cstore] << " compmode: ";
        if(field_dict.dict[i].transforms.size()){
            stream << field_dict.dict[i].transforms[0];
            for(int j = 1; j < field_dict.dict[i].transforms.size(); ++j) {
                stream << "," << field_dict.dict[i].transforms[j];
            }
        } else stream << "auto";
        stream << std::endl;
    }
    stream << "---------------------------------" << std::endl;
}

}
