#include "table.h"

namespace pil {

int TableConstructor::Append(RecordBuilder& builder) {
    if(meta_data.batches.size() == 0)
        meta_data.batches.push_back(std::make_shared<RecordBatch>());

    // Check if the current RecordBatch has reached its Batch limit.
    // If it has then finalize the Batch.
    if(meta_data.batches.back()->n_rec >= 8192) {
        uint32_t batch_id = meta_data.batches.size() == 0 ? 0 : meta_data.batches.size() - 1;
        //std::cerr << "FINALIZING: " << batch_id << std::endl;
        FinalizeBatch(batch_id);
    }

    //std::cerr << "ADDING: " << builder.slots[0]->field_name;
    //for(int i = 1; i < builder.slots.size(); ++i) {
    //    std::cerr << "," << builder.slots[i]->field_name;
    //}
    //std::cerr << std::endl;

    // Foreach field name string in the builder record we check if it
    // exists in the dictionary. If it exists we return that value, otherwise
    // we insert it into the dictionary and return the new value.
    SchemaPattern pattern;
    std::unordered_map<uint32_t, uint32_t> pattern_map;

    for(size_t i = 0; i < builder.slots.size(); ++i) {
        if(field_dict.Find(builder.slots[i]->field_name) == -1) {
            meta_data.field_meta.push_back(std::make_shared<FieldMetaData>());
        }
        int32_t column_id = field_dict.FindOrAdd(builder.slots[i]->field_name,
                                                 builder.slots[i]->primitive_type,
                                                 builder.slots[i]->array_primitive_type);
        assert(column_id != -1);
        pattern.ids.push_back(column_id);
        pattern_map[column_id] = 1;
        //std::cerr << "pattern_map = " << column_id << std::endl;
    }

    // Store Schema-id with each record.
    uint32_t pid = schema_dict.FindOrAdd(pattern);

    // Check the local stack of ColumnSets if the target identifier is present.
    // If the target identifier is NOT available then we insert a new ColumnSet
    // with that identifier in the current Batch and pad with NULL values up
    // to the current offset.
    for(size_t i = 0; i < pattern.ids.size(); ++i) {
        //std::cerr << "checking: " << pattern.ids[i] << "..." << std::endl;
        int32_t _segid = meta_data.batches.back()->FindLocalField(pattern.ids[i]);
        if(_segid == -1) {
            //std::cerr << "target column does NOT Exist in local stack: insert -> " << pattern.ids[i] << " -> " << builder.slots[i]->field_name << std::endl;
            _segid = BatchAddColumn(builder.slots[i]->primitive_type, builder.slots[i]->array_primitive_type, pattern.ids[i]);
            //std::cerr << "_segid=" << _segid << std::endl;
            assert(_segid != -1);
        }

        // Actual addition of data.
        assert(AppendData(builder, i, build_csets[_segid]) == 1);
    }

    // Map GLOBAL to LOCAL Schema in the current RecordBatch.
    // Note: Adding a pattern automatically increments the record count in a RecordBatch.
    meta_data.batches.back()->AddSchema(pid);

    // CRITICAL!
    // Every Field in the current RecordBatch that is NOT in the current
    // Schema MUST be padded with NULLs for the current Schema. This is to
    // maintain the matrix-relationship between tuples and the ColumnSets.

    std::vector<uint32_t> pad_tgts; // Vector to hold values to be padded.

    for(size_t i = 0; i < meta_data.batches.back()->local_dict.size(); ++i) {
        auto it = pattern_map.find(meta_data.batches.back()->local_dict[i]);
        if(it == pattern_map.end()) {
            //auto x = meta_data.batches.back()->global_local_field_map.find(meta_data.batches.back()->local_dict[i]);
            //std::cerr << "can't find = " << meta_data.batches.back()->local_dict[i] << " local= " << x->second << std::endl;
            pad_tgts.push_back(meta_data.batches.back()->local_dict[i]); // if not in set append it to vector
        }
    }

    for(size_t i = 0; i < pad_tgts.size(); ++i) {
        // pad with nulls
        int32_t tgt_id = meta_data.batches.back()->FindLocalField(pad_tgts[i]);
        assert(build_csets[tgt_id].get() != nullptr);
        //std::cerr << "padding id: " << i << "->" << tgt_id << "/" << meta_data.batches.back()->local_dict.size() << std::endl;
        if(tgt_id == -1) {
            for(size_t k = 0; k < meta_data.batches.back()->local_dict.size(); ++k) {
                std::cerr << meta_data.batches.back()->local_dict[k] << std::endl;
            }
        }
        assert(tgt_id != -1);

        PIL_PRIMITIVE_TYPE ptype = field_dict.dict[pad_tgts[i]].ptype;
        PIL_CSTORE_TYPE ctype = field_dict.dict[pad_tgts[i]].cstore;

        int ret_status = 0;
        if(ctype == PIL_CSTORE_COLUMN){
            switch(ptype) {
            case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< ColumnSetBuilder<int8_t> >(build_csets[tgt_id])->PadNull();   break;
            case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int16_t> >(build_csets[tgt_id])->PadNull();  break;
            case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int32_t> >(build_csets[tgt_id])->PadNull();  break;
            case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int64_t> >(build_csets[tgt_id])->PadNull();  break;
            case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< ColumnSetBuilder<uint8_t> >(build_csets[tgt_id])->PadNull();  break;
            case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint16_t> >(build_csets[tgt_id])->PadNull(); break;
            case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint32_t> >(build_csets[tgt_id])->PadNull(); break;
            case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint64_t> >(build_csets[tgt_id])->PadNull(); break;
            case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< ColumnSetBuilder<float> >(build_csets[tgt_id])->PadNull();    break;
            case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< ColumnSetBuilder<double> >(build_csets[tgt_id])->PadNull();   break;
            default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
            }
        } else {
            switch(ptype) {
            case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int8_t> >(build_csets[tgt_id])->PadNull();   break;
            case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int16_t> >(build_csets[tgt_id])->PadNull();  break;
            case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int32_t> >(build_csets[tgt_id])->PadNull();  break;
            case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int64_t> >(build_csets[tgt_id])->PadNull();  break;
            case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint8_t> >(build_csets[tgt_id])->PadNull();  break;
            case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint16_t> >(build_csets[tgt_id])->PadNull(); break;
            case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint32_t> >(build_csets[tgt_id])->PadNull(); break;
            case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint64_t> >(build_csets[tgt_id])->PadNull(); break;
            case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<float> >(build_csets[tgt_id])->PadNull();    break;
            case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<double> >(build_csets[tgt_id])->PadNull();   break;
            default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
            }
        }
        assert(ret_status == 1);
    }

    ++builder.n_added;
    builder.reset();
    //builder.slots.clear();
    return(1); // success
}

// private
int TableConstructor::BatchAddColumn(PIL_PRIMITIVE_TYPE ptype,
                          PIL_PRIMITIVE_TYPE ptype_arr,
                          uint32_t global_id)
{
    //std::cerr << "target column does NOT Exist in local stack: insert -> " << global_id << std::endl;
    if(meta_data.batches.size() == 0) {
        //std::cerr << "need to allocate record batch" << std::endl;
        meta_data.batches.push_back(std::make_shared<RecordBatch>());
    }
    int col_id = meta_data.batches.back()->AddGlobalField(global_id);

    build_csets.push_back(std::unique_ptr<ColumnSet>(new ColumnSet()));

    const uint32_t padding_to = meta_data.batches.back()->n_rec;
    if(padding_to != 0) std::cerr << "padding up to: " << padding_to << std::endl;

    int ret_status = 0;
    // Special case when there is no padding we have to force the correct
    // return value.
    if(padding_to == 0) ret_status = 1;

    if(ptype == PIL_TYPE_BYTE_ARRAY) {
        for(uint32_t j = 0; j < padding_to; ++j) {
           switch(ptype_arr) {
           case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int8_t> >(build_csets.back())->PadNull();   break;
           case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int16_t> >(build_csets.back())->PadNull();  break;
           case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int32_t> >(build_csets.back())->PadNull();  break;
           case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<int64_t> >(build_csets.back())->PadNull();  break;
           case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint8_t> >(build_csets.back())->PadNull();  break;
           case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint16_t> >(build_csets.back())->PadNull(); break;
           case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint32_t> >(build_csets.back())->PadNull(); break;
           case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<uint64_t> >(build_csets.back())->PadNull(); break;
           case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<float> >(build_csets.back())->PadNull();    break;
           case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< ColumnSetBuilderTensor<double> >(build_csets.back())->PadNull();   break;
           default: std::cerr << "no known type: " << ptype_arr << std::endl; ret_status = -1; break;
           }
           assert(ret_status == 1);
        }
    } else {
        for(uint32_t j = 0; j < padding_to; ++j) {
           switch(ptype) {
           case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< ColumnSetBuilder<int8_t> >(build_csets.back())->PadNull();   break;
           case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int16_t> >(build_csets.back())->PadNull();  break;
           case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int32_t> >(build_csets.back())->PadNull();  break;
           case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< ColumnSetBuilder<int64_t> >(build_csets.back())->PadNull();  break;
           case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< ColumnSetBuilder<uint8_t> >(build_csets.back())->PadNull();  break;
           case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint16_t> >(build_csets.back())->PadNull(); break;
           case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint32_t> >(build_csets.back())->PadNull(); break;
           case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< ColumnSetBuilder<uint64_t> >(build_csets.back())->PadNull(); break;
           case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< ColumnSetBuilder<float> >(build_csets.back())->PadNull();    break;
           case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< ColumnSetBuilder<double> >(build_csets.back())->PadNull();   break;
           default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
           }
           assert(ret_status == 1);
        }
    }

    //std::cerr << "AFTER padding=" << build_csets.back()->n << std::endl;

    //std::cerr << "returning: " << col_id << " with ret_status=" << ret_status << std::endl;
    if(ret_status == 1) return(col_id);
    else return(-1);
}

// private
int TableConstructor::AppendData(const RecordBuilder& builder,
                      const uint32_t slot_offset,
                      std::shared_ptr<ColumnSet> dst_column)
{
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

int TableConstructor::AddRecordBatchSchemas(std::shared_ptr<ColumnSet> cset, const uint32_t batch_id) {
    if(cset.get() == nullptr) return(-1);

    // Todo: this might be expensive
    Compressor compressor;

    // Core updates: schemas
    // Append a new FieldMetaData to the Core FieldMetaData list and use that offset.
    meta_data.core_meta.push_back(std::make_shared<FieldMetaData>());

    // Adding a RecordBatch to the core FieldMetaData will return the offset it was added to.
    uint32_t core_batch_id = meta_data.core_meta[batch_id]->AddBatch(cset);
    // Compress the Schema identifiers for this RecordBatch.
    static_cast<ZstdCompressor*>(&compressor)->Compress(cset, PIL_CSTORE_COLUMN, PIL_ZSTD_DEFAULT_LEVEL);
    //std::cerr << "SCHEMAS=" << cset->columns[0]->uncompressed_size << "->" << cset->columns[0]->compressed_size << std::endl;
    //std::cerr << "core-id=" << core_batch_id << "/" << meta_data.core_meta.back()->cset_meta.size() << std::endl;
    meta_data.core_meta[batch_id]->cset_meta[core_batch_id]->UpdateColumnSet(meta_data.batches[batch_id]->schemas);

    // Serialize batches
    meta_data.core_meta[0]->cset_meta[core_batch_id]->column_meta_data.back()->file_offset = out_stream.tellp(); // start of the data
    //std::cerr << "Serializing BATCHES" << std::endl;
    // Write out the ColumnSet to disk in the appropriate place / file.

    // Todo: if not single archive
    // This is bad!!!!!!!!!!!!!
    meta_data.core_meta[0]->cset_meta[core_batch_id]->SerializeColumnSet(meta_data.batches[batch_id]->schemas, out_stream);

    return(core_batch_id);
}

int TableConstructor::FinalizeBatch(const uint32_t batch_id) {
    Compressor compressor;
    uint32_t mem_in = 0, mem_out = 0;

    // Add the Schemas for the RecordBatch to the meta data indices.
    // This function returns the offset where the data was added. This offset
    // is used again later to update statistics following compression / encoding.
    int core_batch_id = AddRecordBatchSchemas(meta_data.batches[batch_id]->schemas, batch_id);
    if(core_batch_id < 0) {
        std::cerr << "add batch corruption" << std::endl;
        exit(1);
    }

    // This is NOT thread safe.
    for(size_t i = 0; i < build_csets.size(); ++i) {
        uint32_t global_id = meta_data.batches[batch_id]->local_dict[i];
        // Add ColumnSet to the FieldMetaData
        int target = meta_data.AddColumnSet(build_csets[i], global_id, field_dict);

        // Update memory usage.
        mem_in += build_csets[i]->GetMemoryUsage();

        // Compress ColumnSet according as described in the paired FieldMeta
        // record or automatically.
        int ret = compressor.Compress(build_csets[i], field_dict.dict[global_id]);
        std::cerr << field_dict.dict[global_id].field_name << "\t" << "compressed: n=" << build_csets[i]->size() << " size=" << build_csets[i]->GetMemoryUsage() << "->" << ret << " (" << (float)build_csets[i]->GetMemoryUsage()/ret << "-fold)" << std::endl;

        // Update the target meta information with the new compressed data sizes.
        meta_data.UpdateColumnSet(build_csets[i], global_id, target);

        // Write out.

        // Todo: write data to single archive if split is deactivated
        std::shared_ptr<FieldMetaData> tgt_meta_field = meta_data.field_meta[global_id];
        if(single_archive == false && tgt_meta_field->open_writer == false)
            tgt_meta_field->OpenWriter("/Users/Mivagallery/Desktop/pil/test_" + field_dict.dict[global_id].field_name);

        // Write out the ColumnSet to disk in the appropriate place / file.
        if(single_archive == false) tgt_meta_field->SerializeColumnSet(build_csets[i]);
        else tgt_meta_field->SerializeColumnSet(build_csets[i], out_stream);

        mem_out += ret;
    }

    // Todo: do not release memory after each Batch update.
    build_csets.clear();
    std::cerr << "total: compressed: " << mem_in << "->" << mem_out << "(" << (float)mem_in/mem_out << "-fold)" << std::endl;
    c_in  += mem_in;
    c_out += mem_out;

    //meta_data.batches.back()->Serialize(out_stream);

    // Push back a new RecordBatch
    meta_data.batches.push_back(std::make_shared<RecordBatch>());

    return(1);
}

int TableConstructor::Finalize() {
    uint32_t batch_id = meta_data.batches.size() == 0 ? 0 : meta_data.batches.size() - 1;
    int ok = FinalizeBatch(batch_id);
    assert(ok != -1);
    ok = meta_data.Serialize(out_stream);
    assert(ok != -1);
    return(out_stream.good());
}

void TableConstructor::Describe(std::ostream& stream) {
    stream << "---------------------------------" << std::endl;
    for(size_t i = 0; i < field_dict.dict.size(); ++i) {
        stream << field_dict.dict[i].field_name << ": ptype " << PIL_PRIMITIVE_TYPE_STRING[field_dict.dict[i].ptype] << " cstore: " << PIL_CSTORE_TYPE_STRING[field_dict.dict[i].cstore] << " n=" << meta_data.field_meta[i]->TotalOccurences() << " compmode: ";
        if(field_dict.dict[i].transforms.size()) {
            stream << field_dict.dict[i].transforms[0];
            for(size_t j = 1; j < field_dict.dict[i].transforms.size(); ++j) {
                stream << "," << field_dict.dict[i].transforms[j];
            }
        } else stream << "auto";
        stream << " Average cols=" << meta_data.field_meta[i]->AverageColumns() << ", batches_n=" << meta_data.field_meta[i]->TotalCount() << " mean-comp: " << meta_data.field_meta[i]->AverageCompressionFold() << "-fold";
        stream << " U=" << meta_data.field_meta[i]->TotalUncompressed() << " C=" << meta_data.field_meta[i]->TotalCompressed();
        stream << std::endl;
    }
    stream << "---------------------------------" << std::endl;

   return;

    stream << "Batches=" << meta_data.batches.size() << std::endl;
    for(size_t i = 0; i < meta_data.batches.size(); ++i) {
        // Residual RecordBatch.
        //if(meta_data.batches[i]->file_offset == 0 && i != 0) break;
        //stream << "Offset=" << meta_data.batches[i]->file_offset << ", n=" << meta_data.batches[i]->n_rec << " fields=[" << field_dict.dict[meta_data.batches[i]->local_dict[0]].field_name;

        for(size_t j = 1; j < meta_data.batches[i]->local_dict.size(); ++j) {
            stream << ", " << field_dict.dict[meta_data.batches[i]->local_dict[j]].field_name;
        }
        stream << "]" << std::endl;
    }
    stream << "---------------------------------" << std::endl;

    //return;

    stream << "Fields=" << meta_data.field_meta.size() << std::endl;
    for(size_t i = 0; i < meta_data.field_meta.size(); ++i) {
        stream << "\t" << field_dict.dict[i].field_name << ": " << meta_data.field_meta[i]->cset_meta.size() << std::endl;
        stream << "\t\tbatches=[";
        if(meta_data.field_meta[i]->cset_meta[0]->column_meta_data.size()) stream << meta_data.field_meta[i]->cset_meta[0]->record_batch_id;
        for(size_t j = 1; j < meta_data.field_meta[i]->cset_meta.size(); ++j) {
            stream << "," << meta_data.field_meta[i]->cset_meta[j]->record_batch_id;
        }
        stream << "]" << std::endl;

        stream << "\t\tcsets=[\n";
        for(size_t k = 0; k < meta_data.field_meta[i]->cset_meta.size(); ++k) {
            stream << "\t\t\t[" << meta_data.field_meta[i]->cset_meta[k]->column_meta_data[0]->n;
            for(size_t j = 1; j < meta_data.field_meta[i]->cset_meta[k]->column_meta_data.size(); ++j) {
                stream << "," << meta_data.field_meta[i]->cset_meta[k]->column_meta_data[j]->n;
            }
            stream << "]\n";
        }
        stream << "\t\t]" << std::endl;
    }
}

}
