#ifndef TABLE_H_
#define TABLE_H_

#include <unordered_map>

#include "memory_pool.h"
#include "columnstore.h"
#include "record_builder.h"
#include "third_party/xxhash/xxhash.h"
#include "compression/variant_digest_manager.h"

namespace pil {

// Segment represent a contiguous slice of a columnstore.
struct Segment {
public:
    Segment(int32_t id, MemoryPool* pool) : init_marker(0), n_bytes(0), global_id(id), columnset(pool), eof_marker(0){}

    int Write(std::ostream& stream);

    int size() const { return(columnset.columns.size()); }

    // Returns the total number of bytes the ColumnStore buffers use.
    int64_t size_bytes() const {
        int64_t total = 0;
        for(int i = 0; i < columnset.size(); ++i) {
            total += columnset.columns[i]->GetMemoryUsage();
        }

        return total;
    }

public:
    uint64_t init_marker; // initation marker for data
    uint32_t n_bytes;
    int32_t global_id; // have to guarantee parity between this parameter and that in columnset->columns
    ColumnSet columnset;
    uint64_t eof_marker;
};

//
// A SchemaPattern is the collection of ColumnSet identifiers a Record maps to.
// The `ids` vector of identifiers are indirectly encoded as an integer (indirect
// encoding).
struct SchemaPattern {
    uint64_t Hash() const { // return a 64-bit hash of the current pattern
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
    std::string file_name;
    PIL_PRIMITIVE_TYPE ptype;
};

struct FieldDictionary {
    uint32_t FindOrAdd(const std::string& field_name, PIL_PRIMITIVE_TYPE ptype) {
        int32_t column_id = -1;
        std::unordered_map<std::string, uint32_t>::const_iterator s = map.find(field_name);
        if(s != map.end()){
            //std::cerr << "found: " << s->first << "@" << s->second << std::endl;
            column_id = s->second;
        } else {
            std::cerr << "did not find: " << field_name << std::endl;
            column_id = dict.size();
            map[field_name] = column_id;
            dict.push_back(DictionaryFieldType());
            dict.back().file_name = field_name;
            dict.back().ptype = ptype;

        }
        //std::cerr << "id=" << column_id << std::endl;
        return(column_id);
    }

    // Global dictionary of ColumnSet identifiers
    // Field name string -> global identifier (dictionary encoding)
    std::vector<DictionaryFieldType> dict; // Field typing.
    std::unordered_map<std::string, uint32_t> map; // Field dictonary
};

struct PatternDictionary {
    uint32_t FindOrAdd(const SchemaPattern& pattern) {
        const uint64_t phash = pattern.Hash();
        std::unordered_map<uint64_t, uint32_t>::const_iterator s = map.find(phash);
        int pid = 0;
        if(s != map.end()){
            //std::cerr << "found: " << s->first << "@" << s->second << std::endl;
            pid = s->second;
        } else {
            std::cerr << "did not find: " << phash << std::endl;
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

struct Table {
public:
    // Accessors
    //const SegmentIndex& column(int p);

    // Entry point for adding into the Table
    // Append unknown struct and records its schema.
    //
    // Internal process:
    // 1) Provide RecordBuilder instance.
    // 2) Foreach slot in Record.
    //   a) Map field_name -> dictionary-encoded identifier.
    //   b) If field_name is not found then insert and return the newly generated identifier.
    //   c) Add value(s) to the current ColumnSet with that identifier.
    // 3) Update RecordIndex
    int Append(RecordBuilder& builder) {
        // Foreach field name string in the builder record we check if it
        // exists in the dictionary. If it exists we return that value, otherwise
        // we insert it into the dictionary and return the new value.
        SchemaPattern pattern;
        for(int i = 0; i < builder.slots.size(); ++i) {
            int32_t column_id = field_dict.FindOrAdd(builder.slots[i]->field_name, builder.slots[i]->primitive_type);
            pattern.ids.push_back(column_id);
        }

        // We now have the target column id and the pattern
        /*std::cerr << "pattern=[";
        for(int i = 0; i < pattern.ids.size(); ++i) {
            std::cerr << pattern.ids[i] << ",";
        } std::cerr << "]" << std::endl;
        */

        // Store pattern-id with each record.
        uint32_t pid = pattern_dict.FindOrAdd(pattern);

        // Check the local stack of Segments if the target identifier is present.
        for(int i = 0; i < pattern.ids.size(); ++i) {
            int32_t _segid = -1;
            std::unordered_map<uint32_t,uint32_t>::const_iterator s = _seg_map.find(pattern.ids[i]);
            if(s != _seg_map.end()){
                //std::cerr << "Target column exist in local stack: " << s->first << "->" << s->second << std::endl;
                // insert
                _segid = s->second;
            } else {
                std::cerr << "target column does NOT Exist in local stack: insert -> " << pattern.ids[i] << std::endl;
                _segid = _seg_stack.size();
                _seg_map[pattern.ids[i]] = _segid;
                MemoryPool* pool = pil::default_memory_pool();
                _seg_stack.push_back(std::unique_ptr<Segment>(new Segment(pattern.ids[i], pool)));
            }

            //std::cerr << "inserting into stack: " << _segid << std::endl;
            //std::cerr << "stride=" << builder.slots[i]->stride << std::endl;

            // Check for limit.
            ColumnSet& _seg = _seg_stack[_segid]->columnset;
            if(_seg.columns.size()){
                // The first ColumnSet is guaranteed to be the longest.
                // If the number of elements in a ColumnStore in a ColumnSet surpasses or
                // equals the desired size of a Batch then emit this temporary Segment.
                if(_seg.columns.front()->size() >= 4096) {
                    std::cerr << "limit reached for _seg column: " << _seg.size() << "->" << _seg.GetMemoryUsage() << std::endl;
                    // 1: Flush this ColumnSet
                    std::unique_ptr<Segment>& segment = _seg_stack[_segid];
                    segment->global_id = _segid;
                    segment->columnset.global_id = _segid;
                    std::cerr << "stride_size=" << segment->columnset.stride->GetMemoryUsage() << std::endl;
                    uint8_t md5sum[16];
                    Digest::GenerateMd5(segment->columnset.columns.front()->buffer->mutable_data(), segment->columnset.columns.front()->mem_use, md5sum);
                    for(int i = 0; i < 16; ++i) std::cerr << (int)md5sum[i];
                    std::cerr << std::endl;

                    // 2: Clear it and start over.
                    _seg.clear();
                }
            }

            // Append data to the current (unflushed) Segment.
            PIL_PRIMITIVE_TYPE ptype = builder.slots[i]->primitive_type;
            int ret_status = 0;
            switch(ptype) {
            case(PIL_TYPE_INT8):   ret_status = static_cast<ColumnSetBuilder<int8_t>*>(&_seg)->Append(reinterpret_cast<int8_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_INT16):  ret_status = static_cast<ColumnSetBuilder<int16_t>*>(&_seg)->Append(reinterpret_cast<int16_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_INT32):  ret_status = static_cast<ColumnSetBuilder<int32_t>*>(&_seg)->Append(reinterpret_cast<int32_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_INT64):  ret_status = static_cast<ColumnSetBuilder<int64_t>*>(&_seg)->Append(reinterpret_cast<int64_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_UINT8):  ret_status = static_cast<ColumnSetBuilder<uint8_t>*>(&_seg)->Append(reinterpret_cast<uint8_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_UINT16): ret_status = static_cast<ColumnSetBuilder<uint16_t>*>(&_seg)->Append(reinterpret_cast<uint16_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_UINT32): ret_status = static_cast<ColumnSetBuilder<uint32_t>*>(&_seg)->Append(reinterpret_cast<uint32_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_UINT64): ret_status = static_cast<ColumnSetBuilder<uint64_t>*>(&_seg)->Append(reinterpret_cast<uint64_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_FLOAT):  ret_status = static_cast<ColumnSetBuilder<float>*>(&_seg)->Append(reinterpret_cast<float*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_DOUBLE): ret_status = static_cast<ColumnSetBuilder<double>*>(&_seg)->Append(reinterpret_cast<double*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
            }
            assert(ret_status == 1);

            //std::cerr << "pool in " << _segid << ": " << _seg.pool_->bytes_allocated() << std::endl;
        }

        ++builder.n_added;
        builder.slots.clear();
        return(1); // success
    }

protected:
    int FindFieldIdentifier(const std::string& field_name) const;
    int AppendRecord();

protected:
    FieldDictionary field_dict;
    PatternDictionary pattern_dict;

    // Indices.
    //ColumnIndex colindex; // ColumnIndex is updated every time a ColumnSet (Segment) is written to disk.
    //RecordColumnRevIndex revindex;

public: // private:
    // Construction helpers
    std::unordered_map<uint32_t, uint32_t> _seg_map; // Reverse lookup of current loaded Segment stacks and a provided FieldDict identifier.
    std::vector< std::unique_ptr<Segment> > _seg_stack; // temporary Segments used during construction.
};


}



#endif /* TABLE_H_ */
