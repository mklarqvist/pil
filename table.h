#ifndef TABLE_H_
#define TABLE_H_

#include <algorithm>
#include <unordered_map>

#include "memory_pool.h"
#include "columnstore.h"
#include "record_builder.h"
#include "third_party/xxhash/xxhash.h"
#include "compression/variant_digest_manager.h"

namespace pil {

// Segment represent a contiguous slice of a columnstore on DISK
struct Segment {
public:
    Segment(int32_t id) : init_marker(0), n_bytes(0), global_id(id), columnset(), eof_marker(0){}

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
    std::string field_name;
    PIL_PRIMITIVE_TYPE ptype;
    PIL_PRIMITIVE_TYPE ptype_array;
};

struct FieldDictionary {
    uint32_t FindOrAdd(const std::string& field_name, PIL_PRIMITIVE_TYPE ptype) {
        int32_t column_id = -1;
        std::unordered_map<std::string, uint32_t>::const_iterator s = map.find(field_name);
        if(s != map.end()){
            //std::cerr << "found: " << s->first << "@" << s->second << std::endl;
            if(dict[s->second].ptype != ptype) {
                std::cerr << "ptype mismatch: illegal! -> " << (int)ptype << "!=" << (int)dict[s->second].ptype << std::endl;
                exit(1);
            }
            column_id = s->second;
        } else {
            std::cerr << "did not find: " << field_name << std::endl;
            column_id = dict.size();
            map[field_name] = column_id;
            dict.push_back(DictionaryFieldType());
            dict.back().field_name = field_name;
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

struct SchemaDictionary {
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

struct ColumnSetOffset {
    uint32_t id, segment_id, element_offset;
};

// This inverted index allows us to map each record to the ColumnSet values it
// has in a 1:1 fashion. This index allows us to maintain the individual schema
//
// Every ColumnSet (by default 4096 elements) are emitted as a Segment.
// A collection of Segments are called a Batch. If a particular ColumnSet
// does not appear in the Batch that column identifier will not be used.
//
// We can perform efficient predicate evaluation using vectorized
// instructions (SIMD) on multiple consequtive elements of the dictionary
// encoded patterns.
struct RecordBatch {
    RecordBatch() : n_rec(0), schemas(){}

    void operator++(){ ++n_rec; }

    // Insert a global schema id into the record batch
    int AddSchema(uint32_t pid) {
        int32_t local = FindGlobalPattern(pid);
        if(local == -1) {
            std::cerr << "inserting: " << pid << std::endl;
            global_local_schema_map[pid] = dict.size();
            dict.push_back(pid);
        }

        int insert_status = static_cast<ColumnSetBuilder<uint32_t>*>(&schemas)->Append(pid);
        ++n_rec;
        return(insert_status);
    }

public:
    int32_t FindGlobalPattern(const uint32_t id) const {
        std::unordered_map<uint32_t, uint32_t>::const_iterator ret = global_local_schema_map.find(id);
        if(ret != global_local_schema_map.end()){
            return(ret->second);
        } else return(-1);
    }

public:
    uint32_t n_rec; // Guarantee parity with schemas.columns[0]->n

    // Dictionary-encoding of dictionary-encoded of field identifiers as a _Schema_
    std::vector<uint32_t> dict; // local dict-encoded schema vector
    std::unordered_map<uint32_t, uint32_t> global_local_schema_map; // Map from global schema id -> local schema id
    // ColumnStore for Schemas. ALWAYS cast as uint32_t
    ColumnSet schemas; // Array storage of the local dictionary-encoded BatchPatterns.
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
        if(record_batch.get() == nullptr)
            record_batch = std::make_shared<RecordBatch>();

        // Check if the current RecordBatch has reached its Batch limit.
        // If it has then finalize the Batch.
        if(record_batch->n_rec >= 4096) {
            std::cerr << "record limit reached: " << record_batch->schemas.columns[0]->size() << ":" << record_batch->schemas.GetMemoryUsage() << std::endl;
            // Todo: finalize a record batch
            record_batch = std::make_shared<RecordBatch>();
        }

        std::cerr << "before pattern check: " << std::endl;


        // Foreach field name string in the builder record we check if it
        // exists in the dictionary. If it exists we return that value, otherwise
        // we insert it into the dictionary and return the new value.
        SchemaPattern pattern;
        for(int i = 0; i < builder.slots.size(); ++i) {
            std::cerr << "schema slot " << i << "/" << builder.slots.size() << std::endl;
            std::cerr << "test=" << builder.slots[i]->field_name << std::endl;
            int32_t column_id = field_dict.FindOrAdd(builder.slots[i]->field_name, builder.slots[i]->primitive_type);
            pattern.ids.push_back(column_id);
        }

        std::cerr << "pattern=" << pattern.ids.size() << std::endl;

        // Store Schema-id with each record.
        uint32_t pid = schema_dict.FindOrAdd(pattern);
        std::cerr << "pid=" << pid << std::endl;

        // Check the local stack of ColumnSets if the target identifier is present.
        for(int i = 0; i < pattern.ids.size(); ++i) {
            std::cerr << "checking: " << pattern.ids[i] << "..." << std::endl;
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
                _seg_stack.push_back(std::unique_ptr<ColumnSet>(new ColumnSet()));

                PIL_PRIMITIVE_TYPE ptype = builder.slots[i]->primitive_type;

                // todo: insert padding from 0 to current offset in record batch
                const uint32_t padding_to = record_batch->n_rec;
                std::cerr << "padding up to: " << padding_to << std::endl;
                for(int j = 0; j < padding_to; ++j){
                    int ret_status = 0;
                   switch(ptype) {
                   case(PIL_TYPE_INT8):   ret_status = static_cast<ColumnSetBuilder<int8_t>*>(_seg_stack.back().get())->Append(0); break;
                   case(PIL_TYPE_INT16):  ret_status = static_cast<ColumnSetBuilder<int16_t>*>(_seg_stack.back().get())->Append(0); break;
                   case(PIL_TYPE_INT32):  ret_status = static_cast<ColumnSetBuilder<int32_t>*>(_seg_stack.back().get())->Append(0); break;
                   case(PIL_TYPE_INT64):  ret_status = static_cast<ColumnSetBuilder<int64_t>*>(_seg_stack.back().get())->Append(0); break;
                   case(PIL_TYPE_UINT8):  ret_status = static_cast<ColumnSetBuilder<uint8_t>*>(_seg_stack.back().get())->Append(0); break;
                   case(PIL_TYPE_UINT16): ret_status = static_cast<ColumnSetBuilder<uint16_t>*>(_seg_stack.back().get())->Append(0); break;
                   case(PIL_TYPE_UINT32): ret_status = static_cast<ColumnSetBuilder<uint32_t>*>(_seg_stack.back().get())->Append(0); break;
                   case(PIL_TYPE_UINT64): ret_status = static_cast<ColumnSetBuilder<uint64_t>*>(_seg_stack.back().get())->Append(0); break;
                   case(PIL_TYPE_FLOAT):  ret_status = static_cast<ColumnSetBuilder<float>*>(_seg_stack.back().get())->Append(0); break;
                   case(PIL_TYPE_DOUBLE): ret_status = static_cast<ColumnSetBuilder<double>*>(_seg_stack.back().get())->Append(0); break;
                   default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
                   }
                   assert(ret_status == 1);
                }
            }


            std::cerr << "inserting into stack: " << _segid << std::endl;
            std::cerr << "stride=" << builder.slots[i]->stride << std::endl;

            // Append data to the current (unflushed) Segment.
            PIL_PRIMITIVE_TYPE ptype = builder.slots[i]->primitive_type;
            int ret_status = 0;
            switch(ptype) {
            case(PIL_TYPE_INT8):   ret_status = static_cast<ColumnSetBuilder<int8_t>*>(_seg_stack[_segid].get())->Append(reinterpret_cast<int8_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_INT16):  ret_status = static_cast<ColumnSetBuilder<int16_t>*>(_seg_stack[_segid].get())->Append(reinterpret_cast<int16_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_INT32):  ret_status = static_cast<ColumnSetBuilder<int32_t>*>(_seg_stack[_segid].get())->Append(reinterpret_cast<int32_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_INT64):  ret_status = static_cast<ColumnSetBuilder<int64_t>*>(_seg_stack[_segid].get())->Append(reinterpret_cast<int64_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_UINT8):  ret_status = static_cast<ColumnSetBuilder<uint8_t>*>(_seg_stack[_segid].get())->Append(reinterpret_cast<uint8_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_UINT16): ret_status = static_cast<ColumnSetBuilder<uint16_t>*>(_seg_stack[_segid].get())->Append(reinterpret_cast<uint16_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_UINT32): ret_status = static_cast<ColumnSetBuilder<uint32_t>*>(_seg_stack[_segid].get())->Append(reinterpret_cast<uint32_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_UINT64): ret_status = static_cast<ColumnSetBuilder<uint64_t>*>(_seg_stack[_segid].get())->Append(reinterpret_cast<uint64_t*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_FLOAT):  ret_status = static_cast<ColumnSetBuilder<float>*>(_seg_stack[_segid].get())->Append(reinterpret_cast<float*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            case(PIL_TYPE_DOUBLE): ret_status = static_cast<ColumnSetBuilder<double>*>(_seg_stack[_segid].get())->Append(reinterpret_cast<double*>(builder.slots[i]->data), builder.slots[i]->stride); break;
            default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
            }
            assert(ret_status == 1);

            //std::cerr << "pool in " << _segid << ": " << _seg.pool_->bytes_allocated() << std::endl;
        }

        // Note: Adding a pattern automatically increments the record count in a RecordBatch.
        record_batch->AddSchema(pid);

        // todo: for all columns that were NOT in the pattern
        // we have to pad those columns with a NULL value
        // compare two sorted lists in O(A + B + 1)-time.
        std::sort(pattern.ids.begin(), pattern.ids.end());
        uint32_t finger = 0;
        for(int i = 0; i < field_dict.dict.size(); ++i){
            if(pattern.ids[finger] == i){ std::cerr << "skipping: " << i << std::endl; continue; }
            // pad with nulls
            std::cerr << "padding id: " << i << std::endl;
            PIL_PRIMITIVE_TYPE ptype = field_dict.dict[i].ptype;
            int ret_status = 0;
            switch(ptype) {
            case(PIL_TYPE_INT8):   ret_status = static_cast<ColumnSetBuilder<int8_t>*>(_seg_stack[i].get())->PadNull(); break;
            case(PIL_TYPE_INT16):  ret_status = static_cast<ColumnSetBuilder<int16_t>*>(_seg_stack[i].get())->PadNull(); break;
            case(PIL_TYPE_INT32):  ret_status = static_cast<ColumnSetBuilder<int32_t>*>(_seg_stack[i].get())->PadNull(); break;
            case(PIL_TYPE_INT64):  ret_status = static_cast<ColumnSetBuilder<int64_t>*>(_seg_stack[i].get())->PadNull(); break;
            case(PIL_TYPE_UINT8):  ret_status = static_cast<ColumnSetBuilder<uint8_t>*>(_seg_stack[i].get())->PadNull(); break;
            case(PIL_TYPE_UINT16): ret_status = static_cast<ColumnSetBuilder<uint16_t>*>(_seg_stack[i].get())->PadNull(); break;
            case(PIL_TYPE_UINT32): ret_status = static_cast<ColumnSetBuilder<uint32_t>*>(_seg_stack[i].get())->PadNull(); break;
            case(PIL_TYPE_UINT64): ret_status = static_cast<ColumnSetBuilder<uint64_t>*>(_seg_stack[i].get())->PadNull(); break;
            case(PIL_TYPE_FLOAT):  ret_status = static_cast<ColumnSetBuilder<float>*>(_seg_stack[i].get())->PadNull(); break;
            case(PIL_TYPE_DOUBLE): ret_status = static_cast<ColumnSetBuilder<double>*>(_seg_stack[i].get())->PadNull(); break;
            default: std::cerr << "no known type: " << ptype << std::endl; ret_status = -1; break;
            }
            assert(ret_status == 1);
        }

        ++builder.n_added;
        builder.slots.clear();
        return(1); // success
    }

protected:
    int FindFieldIdentifier(const std::string& field_name) const;

public:
    FieldDictionary field_dict;
    SchemaDictionary schema_dict;
    std::shared_ptr<RecordBatch> record_batch;

    // Indices.
    //ColumnIndex colindex; // ColumnIndex is updated every time a ColumnSet (Segment) is written to disk.
    //RecordColumnRevIndex revindex;

public: // private:
    // Construction helpers
    std::unordered_map<uint32_t, uint32_t> _seg_map; // Reverse lookup of current loaded Segment stacks and a provided FieldDict identifier.
    std::vector< std::unique_ptr<ColumnSet> > _seg_stack; // temporary Segments used during construction.
};


}



#endif /* TABLE_H_ */
