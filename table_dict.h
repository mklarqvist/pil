#ifndef TABLE_DICT_H_
#define TABLE_DICT_H_

#include <cstdlib>
#include <vector>
#include <unordered_map>
#include <iostream>

#include "pil.h"
#include "third_party/xxhash/xxhash.h"

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
    std::vector<PIL_COMPRESSION_TYPE> transforms;
};

/**<
 * Dictionary encoding for Schema Fields. This structure allows us to map
 * from a field name string to a number representing the global identifier.
 */
struct FieldDictionary {
    int32_t Find(const std::string& field_name) {
        std::unordered_map<std::string, uint32_t>::const_iterator s = map.find(field_name);
        if(s != map.end()){
            return(s->second);
        } else {
            return(-1);
        }
    }

    uint32_t FindOrAdd(const std::string& field_name, PIL_PRIMITIVE_TYPE ptype, PIL_PRIMITIVE_TYPE ptype_arr) {
        int32_t column_id = -1;
        std::unordered_map<std::string, uint32_t>::const_iterator s = map.find(field_name);
        if(s != map.end()){
            //std::cerr << "found: " << s->first << "@" << s->second << std::endl;
            //std::cerr << "inserting key: " << field_name << ": " << (int)ptype << "," << ptype_arr << std::endl;
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

}



#endif /* TABLE_DICT_H_ */
