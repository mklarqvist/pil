#ifndef RECORD_BUILDER_H_
#define RECORD_BUILDER_H_

#include <cstdint>
#include <string>
#include <algorithm>
#include <vector>

#include "pil.h"

namespace pil {

/*------ Record importer --------*/

// Supportive structure for importing abstract schemas that can be
// different for every record.
struct RecordBuilderFields {
public:
    RecordBuilderFields() :
        primitive_type(PIL_TYPE_UNKNOWN),
        array_primitive_type(PIL_TYPE_UNKNOWN),
        n(0), m(4096), stride(0), data(new uint8_t[4096])
    {}
    ~RecordBuilderFields(){ delete[] data; }

    int resize(uint32_t new_size) {
        if(new_size <= m) return 1;

        uint8_t* old = data;
        data = new uint8_t[new_size];
        memcpy(data, old, n);
        delete[] old;
        m = new_size;

        return 1;
    }

public:
    std::string field_name;
    PIL_PRIMITIVE_TYPE primitive_type; // primary type
    PIL_PRIMITIVE_TYPE array_primitive_type; // if primary type is an array then this secondary primary type is used to denote the "actual" primitive type of the byte array
    uint32_t n, m; // number of used bytes, allocated bytes
    uint32_t stride; // number of elements
    uint8_t* data; // actual data
};

struct RecordBuilder {
public:
    RecordBuilder() : n_added(0), n_used(0) {}

    template <class T>
    int AddArray(const std::string& id, PIL_PRIMITIVE_TYPE ptype, const T* value, uint32_t n_values) {
        if(ptype == PIL_TYPE_BYTE_ARRAY || ptype == PIL_TYPE_FIXED_LEN_BYTE_ARRAY || ptype == PIL_TYPE_UNKNOWN) return -1;

        if(n_used == slots.size()) {
            slots.push_back( std::unique_ptr<RecordBuilderFields>(new RecordBuilderFields()) );
        }
        slots[n_used]->field_name = id;
        slots[n_used]->stride = n_values;
        slots[n_used]->primitive_type = PIL_TYPE_BYTE_ARRAY;
        slots[n_used]->array_primitive_type = ptype;

        if(n_values * sizeof(T) > slots[n_used]->m)
            slots[n_used]->resize(n_values * sizeof(T) + 1024);

        for(uint32_t i = 0; i < n_values; ++i){
            reinterpret_cast<T*>(slots[n_used]->data)[i] = value[i];
        }
        slots[n_used]->n = sizeof(T) * n_values;
        ++n_used;

        // Success
        return(1);
    }

    template <class T>
    int AddArray(const std::string& id, PIL_PRIMITIVE_TYPE ptype, const std::vector<T>& values) {
        if(ptype == PIL_TYPE_BYTE_ARRAY || ptype == PIL_TYPE_FIXED_LEN_BYTE_ARRAY || ptype == PIL_TYPE_UNKNOWN) return -1;
        if(n_used == slots.size()) {
            slots.push_back( std::unique_ptr<RecordBuilderFields>(new RecordBuilderFields()) );
        }
        slots[n_used]->field_name = id;
        slots[n_used]->stride = values.size();
        slots[n_used]->primitive_type = PIL_TYPE_BYTE_ARRAY;
        slots[n_used]->array_primitive_type = ptype;

        if(values.size() * sizeof(T) > slots[n_used]->m)
            slots[n_used]->resize(values.size() * sizeof(T) + 1024);

        for(size_t i = 0; i < values.size(); ++i){
            reinterpret_cast<T*>(slots[n_used]->data)[i] = values[i];
        }
        slots[n_used]->n = sizeof(T) * values.size();
        ++n_used;

        // Success
        return(1);
    }

    template <class T>
    int Add(const std::string& id, PIL_PRIMITIVE_TYPE ptype, const T value) {
        if(ptype == PIL_TYPE_BYTE_ARRAY || ptype == PIL_TYPE_FIXED_LEN_BYTE_ARRAY || ptype == PIL_TYPE_UNKNOWN) return -1;
        if(n_used == slots.size()) {
            slots.push_back( std::unique_ptr<RecordBuilderFields>(new RecordBuilderFields()) );
        }
        slots[n_used]->field_name = id;
        slots[n_used]->stride = 1;
        slots[n_used]->primitive_type = ptype;
        reinterpret_cast<T*>(slots[n_used]->data)[0] = value;
        slots[n_used]->n = sizeof(T);
        ++n_used;

        // Success
        return(1);
    }

    template <class T>
    int Add(const std::string& id, PIL_PRIMITIVE_TYPE ptype, const T* value, uint32_t n_values) {
        if(ptype == PIL_TYPE_BYTE_ARRAY || ptype == PIL_TYPE_FIXED_LEN_BYTE_ARRAY || ptype == PIL_TYPE_UNKNOWN) return -1;
        if(n_used == slots.size()) {
            slots.push_back( std::unique_ptr<RecordBuilderFields>(new RecordBuilderFields()) );
        }
        slots[n_used]->field_name = id;
        slots[n_used]->stride = n_values;
        slots[n_used]->primitive_type = ptype;

        if(n_values * sizeof(T) > slots[n_used]->m)
            slots[n_used]->resize(n_values * sizeof(T) + 1024);

        for(int i = 0; i < n_values; ++i){
            reinterpret_cast<T*>(slots[n_used]->data)[i] = value[i];
        }
        slots[n_used]->n = sizeof(T) * n_values;
        ++n_used;

        // Success
        return(1);
    }

    template <class T>
    int Add(const std::string& id, PIL_PRIMITIVE_TYPE ptype, const std::vector<T>& values) {
        if(ptype == PIL_TYPE_BYTE_ARRAY || ptype == PIL_TYPE_FIXED_LEN_BYTE_ARRAY || ptype == PIL_TYPE_UNKNOWN) return -1;
        if(n_used == slots.size()) {
            slots.push_back( std::unique_ptr<RecordBuilderFields>(new RecordBuilderFields()) );
        }
        slots[n_used]->field_name = id;
        slots[n_used]->stride = values.size();
        slots[n_used]->primitive_type = ptype;

        if(values.size() * sizeof(T) > slots[n_used]->m)
            slots[n_used]->resize(values.size() * sizeof(T) + 1024);

        for(size_t i = 0; i < values.size(); ++i){
            reinterpret_cast<T*>(slots[n_used]->data)[i] = values[i];
        }
        slots[n_used]->n = sizeof(T) * values.size();
        ++n_used;

        // Success
        return(1);
    }

    int PrintDebug() {
        for(size_t i = 0; i < slots.size(); ++i){
            std::cerr << "field-" << i << ": " << slots[i]->field_name << ":" << slots[i]->primitive_type << " bytelen=" << slots[i]->n << " n=" << slots[i]->stride << std::endl;
        }

        return(1);
    }

    void reset() {
        n_used = 0;
        for(size_t i = 0; i < slots.size(); ++i) {
            slots[i]->n = 0;
        }
    }

public:
    uint64_t n_added;
    uint32_t n_used;
    std::vector< std::unique_ptr<RecordBuilderFields> > slots;
};

}



#endif /* RECORD_BUILDER_H_ */
