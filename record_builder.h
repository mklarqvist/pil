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
    int32_t n, m; // number of used bytes, allocated bytes
    uint32_t stride; // number of elements
    uint8_t* data; // actual data
};

struct RecordBuilder {
public:
    template <class T>
    int AddArray(const std::string& id, PIL_PRIMITIVE_TYPE ptype, const T* value, uint32_t n_values) {
        if(ptype == PIL_TYPE_BYTE_ARRAY || ptype == PIL_TYPE_FIXED_LEN_BYTE_ARRAY || ptype == PIL_TYPE_UNKNOWN) return -1;
        slots.push_back( std::unique_ptr<RecordBuilderFields>(new RecordBuilderFields()) );
        slots.back()->field_name = id;
        slots.back()->stride = n_values;
        slots.back()->primitive_type = PIL_TYPE_BYTE_ARRAY;
        slots.back()->array_primitive_type = ptype;

        if(n_values * sizeof(T) > slots.back()->m)
            slots.back()->resize(n_values * sizeof(T) + 1024);

        for(int i = 0; i < n_values; ++i){
            reinterpret_cast<T*>(slots.back()->data)[i] = value[i];
        }
        slots.back()->n = sizeof(T) * n_values;

        // Success
        return(1);
    }

    template <class T>
    int AddArray(const std::string& id, PIL_PRIMITIVE_TYPE ptype, const std::vector<T>& values) {
        if(ptype == PIL_TYPE_BYTE_ARRAY || ptype == PIL_TYPE_FIXED_LEN_BYTE_ARRAY || ptype == PIL_TYPE_UNKNOWN) return -1;
        slots.push_back( std::unique_ptr<RecordBuilderFields>(new RecordBuilderFields()) );
        slots.back()->field_name = id;
        slots.back()->stride = values.size();
        slots.back()->primitive_type = PIL_TYPE_BYTE_ARRAY;
        slots.back()->array_primitive_type = ptype;

        if(values.size() * sizeof(T) > slots.back()->m)
            slots.back()->resize(values.size() * sizeof(T) + 1024);

        for(int i = 0; i < values.size(); ++i){
            reinterpret_cast<T*>(slots.back()->data)[i] = values[i];
        }
        slots.back()->n = sizeof(T) * values.size();

        // Success
        return(1);
    }

    template <class T>
    int Add(const std::string& id, PIL_PRIMITIVE_TYPE ptype, const T value) {
        if(ptype == PIL_TYPE_BYTE_ARRAY || ptype == PIL_TYPE_FIXED_LEN_BYTE_ARRAY || ptype == PIL_TYPE_UNKNOWN) return -1;
        slots.push_back( std::unique_ptr<RecordBuilderFields>(new RecordBuilderFields()) );
        slots.back()->field_name = id;
        slots.back()->stride = 1;
        slots.back()->primitive_type = ptype;
        reinterpret_cast<T*>(slots.back()->data)[0] = value;
        slots.back()->n = sizeof(T);

        // Success
        return(1);
    }

    template <class T>
    int Add(const std::string& id, PIL_PRIMITIVE_TYPE ptype, const T* value, uint32_t n_values) {
        if(ptype == PIL_TYPE_BYTE_ARRAY || ptype == PIL_TYPE_FIXED_LEN_BYTE_ARRAY || ptype == PIL_TYPE_UNKNOWN) return -1;
        slots.push_back( std::unique_ptr<RecordBuilderFields>(new RecordBuilderFields()) );
        slots.back()->field_name = id;
        slots.back()->stride = n_values;
        slots.back()->primitive_type = ptype;

        if(n_values * sizeof(T) > slots.back()->m)
            slots.back()->resize(n_values * sizeof(T) + 1024);

        for(int i = 0; i < n_values; ++i){
            reinterpret_cast<T*>(slots.back()->data)[i] = value[i];
        }
        slots.back()->n = sizeof(T) * n_values;

        // Success
        return(1);
    }

    template <class T>
    int Add(const std::string& id, PIL_PRIMITIVE_TYPE ptype, const std::vector<T>& values) {
        if(ptype == PIL_TYPE_BYTE_ARRAY || ptype == PIL_TYPE_FIXED_LEN_BYTE_ARRAY || ptype == PIL_TYPE_UNKNOWN) return -1;
        slots.push_back( std::unique_ptr<RecordBuilderFields>(new RecordBuilderFields()) );
        slots.back()->field_name = id;
        slots.back()->stride = values.size();
        slots.back()->primitive_type = ptype;

        if(values.size() * sizeof(T) > slots.back()->m)
            slots.back()->resize(values.size() * sizeof(T) + 1024);

        for(int i = 0; i < values.size(); ++i){
            reinterpret_cast<T*>(slots.back()->data)[i] = values[i];
        }
        slots.back()->n = sizeof(T) * values.size();

        // Success
        return(1);
    }

    // Partial specialization for std::string
    int Add(const std::string& id, PIL_PRIMITIVE_TYPE ptype, const std::string& values);

    int PrintDebug() {
        for(int i = 0; i < slots.size(); ++i){
            std::cerr << "field-" << i << ": " << slots[i]->field_name << ":" << slots[i]->primitive_type << " bytelen=" << slots[i]->n << " n=" << slots[i]->stride << std::endl;
        }

        return(1);
    }

public:
    int n_added;
    uint32_t batch_size;

    // Vector of slots.
    std::vector< std::unique_ptr<RecordBuilderFields> > slots;
};

}



#endif /* RECORD_BUILDER_H_ */
