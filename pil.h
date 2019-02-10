#ifndef PIL_H_
#define PIL_H_

namespace pil {

/*------ Core enums --------*/
typedef enum {
    PIL_TYPE_UNKNOWN,
    PIL_TYPE_INT8,
    PIL_TYPE_UINT8,
    PIL_TYPE_INT16,
    PIL_TYPE_UINT16,
    PIL_TYPE_INT32,
    PIL_TYPE_UINT32,
    PIL_TYPE_INT64,
    PIL_TYPE_UINT64,
    PIL_TYPE_FLOAT,
    PIL_TYPE_DOUBLE,
    PIL_TYPE_BOOLEAN,
    PIL_TYPE_BYTE_ARRAY, // these must be followed by a second PIL_PRIMITIVE_TYPE in their implementation
    PIL_TYPE_FIXED_LEN_BYTE_ARRAY
} PIL_PRIMITIVE_TYPE;

typedef enum {
    PIL_CSTORE_UNKNOWN,
    PIL_CSTORE_TENSOR,
    PIL_CSTORE_COLUMN
} PIL_CSTORE_TYPE;

}

#endif /* PIL_H_ */
