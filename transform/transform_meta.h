#ifndef TRANSFORM_META_H_
#define TRANSFORM_META_H_

#include <vector>
#include <memory>

#include "../pil.h"

namespace pil {

// Should add at least (in order of addition):
// CODEC, uncompresed size, compressed size, N values, (type, length, data)-tuples
struct TransformMetaTuple {
    TransformMetaTuple() : ptype(PIL_TYPE_UNKNOWN), n_data(0), data(nullptr){}
    ~TransformMetaTuple(){ delete[] data; }

    PIL_PRIMITIVE_TYPE ptype; // ptype of the following data
    int32_t n_data; // number of primitives of ptype in data
    uint8_t* data;
};

struct TransformMeta {
    TransformMeta() : ctype(PIL_COMPRESS_NONE), u_sz(0), c_sz(0), n_tuples(0){}
    TransformMeta(PIL_COMPRESSION_TYPE p) : ctype(p), u_sz(0), c_sz(0), n_tuples(0){}
    TransformMeta(PIL_COMPRESSION_TYPE p, int64_t un_sz) : ctype(p), u_sz(un_sz), c_sz(0), n_tuples(0){}
    TransformMeta(PIL_COMPRESSION_TYPE p, int64_t un_sz, int64_t co_sz) : ctype(p), u_sz(un_sz), c_sz(co_sz), n_tuples(0){}

    PIL_COMPRESSION_TYPE ctype;
    int64_t u_sz, c_sz; // uncompressed/compressed size of the referred columnstore
    int64_t n_tuples; // for serialization only
    std::vector< std::unique_ptr<TransformMetaTuple> > tuples;
};

}



#endif /* TRANSFORM_META_H_ */
