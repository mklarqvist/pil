#ifndef TRANSFORM_META_H_
#define TRANSFORM_META_H_

#include <vector>
#include <memory>

#include "../pil.h"
#include "variant_digest_manager.h"

namespace pil {

// Should add at least (in order of addition):
// CODEC, uncompresed size, compressed size, N values, (type, length, data)-tuples
struct TransformMetaTuple {
    TransformMetaTuple() : ptype(PIL_TYPE_UNKNOWN), n_data(0), data(nullptr){}
    ~TransformMetaTuple(){ delete[] data; }

    int Serialize(std::ostream& stream) {
        stream.write(reinterpret_cast<char*>(&ptype),  sizeof(PIL_PRIMITIVE_TYPE));
        stream.write(reinterpret_cast<char*>(&n_data), sizeof(int32_t));
        if(n_data) stream.write(reinterpret_cast<char*>(data), n_data);
        return(1);
    }

    PIL_PRIMITIVE_TYPE ptype; // ptype of the following data
    int32_t n_data; // number of primitives of ptype in data
    uint8_t* data;
};

struct TransformMeta {
    TransformMeta() : ctype(PIL_COMPRESS_NONE), u_sz(0), c_sz(0), n_tuples(0){ memset(md5_checksum, 0, 16); }
    TransformMeta(PIL_COMPRESSION_TYPE p) : ctype(p), u_sz(0), c_sz(0), n_tuples(0){ memset(md5_checksum, 0, 16); }
    TransformMeta(PIL_COMPRESSION_TYPE p, int64_t un_sz) : ctype(p), u_sz(un_sz), c_sz(0), n_tuples(0){ memset(md5_checksum, 0, 16); }
    TransformMeta(PIL_COMPRESSION_TYPE p, int64_t un_sz, int64_t co_sz) : ctype(p), u_sz(un_sz), c_sz(co_sz), n_tuples(0){ memset(md5_checksum, 0, 16); }

    int Serialize(std::ostream& stream) {
        stream.write(reinterpret_cast<char*>(&ctype), sizeof(PIL_COMPRESSION_TYPE));
        stream.write(reinterpret_cast<char*>(&u_sz),  sizeof(int64_t));
        stream.write(reinterpret_cast<char*>(&c_sz),  sizeof(int64_t));
        stream.write(reinterpret_cast<char*>(md5_checksum), 16);
        n_tuples = tuples.size();
        stream.write(reinterpret_cast<char*>(&n_tuples), sizeof(int64_t));
        for(int i = 0; i < n_tuples; ++i) tuples[i]->Serialize(stream);
        return(1);
    }

    void SetChecksum(const uint8_t* md5) { memcpy(md5_checksum, md5, 16); }

    int ComputeChecksum(const uint8_t* in, const uint32_t l_in) {
        Digest::GenerateMd5(in, l_in, md5_checksum);
        return(1);
    }

    // Todo:
    // int Append()

    PIL_COMPRESSION_TYPE ctype;
    int64_t u_sz, c_sz; // uncompressed/compressed size of the referred columnstore
    uint8_t md5_checksum[16]; // checksum for buffer
    int64_t n_tuples; // for serialization only
    std::vector< std::unique_ptr<TransformMetaTuple> > tuples;
};

}



#endif /* TRANSFORM_META_H_ */
