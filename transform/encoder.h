#ifndef ENCODER_H_
#define ENCODER_H_

#include <cassert>
#include <cmath>
#include <unordered_map>

#include "../pil.h"
#include "transformer.h"
#include "../buffer.h"

namespace pil {

// A: 65, a: 97 -> 0
// C: 67, c: 99 -> 1
// G: 71, g: 103 -> 2
// T: 84, t: 116 -> 3
// N: 78, n: 110 -> 4
static const uint8_t BaseBitTable[256] =
{
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

// base encoder
class Encoder : public Transformer {
public:
    Encoder(){}
    Encoder(std::shared_ptr<ResizableBuffer> data) : Transformer(data){}

    inline std::shared_ptr<ResizableBuffer> data() const { return(buffer); }
};

class BaseBitEncoder : public Encoder {
public:
    int Encode(const uint8_t* in, const uint32_t n_in) {
        int n_bytes = std::ceil((float)n_in / 4); // 2 bits per symbol

        if(buffer.get() == nullptr) {
            assert(AllocateResizableBuffer(pool_, n_bytes + 64, &buffer) == 1);
        }

        if(buffer->capacity() < n_in + 64) {
            assert(buffer->Reserve(n_in + 64) == 1);
        }

        memset(buffer->mutable_data(), 0, buffer->capacity()); // zero out buffer
        uint8_t* out = buffer->mutable_data();

        uint32_t offset = 0; // offset into in byte stream
        assert(n_bytes > 1);
        for(int i = 0; i < n_bytes - 1; ++i) {
            out[i] = 0;
            //std::cerr << "b=" << std::bitset<8>(out[i]) << std::endl;
            for(int j = 0; j < 4; ++j, ++offset) {
                //std::cerr << "in=" << in[offset] << " -> " << (int)BaseBitTable[in[offset]] << " @ " << 2*j << std::endl;
                out[i] |= (BaseBitTable[in[offset]] << 2*j);
            }
            //std::cerr << "a=" << std::bitset<8>(out[i]) << std::endl;
        }

        uint32_t residual = n_in % 4;
        //std::cerr << "residual=" << residual << std::endl;
        for(uint32_t j = 0; j < residual; ++j, ++offset) {
            out[n_bytes - 1] |= BaseBitTable[in[offset]] << j;
        }

        //std::cerr << "new packed=" << n_in << "->" << n_bytes << std::endl;

        return(n_bytes);
    }

};

}



#endif /* ENCODER_H_ */
