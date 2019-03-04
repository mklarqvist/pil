#ifndef FQZCOMP_QUAL_H_
#define FQZCOMP_QUAL_H_

#include "../column_store.h"

namespace pil {

class FqzCompQual {
public:
    uint8_t* Compress(int vers, int level, std::shared_ptr<ColumnSet> quals, size_t& out_size);
    uint8_t* Decompress(uint8_t *in, size_t in_size, size_t& out_size);
};

}

#endif /* FQZCOMP_QUAL_H_ */
