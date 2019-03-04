#ifndef FQZCOMP_QUAL_H_
#define FQZCOMP_QUAL_H_

#include "../column_store.h"

namespace pil {

class FqzCompQual {
public:
    int Compress(int vers, int level, std::shared_ptr<ColumnSet> quals, std::shared_ptr<ResizableBuffer> out, size_t& out_size);
    int Decompress(std::shared_ptr<ColumnSet> quals, std::shared_ptr<ResizableBuffer> out, size_t& out_size);
};

}

#endif /* FQZCOMP_QUAL_H_ */
