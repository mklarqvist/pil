#include <iostream>

#include "compressor.h"
#include "variant_digest_manager.h"

namespace pil {

int Compressor::Compress(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    // todo: fix transforms
    if(field.transforms.size() == 0) {
        std::cerr << "auto-compression" << std::endl;
        return(CompressAuto(cset, field));
    }
    std::cerr << "first compressor=" << field.transforms.front() << std::endl;

    switch(field.transforms.front()) {
    case(PIL_COMPRESS_AUTO): return(CompressAuto(cset, field)); break;
    case(PIL_COMPRESS_RC_QUAL): return(static_cast<QualityCompressor*>(this)->Compress(cset, field.cstore)); break;
    case(PIL_COMPRESS_RC_BASES): return(static_cast<SequenceCompressor*>(this)->Compress(cset, field.cstore)); break;
    default: std::cerr << "unknown ctype: " << (int)field.transforms.front() << std::endl; return -1; break;
    }
}

int Compressor::CompressAuto(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    if(cset.get() == nullptr) return(-1);

    std::cerr << "auto compression ---->" << std::endl;
    int ret = 0;
    if(field.cstore == PIL_CSTORE_COLUMN) {
       std::cerr << "in cstore col: n=" << cset->size() << std::endl;
       for(int i = 0; i < cset->size(); ++i) {
           ret += static_cast<ZstdCompressor*>(this)->Compress(
                   cset->columns[i]->buffer->mutable_data(),
                   cset->columns[i]->uncompressed_size,
                   6);

       }
       return(ret);
    } else if(field.cstore == PIL_CSTORE_TENSOR) {
       std::cerr << "computing deltas in place" << std::endl;
       compute_deltas_inplace(reinterpret_cast<uint32_t*>(cset->columns[0]->buffer->mutable_data()),
                              cset->columns[0]->n,
                              0);

       int ret1 = static_cast<ZstdCompressor*>(this)->Compress(
                               cset->columns[0]->buffer->mutable_data(),
                               cset->columns[0]->uncompressed_size,
                               6);
       std::cerr << "delta-zstd: " << cset->columns[0]->uncompressed_size << "->" << ret1 << " (" << (float)cset->columns[0]->uncompressed_size/ret1 << "-fold)" << std::endl;

       int ret2 = static_cast<ZstdCompressor*>(this)->Compress(
                                      cset->columns[1]->buffer->mutable_data(),
                                      cset->columns[1]->uncompressed_size,
                                      6);
       std::cerr << "data-zstd: " << cset->columns[1]->uncompressed_size << "->" << ret2 << " (" << (float)cset->columns[1]->uncompressed_size/ret2 << "-fold)" << std::endl;

       ret += ret1 + ret2;
       return(ret);
    } else {
       //std::cerr << "unknown cstore type" << std::endl;
       return(-1);
    }

    return(ret);
}

/*
bool ZstdCompressor::Decompress(const yon_buffer_t& src, yon_buffer_t& dst){
	const size_t ret = ZSTD_decompress(
							   dst.data(),
							   dst.capacity(),
							   src.data(),
							   src.size());

	//std::cerr << utility::timestamp("LOG","COMPRESSION") << "Input: " << src.size() << " and output: " << ret << " -> " << (float)ret/src.size() << "-fold"  << std::endl;

	if(ZSTD_isError(ret)){
		std::cerr << utility::timestamp("ERROR","ZSTD") << ZSTD_getErrorString(ZSTD_getErrorCode(ret)) << std::endl;
		return(false);
	}

	dst.n_chars_ = ret;

	return true;
}
*/

}
