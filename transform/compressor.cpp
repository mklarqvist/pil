#include "../transform/compressor.h"

#include <iostream>

#include "../transform/variant_digest_manager.h"

namespace pil {

int CompressCigar(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    if(cset.get() == nullptr) return(-1);

   //std::cerr << "auto compression ---->" << std::endl;
   int ret = 0;
   if(field.cstore == PIL_CSTORE_COLUMN) {
       std::cerr << "illegal cigar type" << std::endl;
       exit(1);
   } else if(field.cstore == PIL_CSTORE_TENSOR) {

       uint8_t* data = cset->columns[1]->mutable_data();
       uint32_t* offsets = reinterpret_cast<uint32_t*>(cset->columns[0]->mutable_data());

       // Diff is the individual length
       uint32_t diff = offsets[1] - offsets[0];
       assert(diff % 2 == 0);
       for(int i = 0; i < diff; i += 2) {

       }

       for(int i = 1; i < cset->columns[1]->size(); ++i) {

           diff = offsets[i] - offsets[i - 1];
       }


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
