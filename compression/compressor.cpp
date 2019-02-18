#include <iostream>

#include "compressor.h"
#include "variant_digest_manager.h"

namespace pil {

int Compressor::Compress(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    // todo: fix transforms
    if(field.transforms.size() == 0) {
        //std::cerr << "auto-compression" << std::endl;
        return(CompressAuto(cset, field));
    }
    // Todo: fix
    //std::cerr << "first compressor=" << field.transforms.front() << std::endl;

    int ret = 0;
    for(int i = 0; i < field.transforms.size(); ++i) {
        int ret2 = -1;
        switch(field.transforms[i]) {
        case(PIL_COMPRESS_AUTO):
        case(PIL_COMPRESS_ZSTD): ret2 = CompressAuto(cset, field); break;
        case(PIL_ENCODE_DICT):   ret2 = DictionaryEncode(cset, field); break;
        case(PIL_ENCODE_DELTA):  ret2 = DeltaEncode(cset, field); break;
        case(PIL_COMPRESS_RC_QUAL): ret2 = static_cast<QualityCompressor*>(this)->Compress(cset, field.cstore); break;
        case(PIL_COMPRESS_RC_BASES): ret2 = static_cast<SequenceCompressor*>(this)->Compress(cset, field.cstore); break;
        default: std::cerr << "unknown ctype: " << (int)field.transforms.front() << std::endl; break;
        }
        if(ret2 != -1) ret += ret2;
    }
    return(ret);
}

int Compressor::CompressAuto(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    if(cset.get() == nullptr) return(-1);

    //std::cerr << "auto compression ---->" << std::endl;
    int ret = 0;
    if(field.cstore == PIL_CSTORE_COLUMN) {
       std::cerr << "COMPRESS: ZSTD@columnar: n=" << cset->size() << std::endl;
       for(int i = 0; i < cset->size(); ++i) {
           // Not every ColumnStore has a Nullity bitmap. For example, Dictionary
           // columns.
           if(cset->columns[i]->nullity.get() != nullptr) {
               const uint32_t n_nullity = std::ceil((float)cset->columns[i]->n / 32);

               int retNull = static_cast<ZstdCompressor*>(this)->Compress(
                                  cset->columns[i]->nullity->mutable_data(),
                                  n_nullity,
                                  PIL_ZSTD_DEFAULT_LEVEL);

               cset->columns[i]->nullity_u = n_nullity;
               cset->columns[i]->nullity_c = retNull;
               ret += retNull;
               memcpy(cset->columns[i]->nullity->mutable_data(), buffer->mutable_data(), retNull);
               std::cerr << "nullity-zstd: " << n_nullity << "->" << retNull << " (" << (float)n_nullity/retNull << "-fold)" << std::endl;
           }

           int ret2 = static_cast<ZstdCompressor*>(this)->Compress(
                   cset->columns[i]->buffer->mutable_data(),
                   cset->columns[i]->uncompressed_size,
                   PIL_ZSTD_DEFAULT_LEVEL);

           std::cerr << "data-zstd: " << cset->columns[i]->uncompressed_size << "->" << ret2 << " (" << (float)cset->columns[i]->uncompressed_size/ret2 << "-fold)" << std::endl;

           cset->columns[i]->compressed_size = ret2;
           memcpy(cset->columns[i]->buffer->mutable_data(), buffer->mutable_data(), ret2);
           cset->columns[i]->transformations.push_back(PIL_COMPRESS_ZSTD);
           ret += ret2;

       }
       return(ret);
    } else if(field.cstore == PIL_CSTORE_TENSOR) {
       std::cerr << "computing deltas in place" << std::endl;
       compute_deltas_inplace(reinterpret_cast<uint32_t*>(cset->columns[0]->buffer->mutable_data()),
                              cset->columns[0]->n,
                              0);
       cset->columns[0]->transformations.push_back(PIL_ENCODE_DELTA);

       int ret1 = static_cast<ZstdCompressor*>(this)->Compress(
                               cset->columns[0]->buffer->mutable_data(),
                               cset->columns[0]->uncompressed_size,
                               PIL_ZSTD_DEFAULT_LEVEL);
       memcpy(cset->columns[0]->buffer->mutable_data(), buffer->mutable_data(), ret1);
       cset->columns[0]->compressed_size = ret1;
       cset->columns[0]->transformations.push_back(PIL_COMPRESS_ZSTD);

       const uint32_t n_nullity = std::ceil((float)cset->columns[0]->n / 32);
       int retNull = static_cast<ZstdCompressor*>(this)->Compress(
                                     cset->columns[0]->nullity->mutable_data(),
                                     n_nullity,
                                     PIL_ZSTD_DEFAULT_LEVEL);

      cset->columns[0]->nullity_u = n_nullity;
      cset->columns[0]->nullity_c = retNull;
      memcpy(cset->columns[0]->nullity->mutable_data(), buffer->mutable_data(), retNull);
      ret += retNull;
      std::cerr << "nullity-zstd: " << n_nullity << "->" << retNull << " (" << (float)n_nullity/retNull << "-fold)" << std::endl;


       std::cerr << "delta-zstd: " << cset->columns[0]->uncompressed_size << "->" << ret1 << " (" << (float)cset->columns[0]->uncompressed_size/ret1 << "-fold)" << std::endl;

       int ret2 = static_cast<ZstdCompressor*>(this)->Compress(
                                      cset->columns[1]->buffer->mutable_data(),
                                      cset->columns[1]->uncompressed_size,
                                      PIL_ZSTD_DEFAULT_LEVEL);
       std::cerr << "data-zstd: " << cset->columns[1]->uncompressed_size << "->" << ret2 << " (" << (float)cset->columns[1]->uncompressed_size/ret2 << "-fold)" << std::endl;
       memcpy(cset->columns[1]->buffer->mutable_data(), buffer->mutable_data(), ret2);
       cset->columns[1]->compressed_size = ret2;
       cset->columns[1]->transformations.push_back(PIL_COMPRESS_ZSTD);

       ret += ret1 + ret2 + retNull;
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
