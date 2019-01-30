#include <iostream>

#include "zstd_codec.h"
#include "variant_digest_manager.h"

namespace pil {

ZSTDCodec::ZSTDCodec() :
	compression_level_data(0),
	compression_level_strides(0),
	compression_context_(ZSTD_createCCtx()),
	decompression_context_(ZSTD_createDCtx())
{

}

ZSTDCodec::~ZSTDCodec(){
	ZSTD_freeCCtx(this->compression_context_);
	ZSTD_freeDCtx(this->decompression_context_);
}

int ZSTDCodec::Compress(const uint8_t* src, const uint32_t n_src, uint8_t* dst, uint32_t n_dst, const int compression_level){

    const int32_t ret = ZSTD_compress(dst, n_dst, src, n_src, compression_level);
    if(ZSTD_isError(ret)){
        std::cerr << "zstd error: " << ZSTD_getErrorString(ZSTD_getErrorCode(ret)) << std::endl;
        return(-1);
    }

    return(1);
}

/*
bool ZSTDCodec::Decompress(const yon_buffer_t& src, yon_buffer_t& dst){
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
