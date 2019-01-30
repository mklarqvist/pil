#ifndef ALGORITHM_COMPRESSION_ZSTD_CODEC_H_
#define ALGORITHM_COMPRESSION_ZSTD_CODEC_H_

#include <cstdint>

#include "zstd.h"
#include "zstd_errors.h"

namespace pil {

class ZSTDCodec {
private:
	typedef        ZSTDCodec   self_type;
	typedef struct ZSTD_CCtx_s ZSTD_CCtx;
	typedef struct ZSTD_DCtx_s ZSTD_DCtx;

public:
	ZSTDCodec();
	~ZSTDCodec();

	void SetLevel(const int32_t& c){ this->compression_level_data = c; }

	int Compress(const uint8_t* src, const uint32_t n_src, uint8_t* dst, uint32_t n_dst, const int compression_level);
	//bool Decompress(const yon_buffer_t& src, yon_buffer_t& dst);

private:
	int32_t compression_level_data;
	int32_t compression_level_strides;
	ZSTD_CCtx* compression_context_; // recycle contexts
	ZSTD_DCtx* decompression_context_; // recycle contexts
};


}



#endif /* ALGORITHM_COMPRESSION_ZSTD_CODEC_H_ */
