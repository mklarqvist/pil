#include "../transform/compressor.h"

#include <iostream>

#include "../transform/variant_digest_manager.h"

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
        case(PIL_COMPRESS_ZSTD):     ret2 = CompressAuto(cset, field); break;
        case(PIL_ENCODE_DICT):       ret2 = DictionaryEncode(cset, field); break;
        case(PIL_ENCODE_DELTA):      ret2 = DeltaEncode(cset, field); break;
        case(PIL_COMPRESS_RC_QUAL):  ret2 = static_cast<QualityCompressor*>(this)->Compress(cset, field.cstore); break;
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
       //std::cerr << "COMPRESS: ZSTD@columnar: n=" << cset->size() << std::endl;
       for(int i = 0; i < cset->size(); ++i) {
           // try dictionary encoding
          int has_dict = DictionaryEncode(cset->columns[i], field);
          if(has_dict) {
            int ret_dict = static_cast<ZstdCompressor*>(this)->Compress(
                            cset->columns[i]->dictionary->mutable_data(),
                            cset->columns[i]->dictionary->GetUncompressedSize(),
                            PIL_ZSTD_DEFAULT_LEVEL);

            cset->columns[i]->dictionary->UnsafeSetCompressedSize(ret_dict);
            std::cerr << "DICT ZSTD: " << cset->columns[i]->dictionary->GetUncompressedSize() << "->" << cset->columns[i]->dictionary->GetCompressedSize() << " (" <<
                 (float)cset->columns[i]->dictionary->GetUncompressedSize()/cset->columns[i]->dictionary->GetCompressedSize() << "-fold)" << std::endl;

            ret += ret_dict;
        }

           // Not every ColumnStore has a Nullity bitmap. For example, Dictionary
           // columns.
           if(cset->columns[i]->nullity.get() != nullptr) {
               const uint32_t n_nullity = std::ceil((float)cset->columns[i]->n_records / 32);
               int retNull = static_cast<ZstdCompressor*>(this)->Compress(
                                  cset->columns[i]->nullity->mutable_data(),
                                  n_nullity,
                                  PIL_ZSTD_DEFAULT_LEVEL);

               cset->columns[i]->nullity_u = n_nullity;
               cset->columns[i]->nullity_c = retNull;
               ret += retNull;
               memcpy(cset->columns[i]->nullity->mutable_data(), buffer->mutable_data(), retNull);
               //std::cerr << "nullity-zstd: " << n_nullity << "->" << retNull << " (" << (float)n_nullity/retNull << "-fold)" << std::endl;
           }

           int64_t n_in = cset->columns[i]->buffer.length();
           int ret2 = static_cast<ZstdCompressor*>(this)->Compress(
                   cset->columns[i]->buffer.mutable_data(),
                   cset->columns[i]->buffer.length(),
                   PIL_ZSTD_DEFAULT_LEVEL);

           //std::cerr << "data-zstd: " << cset->columns[i]->buffer.length() << "->" << ret2 << " (" << (float)cset->columns[i]->buffer.length()/ret2 << "-fold)" << std::endl;

           cset->columns[i]->compressed_size = ret2;
           cset->columns[i]->buffer.UnsafeSetLength(ret2);
           memcpy(cset->columns[i]->buffer.mutable_data(), buffer->mutable_data(), ret2);
           cset->columns[i]->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_COMPRESS_ZSTD,n_in,ret2));
           ret += ret2;

       }
       return(ret);
    } else if(field.cstore == PIL_CSTORE_TENSOR) {
        int has_dict = DictionaryEncode(cset->columns[1], cset->columns[0], field);
        if(has_dict) {
           int ret_dict = static_cast<ZstdCompressor*>(this)->Compress(
                              cset->columns[1]->dictionary->mutable_data(),
                              cset->columns[1]->dictionary->GetUncompressedSize(),
                              PIL_ZSTD_DEFAULT_LEVEL);

           ret += ret_dict;
           cset->columns[1]->dictionary->UnsafeSetCompressedSize(ret_dict);

           int ret_dict_stride = static_cast<ZstdCompressor*>(this)->Compress(
                                         cset->columns[1]->dictionary->mutable_length_data(),
                                         cset->columns[1]->dictionary->GetUncompressedLengthSize(),
                                         PIL_ZSTD_DEFAULT_LEVEL);

          ret += ret_dict_stride;
          cset->columns[1]->dictionary->UnsafeSetCompressedLengthSize(ret_dict_stride);

           std::cerr << "DICT ZSTD: " << cset->columns[1]->dictionary->GetUncompressedSize() << "->" << cset->columns[1]->dictionary->GetCompressedSize() << " (" <<
                   (float)cset->columns[1]->dictionary->GetUncompressedSize()/cset->columns[1]->dictionary->GetCompressedSize() << "-fold)" << std::endl;
           std::cerr << "DICT ZSTD: " << cset->columns[1]->dictionary->GetUncompressedLengthSize() << "->" << cset->columns[1]->dictionary->GetCompressedLengthSize() << " (" <<
                          (float)cset->columns[1]->dictionary->GetUncompressedLengthSize()/cset->columns[1]->dictionary->GetCompressedLengthSize() << "-fold)" << std::endl;
       }

       //std::cerr << "computing deltas in place" << std::endl;
        DeltaEncode(cset->columns[0]);

       int64_t n_in = cset->columns[0]->buffer.length();
       int ret1 = static_cast<ZstdCompressor*>(this)->Compress(
                               cset->columns[0]->buffer.mutable_data(),
                               cset->columns[0]->buffer.length(),
                               PIL_ZSTD_DEFAULT_LEVEL);

       memcpy(cset->columns[0]->buffer.mutable_data(), buffer->mutable_data(), ret1);
       cset->columns[0]->compressed_size = ret1;
       cset->columns[0]->buffer.UnsafeSetLength(ret1);
       cset->columns[0]->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_COMPRESS_ZSTD,n_in,ret1));

       const uint32_t n_nullity = std::ceil((float)cset->columns[0]->n_records / 32);
       int retNull = static_cast<ZstdCompressor*>(this)->Compress(
                                     cset->columns[0]->nullity->mutable_data(),
                                     n_nullity,
                                     PIL_ZSTD_DEFAULT_LEVEL);

      cset->columns[0]->nullity_u = n_nullity;
      cset->columns[0]->nullity_c = retNull;
      memcpy(cset->columns[0]->nullity->mutable_data(), buffer->mutable_data(), retNull);
      ret += retNull;
      //std::cerr << "nullity-zstd: " << n_nullity << "->" << retNull << " (" << (float)n_nullity/retNull << "-fold)" << std::endl;

       //std::cerr << "delta-zstd: " << cset->columns[0]->buffer.length() << "->" << ret1 << " (" << (float)cset->columns[0]->buffer.length()/ret1 << "-fold)" << std::endl;

      // Attempt Dictionary encoder for Data

       int64_t n_in1 = cset->columns[1]->buffer.length();
       int ret2 = static_cast<ZstdCompressor*>(this)->Compress(
                                      cset->columns[1]->buffer.mutable_data(),
                                      cset->columns[1]->buffer.length(),
                                      PIL_ZSTD_DEFAULT_LEVEL);

       //std::cerr << "data-zstd: " << cset->columns[1]->buffer.length() << "->" << ret2 << " (" << (float)cset->columns[1]->buffer.length()/ret2 << "-fold)" << std::endl;
       memcpy(cset->columns[1]->buffer.mutable_data(), buffer->mutable_data(), ret2);
       cset->columns[1]->compressed_size = ret2;
       cset->columns[1]->buffer.UnsafeSetLength(ret2);
       cset->columns[1]->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_COMPRESS_ZSTD,n_in1,ret2));

       ret += ret1 + ret2 + retNull;
       return(ret);
    } else {
       //std::cerr << "unknown cstore type" << std::endl;
       return(-1);
    }

    return(ret);
}

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
