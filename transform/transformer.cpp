#include "transformer.h"
#include "dictionary_builder.h"
#include "encoder.h"
#include "fastdelta.h"
#include "compressor.h"

// Codecs
#include "zstd.h"
#include "zstd_errors.h"
#include "transformer.h"
#include "base_model.h"
#include "frequency_model.h"

namespace pil {

int Transformer::Transform(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    if(Transformer::ValidTransformationOrder(field.transforms) == false) return(-1);

    // If there is no supplied transformation series then apply the default
    // transformations (auto mode).
    if(field.transforms.size() == 0)
        return(AutoTransform(cset, field));

    // Apply transformations
    int ret = -1;
    int ret_total = 0;
    for(size_t i = 0; i < field.transforms.size(); ++i) {
        switch(field.transforms[i]) {
        case(PIL_COMPRESS_AUTO): ret = AutoTransform(cset, field); break;
        case(PIL_COMPRESS_ZSTD): break;
        case(PIL_COMPRESS_NONE): break;
        case(PIL_COMPRESS_RC_QUAL): ret = static_cast<QualityCompressor*>(this)->Compress(cset, field.cstore); break;
        case(PIL_COMPRESS_RC_BASES): ret = static_cast<SequenceCompressor*>(this)->Compress(cset, field.cstore); break;
        case(PIL_COMPRESS_RC_ILLUMINA_NAME): break;
        case(PIL_ENCODE_DICT): ret = DictionaryEncode(cset, field); break;
        case(PIL_ENCODE_DELTA): ret = static_cast<DeltaEncoder*>(this)->Encode(cset, field); break;
        case(PIL_ENCODE_DELTA_DELTA): break;
        case(PIL_ENCODE_BASES_2BIT): break;
        default: return(-2);
        }
        if(ret < 1) return(ret);
        ret_total += ret;
    }

    // Check if tensor compression is ok
    if(field.cstore == PIL_CSTORE_TENSOR) {
        if(cset->columns[0]->transformation_args.size() == 0) {
            std::cerr << ">>>>>>>>>>>>RESIDUAL COMPRESSION" << std::endl;

            // Compute the delta of the cumulative sums of strides (default storage).
            static_cast<DeltaEncoder*>(this)->UnsafeEncode(cset->columns[0]);

            // Compress the strides with ZSTD.
            int64_t n_in = cset->columns[0]->buffer.length();
            int ret1 = static_cast<ZstdCompressor*>(this)->Compress(
                                   cset->columns[0]->buffer.mutable_data(),
                                   cset->columns[0]->buffer.length(),
                                   PIL_ZSTD_DEFAULT_LEVEL);

            if(ret1 < 0) return(-6); // compression failure

            memcpy(cset->columns[0]->buffer.mutable_data(), buffer->mutable_data(), ret1);
            cset->columns[0]->compressed_size = ret1;
            cset->columns[0]->buffer.UnsafeSetLength(ret1);
            cset->columns[0]->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_COMPRESS_ZSTD,n_in,ret1));
            cset->columns[0]->transformation_args.back()->ComputeChecksum(cset->columns[0]->buffer.mutable_data(), ret1);
            ret_total += ret1;

            // Compress the Nullity bitmap
            if(cset->columns[0]->nullity.get() == nullptr) return(-5); // malformed data

            const uint32_t n_nullity = std::ceil((float)cset->columns[0]->n_records / 32);
            int retNull = static_cast<ZstdCompressor*>(this)->Compress(
                                         cset->columns[0]->nullity->mutable_data(),
                                         n_nullity,
                                         PIL_ZSTD_DEFAULT_LEVEL);

            if(retNull < 0) return(-6); // compression failure
            cset->columns[0]->nullity_u = n_nullity;
            cset->columns[0]->nullity_c = retNull;
            memcpy(cset->columns[0]->nullity->mutable_data(), buffer->mutable_data(), retNull);
            ret_total += retNull;
        }

    }

    return(ret_total);
}

int Transformer::AutoTransform(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    if(cset.get() == nullptr) return(-1);

    if(field.cstore == PIL_CSTORE_COLUMN) return(AutoTransformColumns(cset, field));
    else if(field.cstore == PIL_CSTORE_TENSOR) return(AutoTransformTensor(cset, field));
    else return(-2);
}

int Transformer::AutoTransformColumns(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    if(cset.get() == nullptr) return(-1);
    if(cset->size() == 0) return(1);

    int ret = -1;
    for(int i = 0; i < cset->size(); ++i) {
        if(cset->columns[i].get() == nullptr) return(-4);
        int ret_i = AutoTransformColumn(cset->columns[i], field);
        if(ret_i < 0) return(-3);
        ret += ret_i;
    }

    return(ret);
}

int Transformer::AutoTransformColumn(std::shared_ptr<ColumnStore> cstore, const DictionaryFieldType& field) {
    if(cstore.get() == nullptr) return(-4);
    if(cstore->n_records == 0) return(1);

    int ret = 0;

    // Attempt to Dictionary encode data. If successful then compress the
    // dictionary with ZSTD.
    ColumnDictionary dict;
    int has_dict = DictionaryEncode(cstore, field);
    if(has_dict) {
        int ret_dict = static_cast<ZstdCompressor*>(this)->Compress(
                        cstore->dictionary->mutable_data(),
                        cstore->dictionary->GetUncompressedSize(),
                        PIL_ZSTD_DEFAULT_LEVEL);

        cstore->dictionary->UnsafeSetCompressedSize(ret_dict);
        std::cerr << "DICT ZSTD: " << cstore->dictionary->GetUncompressedSize() << "->" << cstore->dictionary->GetCompressedSize() << " (" <<
             (float)cstore->dictionary->GetUncompressedSize()/cstore->dictionary->GetCompressedSize() << "-fold)" << std::endl;

        ret += ret_dict;
    }

    // Compress the Nullity bitmap.
    // Not every ColumnStore has a Nullity bitmap. For example, Dictionary
    // columns.
    if(cstore->nullity.get() != nullptr) {
       const uint32_t n_nullity = std::ceil((float)cstore->n_records / 32);
       int retNull = static_cast<ZstdCompressor*>(this)->Compress(
                          cstore->nullity->mutable_data(),
                          n_nullity,
                          PIL_ZSTD_DEFAULT_LEVEL);

       cstore->nullity_u = n_nullity;
       cstore->nullity_c = retNull;
       ret += retNull;
       memcpy(cstore->nullity->mutable_data(), buffer->mutable_data(), retNull);
       //std::cerr << "nullity-zstd: " << n_nullity << "->" << retNull << " (" << (float)n_nullity/retNull << "-fold)" << std::endl;
    }

    // Compress the actual data with ZSTD.
    int64_t n_in = cstore->buffer.length();
    int ret2 = static_cast<ZstdCompressor*>(this)->Compress(
           cstore->buffer.mutable_data(),
           cstore->buffer.length(),
           PIL_ZSTD_DEFAULT_LEVEL);

    //std::cerr << "data-zstd: " << cstore->buffer.length() << "->" << ret2 << " (" << (float)cstore->buffer.length()/ret2 << "-fold)" << std::endl;

    cstore->compressed_size = ret2;
    cstore->buffer.UnsafeSetLength(ret2);
    memcpy(cstore->buffer.mutable_data(), buffer->mutable_data(), ret2);
    cstore->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_COMPRESS_ZSTD, n_in, ret2));
    cstore->transformation_args.back()->ComputeChecksum(cstore->buffer.mutable_data(), ret2);
    ret += ret2;

    return(ret);
}

int Transformer::AutoTransformTensor(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    if(cset.get() == nullptr) return(-1);
    if(cset->n != 2) return(-5); // malformed data
    if(cset->columns[0].get() == nullptr) return(-5);
    if(cset->columns[1].get() == nullptr) return(-5);

    int ret = 0;

    // Attempt to dictionary encode the data given the strides.
    // If set then compress the dictionary and strides with ZSTD.
    ColumnDictionary dict;
    int has_dict = DictionaryEncode(cset, field);
    if(has_dict) {
        int ret_dict = static_cast<ZstdCompressor*>(this)->Compress(
                          cset->columns[1]->dictionary->mutable_data(),
                          cset->columns[1]->dictionary->GetUncompressedSize(),
                          PIL_ZSTD_DEFAULT_LEVEL);

        if(ret_dict < 0) return(-6); // compression failure

        ret += ret_dict;
        cset->columns[1]->dictionary->UnsafeSetCompressedSize(ret_dict);

        int ret_dict_stride = static_cast<ZstdCompressor*>(this)->Compress(
                         cset->columns[1]->dictionary->mutable_length_data(),
                         cset->columns[1]->dictionary->GetUncompressedLengthSize(),
                         PIL_ZSTD_DEFAULT_LEVEL);

        if(ret_dict_stride < 0) return(-6); // compression failure

        ret += ret_dict_stride;
        cset->columns[1]->dictionary->UnsafeSetCompressedLengthSize(ret_dict_stride);

        std::cerr << "DICT ZSTD: " << cset->columns[1]->dictionary->GetUncompressedSize() << "->" << cset->columns[1]->dictionary->GetCompressedSize() << " (" <<
               (float)cset->columns[1]->dictionary->GetUncompressedSize()/cset->columns[1]->dictionary->GetCompressedSize() << "-fold)" << std::endl;
        std::cerr << "DICT ZSTD: " << cset->columns[1]->dictionary->GetUncompressedLengthSize() << "->" << cset->columns[1]->dictionary->GetCompressedLengthSize() << " (" <<
                      (float)cset->columns[1]->dictionary->GetUncompressedLengthSize()/cset->columns[1]->dictionary->GetCompressedLengthSize() << "-fold)" << std::endl;
    }

    // Compute the delta of the cumulative sums of strides (default storage).
    static_cast<DeltaEncoder*>(this)->UnsafeEncode(cset->columns[0]);

    // Compress the strides with ZSTD.
    int64_t n_in = cset->columns[0]->buffer.length();
    int ret1 = static_cast<ZstdCompressor*>(this)->Compress(
                           cset->columns[0]->buffer.mutable_data(),
                           cset->columns[0]->buffer.length(),
                           PIL_ZSTD_DEFAULT_LEVEL);

    if(ret1 < 0) return(-6); // compression failure

    memcpy(cset->columns[0]->buffer.mutable_data(), buffer->mutable_data(), ret1);
    cset->columns[0]->compressed_size = ret1;
    cset->columns[0]->buffer.UnsafeSetLength(ret1);
    cset->columns[0]->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_COMPRESS_ZSTD,n_in,ret1));
    cset->columns[0]->transformation_args.back()->ComputeChecksum(cset->columns[0]->buffer.mutable_data(), ret1);

    // Compress the Nullity bitmap
    if(cset->columns[0]->nullity.get() == nullptr) return(-5); // malformed data

    const uint32_t n_nullity = std::ceil((float)cset->columns[0]->n_records / 32);
    int retNull = static_cast<ZstdCompressor*>(this)->Compress(
                                 cset->columns[0]->nullity->mutable_data(),
                                 n_nullity,
                                 PIL_ZSTD_DEFAULT_LEVEL);

    if(retNull < 0) return(-6); // compression failure
    cset->columns[0]->nullity_u = n_nullity;
    cset->columns[0]->nullity_c = retNull;
    memcpy(cset->columns[0]->nullity->mutable_data(), buffer->mutable_data(), retNull);
    ret += retNull;
    //std::cerr << "nullity-zstd: " << n_nullity << "->" << retNull << " (" << (float)n_nullity/retNull << "-fold)" << std::endl;
    //std::cerr << "delta-zstd: " << cset->columns[0]->buffer.length() << "->" << ret1 << " (" << (float)cset->columns[0]->buffer.length()/ret1 << "-fold)" << std::endl;

    // Compress actual data with ZSTD: the data could possibly be dictionary
    // encoded (see above).
    int64_t n_in1 = cset->columns[1]->buffer.length();
    int ret2 = static_cast<ZstdCompressor*>(this)->Compress(
                                  cset->columns[1]->buffer.mutable_data(),
                                  cset->columns[1]->buffer.length(),
                                  PIL_ZSTD_DEFAULT_LEVEL);

    if(ret2 < 0) return(-6); // compression failure

    //std::cerr << "data-zstd: " << cset->columns[1]->buffer.length() << "->" << ret2 << " (" << (float)cset->columns[1]->buffer.length()/ret2 << "-fold)" << std::endl;
    memcpy(cset->columns[1]->buffer.mutable_data(), buffer->mutable_data(), ret2);
    cset->columns[1]->compressed_size = ret2;
    cset->columns[1]->buffer.UnsafeSetLength(ret2);
    cset->columns[1]->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_COMPRESS_ZSTD,n_in1,ret2));
    cset->columns[1]->transformation_args.back()->ComputeChecksum(cset->columns[1]->buffer.mutable_data(), ret2);

    ret += ret1 + ret2 + retNull;
    return(ret);
}


int Transformer::DictionaryEncode(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field, const bool force) {
   if(cset.get() == nullptr) return(-1);

   std::cerr << "in dictionaary encode for set" << std::endl;

    if(cset->columns[1]->dictionary.get() == nullptr)
        cset->columns[1]->dictionary = std::make_shared<ColumnDictionary>();

    std::shared_ptr<DictionaryBuilder> dict = std::static_pointer_cast<DictionaryBuilder>(cset->columns[1]->dictionary);

   int ret_status = -1;
   switch(field.ptype) {
   case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int8_t> >(dict)->Encode(cset, field, force);  break;
   case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int16_t> >(dict)->Encode(cset, field, force); break;
   case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int32_t> >(dict)->Encode(cset, field, force);break;
   case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int64_t> >(dict)->Encode(cset, field, force); break;
   case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint8_t> >(dict)->Encode(cset, field, force);   break;
   case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint16_t> >(dict)->Encode(cset, field, force);  break;
   case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint32_t> >(dict)->Encode(cset, field, force);  break;
   case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint64_t> >(dict)->Encode(cset, field, force);  break;
   case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<float> >(dict)->Encode(cset, field, force);    break;
   case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<double> >(dict)->Encode(cset, field, force);   break;
   }
   std::cerr << "ret_status=" << ret_status << std::endl;


   return(ret_status);
}

int Transformer::DictionaryEncode(std::shared_ptr<ColumnStore> cstore, const DictionaryFieldType& field, bool force) {
   if(cstore.get() == nullptr) return(-1);

   if(cstore->dictionary.get() == nullptr)
       cstore->dictionary = std::make_shared<ColumnDictionary>();

   std::shared_ptr<DictionaryBuilder> dict = std::static_pointer_cast<DictionaryBuilder>(cstore->dictionary);

   std::cerr << "in dictionaary encode for store" << std::endl;

   int ret_status = -1;
   switch(field.ptype) {
   case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int8_t> >(dict)->Encode(cstore, force);  break;
   case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int16_t> >(dict)->Encode(cstore, force); break;
   case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int32_t> >(dict)->Encode(cstore, force);break;
   case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int64_t> >(dict)->Encode(cstore, force); break;
   case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint8_t> >(dict)->Encode(cstore, force);   break;
   case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint16_t> >(dict)->Encode(cstore, force);  break;
   case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint32_t> >(dict)->Encode(cstore, force);  break;
   case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint64_t> >(dict)->Encode(cstore, force);  break;
   case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<float> >(dict)->Encode(cstore, force);    break;
   case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<double> >(dict)->Encode(cstore, force);   break;
   }
   std::cerr << "ret_status=" << ret_status << std::endl;

   if(ret_status < 1) cstore->dictionary = nullptr;


   return(ret_status);
}


}
