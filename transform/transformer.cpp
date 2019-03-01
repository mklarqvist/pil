#include "transformer.h"
#include "dictionary_builder.h"
#include "encoder.h"
#include "fastdelta.h"

namespace pil {

int Transformer::DictionaryEncode(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    if(cset.get() == nullptr) return(-1);

    DictionaryBuilder dict;

    std::cerr << "in dictionaary encode for set" << std::endl;

    int ret_status = -1;
    switch(field.ptype) {
    case(PIL_TYPE_INT8):   ret_status = static_cast<NumericDictionaryBuilder<int8_t>*>(&dict)->Encode(cset, field);  break;
    case(PIL_TYPE_INT16):  ret_status = static_cast<NumericDictionaryBuilder<int16_t>*>(&dict)->Encode(cset, field); break;
    case(PIL_TYPE_INT32):  ret_status = static_cast<NumericDictionaryBuilder<int32_t>*>(&dict)->Encode(cset, field);break;
    case(PIL_TYPE_INT64):  ret_status = static_cast<NumericDictionaryBuilder<int64_t>*>(&dict)->Encode(cset, field); break;
    case(PIL_TYPE_UINT8):  ret_status = static_cast<NumericDictionaryBuilder<uint8_t>*>(&dict)->Encode(cset, field);   break;
    case(PIL_TYPE_UINT16): ret_status = static_cast<NumericDictionaryBuilder<uint16_t>*>(&dict)->Encode(cset, field);  break;
    case(PIL_TYPE_UINT32): ret_status = static_cast<NumericDictionaryBuilder<uint32_t>*>(&dict)->Encode(cset, field);  break;
    case(PIL_TYPE_UINT64): ret_status = static_cast<NumericDictionaryBuilder<uint64_t>*>(&dict)->Encode(cset, field);  break;
    case(PIL_TYPE_FLOAT):  ret_status = static_cast<NumericDictionaryBuilder<float>*>(&dict)->Encode(cset, field);    break;
    case(PIL_TYPE_DOUBLE): ret_status = static_cast<NumericDictionaryBuilder<double>*>(&dict)->Encode(cset, field);   break;
    }
    std::cerr << "ret_status=" << ret_status << std::endl;


    return(1);
}

int Transformer::DictionaryEncode(std::shared_ptr<ColumnStore> cstore, const DictionaryFieldType& field) {
    if(cstore.get() == nullptr) return(-1);

    if(cstore->dictionary.get() == nullptr)
        cstore->dictionary = std::make_shared<ColumnDictionary>();

    std::shared_ptr<DictionaryBuilder> dict = std::static_pointer_cast<DictionaryBuilder>(cstore->dictionary);

    std::cerr << "in dictionaary encode for store" << std::endl;

    int ret_status = -1;
    switch(field.ptype) {
    case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int8_t> >(dict)->Encode(cstore);  break;
    case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int16_t> >(dict)->Encode(cstore); break;
    case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int32_t> >(dict)->Encode(cstore);break;
    case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int64_t> >(dict)->Encode(cstore); break;
    case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint8_t> >(dict)->Encode(cstore);   break;
    case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint16_t> >(dict)->Encode(cstore);  break;
    case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint32_t> >(dict)->Encode(cstore);  break;
    case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint64_t> >(dict)->Encode(cstore);  break;
    case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<float> >(dict)->Encode(cstore);    break;
    case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<double> >(dict)->Encode(cstore);   break;
    }
    std::cerr << "ret_status=" << ret_status << std::endl;

    if(ret_status < 1) cstore->dictionary = nullptr;


    return(ret_status);
}

int Transformer::DictionaryEncode(std::shared_ptr<ColumnStore> cstore, std::shared_ptr<ColumnStore> strides, const DictionaryFieldType& field) {
    if(cstore.get() == nullptr) return(-1);

    if(cstore->dictionary.get() == nullptr)
        cstore->dictionary = std::make_shared<ColumnDictionary>();

    std::shared_ptr<DictionaryBuilder> dict = std::static_pointer_cast<DictionaryBuilder>(cstore->dictionary);

    std::cerr << "in dictionaary encode for store" << std::endl;

    int ret_status = -1;
    switch(field.ptype) {
    case(PIL_TYPE_INT8):   ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int8_t> >(dict)->Encode(cstore, strides);  break;
    case(PIL_TYPE_INT16):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int16_t> >(dict)->Encode(cstore, strides); break;
    case(PIL_TYPE_INT32):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int32_t> >(dict)->Encode(cstore, strides);break;
    case(PIL_TYPE_INT64):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<int64_t> >(dict)->Encode(cstore, strides); break;
    case(PIL_TYPE_UINT8):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint8_t> >(dict)->Encode(cstore, strides);   break;
    case(PIL_TYPE_UINT16): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint16_t> >(dict)->Encode(cstore, strides);  break;
    case(PIL_TYPE_UINT32): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint32_t> >(dict)->Encode(cstore, strides);  break;
    case(PIL_TYPE_UINT64): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<uint64_t> >(dict)->Encode(cstore, strides);  break;
    case(PIL_TYPE_FLOAT):  ret_status = std::static_pointer_cast< NumericDictionaryBuilder<float> >(dict)->Encode(cstore, strides);    break;
    case(PIL_TYPE_DOUBLE): ret_status = std::static_pointer_cast< NumericDictionaryBuilder<double> >(dict)->Encode(cstore, strides);   break;
    }
    std::cerr << "ret_status=" << ret_status << std::endl;

    if(ret_status < 1) cstore->dictionary = nullptr;

    return(ret_status);
}

// todo: fix
int Transformer::DeltaEncode(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    if(cset.get() == nullptr) return(-1);

    //std::cerr << "IN DELTA ENCODER" << std::endl;

    if(field.cstore == PIL_CSTORE_COLUMN) {
        std::shared_ptr<ColumnStore> tgt = cset->columns[0];

        int ret_status = -1;
        switch(field.ptype) {
        case(PIL_TYPE_INT8):
        case(PIL_TYPE_INT16):
        case(PIL_TYPE_INT32):
        case(PIL_TYPE_INT64):
        case(PIL_TYPE_UINT8):
        case(PIL_TYPE_UINT16):
        case(PIL_TYPE_UINT64):
        case(PIL_TYPE_FLOAT):
        case(PIL_TYPE_DOUBLE): return(-1);
        case(PIL_TYPE_UINT32): compute_deltas_inplace(reinterpret_cast<uint32_t*>(tgt->mutable_data()), tgt->n_records, 0); break;
        }
        tgt->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_ENCODE_DELTA,tgt->buffer.length(),tgt->buffer.length()));

    } else if(field.cstore == PIL_CSTORE_TENSOR) {
        std::cerr << "not allowed yet" << std::endl;
        exit(1);

    } else {
        std::cerr << "delta encode in tensor is not allowed" << std::endl;
        exit(1);
    }

    return(1);
}

int Transformer::DeltaEncode(std::shared_ptr<ColumnStore> cstore) {
    // todo: tests
    compute_deltas_inplace(reinterpret_cast<uint32_t*>(cstore->mutable_data()), cstore->n_records, 0);
    cstore->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_ENCODE_DELTA,cstore->buffer.length(),cstore->buffer.length()));
    return(1);
}

}
