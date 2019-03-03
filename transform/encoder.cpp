#include "encoder.h"

namespace pil {

int DeltaEncoder::Encode(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    if(cset.get() == nullptr) return(-1);

    if(field.cstore == PIL_CSTORE_COLUMN) {
        for(int i = 0; i < cset->size(); ++i) {
            std::shared_ptr<ColumnStore> tgt = cset->columns[i];

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
            case(PIL_TYPE_UINT32):
                compute_deltas_inplace(reinterpret_cast<uint32_t*>(tgt->mutable_data()), tgt->n_records, 0);
                ret_status = 1;
                break;
            }
            if(ret_status < 0) return(ret_status);
            tgt->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_ENCODE_DELTA, tgt->buffer.length(), tgt->buffer.length()));
            tgt->transformation_args.back()->ComputeChecksum(tgt->buffer.mutable_data(), tgt->buffer.length());
        }

    } else if(field.cstore == PIL_CSTORE_TENSOR) {
        if(cset->size() != 2) return(-4);
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
        case(PIL_TYPE_UINT32):
            compute_deltas_inplace(reinterpret_cast<uint32_t*>(tgt->mutable_data()), tgt->n_records, 0);
            ret_status = 1;
            break;
        }
        if(ret_status < 0) return(ret_status);
        tgt->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_ENCODE_DELTA, tgt->buffer.length(), tgt->buffer.length()));
        tgt->transformation_args.back()->ComputeChecksum(tgt->buffer.mutable_data(), tgt->buffer.length());

    } else {
        std::cerr << "unknown storage model" << std::endl;
        exit(1);
    }

    return(1);
}

int DeltaEncoder::UnsafeEncode(std::shared_ptr<ColumnStore> cstore) {
    if(cstore.get() == nullptr) return(-4);
    if(cstore->buffer.length() % sizeof(uint32_t) != 0) return(-5);
    if(cstore->n_records * sizeof(uint32_t) != cstore->buffer.length()) return(-5);

    compute_deltas_inplace(reinterpret_cast<uint32_t*>(cstore->mutable_data()), cstore->n_records, 0);
    cstore->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_ENCODE_DELTA, cstore->buffer.length(), cstore->buffer.length()));
    cstore->transformation_args.back()->ComputeChecksum(cstore->buffer.mutable_data(), cstore->buffer.length());

    return(1);
}

int DeltaEncoder::PrefixSum(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
    if(cset.get() == nullptr) return(-1);

    if(field.cstore == PIL_CSTORE_COLUMN) {
        for(int i = 0; i < cset->size(); ++i) {
            std::shared_ptr<ColumnStore> tgt = cset->columns[i];

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
            case(PIL_TYPE_UINT32):
                compute_prefix_sum_inplace(reinterpret_cast<uint32_t*>(tgt->mutable_data()), tgt->n_records, 0);
                ret_status = 1;
                break;
            }
            if(ret_status < 0) return(ret_status);
        }

    } else if(field.cstore == PIL_CSTORE_TENSOR) {
        if(cset->size() != 2) return(-4);
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
        case(PIL_TYPE_UINT32):
        compute_prefix_sum_inplace(reinterpret_cast<uint32_t*>(tgt->mutable_data()), tgt->n_records, 0);
            ret_status = 1;
            break;
        }
        if(ret_status < 0) return(ret_status);

    } else {
        std::cerr << "unknown storage model" << std::endl;
        exit(1);
    }

    return(1);
}

int DeltaEncoder::UnsafePrefixSum(std::shared_ptr<ColumnStore> cstore, const DictionaryFieldType& field) {
    if(cstore.get() == nullptr) return(-4);
    if(cstore->buffer.length() % sizeof(uint32_t) != 0) return(-5);
    if(cstore->n_records * sizeof(uint32_t) != cstore->buffer.length()) return(-5);

    compute_prefix_sum_inplace(reinterpret_cast<uint32_t*>(cstore->mutable_data()), cstore->n_records, 0);

    return(1);
}

}
