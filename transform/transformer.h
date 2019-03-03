#ifndef TRANSFORMER_H_
#define TRANSFORMER_H_

#include "../pil.h"
#include "../buffer.h"
#include "../column_store.h"
#include "../table_schemas.h"

namespace pil {

class Transformer {
public:
    Transformer() : pool_(default_memory_pool()){}
    Transformer(std::shared_ptr<ResizableBuffer> data) : pool_(default_memory_pool()), buffer(data){}

    /**<
     * Primary entry-point for applying a Transformation series to a ColumnSet.
     * Sequentially apply the Transformation series in-order, if valid.
     * @param cset  Source/destination ColumnSet.
     * @param field DictionaryFieldType describing the column store type and primitive type used.
     * @return      Positive values are a success and negative values are failures.
     */
    int Transform(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field);

    /**<
     * Ascertain that the provided set of transformation parameters are legal.
     * It is disallowed to call Dictionary encoding as a non-final step
     * excluding compression. It is also disallowed to call Dictionary
     * encoding more than once (1).
     *
     * Note that invoking Dictionary encoding in a chain will result in predicate
     * pushdowns can only be performed at that level and NOT on the actual data!
     *
     * Allowed: transform 1, transform 2, dictionary encoding, compression
     * Allowed: transform, dictionary encoding, compression1, compression2
     * Disallowed: transform 1, compression, dictionary encoding
     * Disallowed: transform 1, dictionary encoding, transform 2, compression
     * Disallowed: dictionary encoding, transform 1
     *
     * It is disallowed to mix automatic encoding/compression with other types
     * as this may cause irrevocable problems.
     *
     * @param transforms Input vector of PIL_COMPRESSION_TYPE.
     * @return
     */
    static bool ValidTransformationOrder(const std::vector<PIL_COMPRESSION_TYPE>& transforms) {
        if(transforms.size() == 0) return true;
        if(transforms.size() == 1) return true;

        // Check for the presence of dictionary encoder.
        // If NOT present then return TRUE
        uint32_t n_found = 0, n_pos_last = 0, n_found_auto = 0;
        for(size_t i = 0; i < transforms.size(); ++i) {
            n_found += (transforms[i] == PIL_ENCODE_DICT);
            n_found_auto += (transforms[i] == PIL_COMPRESS_AUTO); // not allowed to find an AUTO field if n > 1
            n_pos_last += i * (transforms[i] == PIL_ENCODE_DICT); // set to i if the predicate evaluates to true
        }
        if(n_found_auto) return false; // illegal to have auto mixed
        if(n_found == 0) return true;
        if(n_found != 1) return false; // illegal to have dict more than once

        n_found = 0;
        for(int i = 0; i < n_pos_last; ++i) {
            n_found += (transforms[i] >= 0 && transforms[i] <= 5); // 0-5 is compression codecs
        }
        if(n_found) return false; // found illegal compression before

        for(int i = n_pos_last + 1; i < transforms.size(); ++i) {
            n_found += (transforms[i] > 5); // >5 is encoding codecs
        }
        if(n_found) return false; // found illegal encoding after

        return true;
    }

    int AutoTransform(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field);
    int AutoTransformColumns(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field);
    int AutoTransformColumn(std::shared_ptr<ColumnStore> cstore, const DictionaryFieldType& field);
    int AutoTransformTensor(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field);

    int DeltaEncode(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field);
    int UnsafeDeltaEncode(std::shared_ptr<ColumnStore> cstore);

protected:
    // Any memory is owned by the respective Buffer instance (or its parents).
    MemoryPool* pool_;
    std::shared_ptr<ResizableBuffer> buffer;
};


}

#endif /* TRANSFORMER_H_ */
