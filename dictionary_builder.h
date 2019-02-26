#ifndef DICTIONARY_BUILDER_H_
#define DICTIONARY_BUILDER_H_

#include "columnstore.h"
#include "buffer_builder.h"

namespace pil {

// Class for constructing Dictionaries from ColumnStore data.
// These are used during import steps only.
class DictionaryBuilder {
public:
   DictionaryBuilder() : n_records_(0){}
   virtual ~DictionaryBuilder(){}

   inline int64_t size() const { return n_records_; }

protected:
   int64_t n_records_;
   std::shared_ptr<BufferBuilder> buffer_;
};

template <class T>
class NumericDictionaryBuilder : public DictionaryBuilder {
public:
   int Encode(std::shared_ptr<ColumnStore> column) {
       //std::cerr << "in dictionary encode" << std::endl;
       //std::cerr << "dict input length=" << src->buffer.length() << std::endl;
       assert(column->n * sizeof(T) == column->buffer.length());

       typedef std::unordered_map<T, uint32_t> map_type;
       map_type map;
       std::vector<T> list;
       T* in = reinterpret_cast<T*>(column->mutable_data());

       // Iterate over values in N
       int64_t n_valid = 0;
       for(uint32_t i = 0; i < column->n; ++i) {
           // If value is not in the map we add it to the hash-map and
           // the list of values.
           // If the position is invalid (using nullity map) then don't use it.
           if(column->IsValid(i) == false) continue;

           if(map.find(in[i]) == map.end()) {
               map[in[i]] = list.size();
               //buffer_->Append(list.size());
               list.push_back(in[i]);
           }
           ++n_valid;
       }


       const double ratio_cardinality = (double)list.size() / n_valid;

       if(ratio_cardinality < 0.1) { // 10% or less and we store a dictionary for the data
           std::cerr << "DICT ENCODE unique=" << list.size() << "/" << n_valid << " (" << ratio_cardinality << ")" << std::endl;

           column->have_dictionary = true;
           // Todo: need to transform input column interpretation as
           // uint32_t now

           // 1) insert values into dictionary buffer of columnstore
           // 2) empty local buffer (unsafe set to 0)
           // 3) update local buffer with dictionary-encoded columnstore data
           // test
           for(uint32_t i = 0; i < column->n; ++i) {
               if(column->IsValid(i) == false) {
                   // place a 0
                   in[i] = 0;
                   continue;
               }
               //std::cerr << "Map=" << in[i] << "->" << map[in[i]] << std::endl;
               in[i] = map[in[i]];
           }
           // 4) local buffer.swap(other) -> http://www.cplusplus.com/reference/memory/shared_ptr/swap/

           column->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_ENCODE_DICT, column->buffer.length(), column->buffer.length()));
       }

       //std::cerr << "b buffer=" << b->buffer.length() << " and src=" << column->buffer.length() << std::endl;
       //memcpy(dst->mutable_data(), dat, b->buffer.length());
       //column->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_ENCODE_DICT, b->buffer.length(), b->buffer.length()));
       //std::cerr << "map=" << list.size() << " out of " << src->n << std::endl;
       //std::cerr << "dst=" << dst->n << " sz=" << dst->uncompressed_size << std::endl;

       return(1);
   }

   int Encode(std::shared_ptr<ColumnStore> column, std::shared_ptr<ColumnStore> strides) { return -1; }

protected:

};


}


#endif /* DICTIONARY_BUILDER_H_ */
