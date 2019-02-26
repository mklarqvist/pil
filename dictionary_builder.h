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
    int Encode(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field) {
        if(cset.get() == nullptr) return(-1);

        std::cerr << "in dictionary wrapper" << std::endl;

        if(field.cstore == PIL_CSTORE_COLUMN) {
            int ret = 0;
            for(int i = 0; i < cset->size(); ++i) {
                assert(cset->columns[i].get() != nullptr);
                ret = Encode(cset->columns[i]);
            }
            return(ret);
        } else {
            assert(cset->columns[1].get() != nullptr);
            //return(Encode(cset->columns[1]));
            return(1);
        }
    }

   int Encode(std::shared_ptr<ColumnStore> column) {
       if(column.get() == nullptr) return(-1);

       //std::cerr << "in dictionary encode" << std::endl;
       //std::cerr << "dict input length=" << column->buffer.length() << std::endl;
       if(column->nullity.get() == nullptr) {
           std::cerr << "no nullity" << std::endl;
           return(-1);
       }

       assert(column->n_elements * sizeof(T) == column->buffer.length());

       typedef std::unordered_map<T, uint32_t> map_type;
       map_type map;
       std::vector<T> list;
       T* in = reinterpret_cast<T*>(column->mutable_data());

       // Iterate over values in N
       int64_t n_valid = 0;
       for(uint32_t i = 0; i < column->n_records; ++i) {
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
           for(uint32_t i = 0; i < column->n_records; ++i) {
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

   int Encode(std::shared_ptr<ColumnStore> column, std::shared_ptr<ColumnStore> strides) {
       if(column.get() == nullptr) return(-1);

       std::cerr << "in dictionary encode WITH STRIDES" << std::endl;
       //std::cerr << "dict input length=" << column->buffer.length() << std::endl;
       if(strides->nullity.get() == nullptr) {
           std::cerr << "no nullity!!!!" << std::endl;
           return(-1);
       }

       assert(column->n_elements * sizeof(T) == column->buffer.length());

       typedef std::unordered_map<std::vector<T>, uint32_t> map_type;
       //map_type map;
       std::vector< std::vector<T> > list; // list of lists of type T
       T* in = reinterpret_cast<T*>(column->mutable_data());
       const uint32_t* s = reinterpret_cast<const uint32_t*>(strides->mutable_data());
       const uint32_t n_s = strides->n_records - 1;

       // Iterate over values in N
       int64_t n_valid = 0;
       //int64_t cum_offset = 0;
       T* local_buffer = new T[8196];

      // bool die = false;
       for(uint32_t i = 0; i < n_s; ++i) {
           // If value is not in the map we add it to the hash-map and
           // the list of values.
           // If the position is invalid (using nullity map) then don't use it.
           if(strides->IsValid(i) == false) {
               //std::cerr << "skipping: " << i << " l=" << s[i] << std::endl;
               continue;
           }

           const int64_t l = s[i + 1] - s[i];
           //if(l < 0) exit(1);
           //die += (l < 0);
           //std::cerr << i << "/" << n_s << " with " << s[i] << "->" << s[i+1] << " length is = " << l << " go from " << s[i] << "->" << s[i]+l << std::endl;
           memcpy(local_buffer, &in[s[i]], l);

           /*
           if(map.find(in[i]) == map.end()) {
               map[in[i]] = list.size();
               //buffer_->Append(list.size());
               list.push_back(in[i]);
           }
           */
           ++n_valid;
       }
       std::cerr << "Valid records=" << n_valid << "/" << column->n_records << std::endl;

       delete[] local_buffer;
       //if(die) exit(1);

       return(1);
   }

protected:

};


}


#endif /* DICTIONARY_BUILDER_H_ */
