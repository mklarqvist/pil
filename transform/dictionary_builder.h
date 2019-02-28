#ifndef DICTIONARY_BUILDER_H_
#define DICTIONARY_BUILDER_H_

#include "column_dictionary.h"
#include "../buffer_builder.h"
#include "../table_dict.h"
#include "../columnstore.h"

namespace pil {

// Class for constructing Dictionaries from ColumnStore data.
// These are used during import steps only.
class DictionaryBuilder : public ColumnDictionary {
public:
   virtual ~DictionaryBuilder(){}

   int Serialize(std::ostream& stream) {
       stream.write(reinterpret_cast<char*>(&n_records), sizeof(int64_t));
       stream.write(reinterpret_cast<char*>(&n_elements), sizeof(int64_t));
       stream.write(reinterpret_cast<char*>(&sz_u), sizeof(int64_t));
       stream.write(reinterpret_cast<char*>(&sz_c), sizeof(int64_t));
       if(sz_c != 0) stream.write(reinterpret_cast<char*>(buffer->mutable_data()), sz_c);
       else stream.write(reinterpret_cast<char*>(buffer->mutable_data()), sz_u);
       return(1);
   }
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

       if(ratio_cardinality < 0.2) { // 20% or less and we store a dictionary for the data
           std::cerr << "DICT ENCODE unique=" << list.size() << "/" << n_valid << " (" << ratio_cardinality << ")" << std::endl;

           column->have_dictionary = true;

           // Step 1: Set dict buffer size to columndata size
           if(buffer.get() == nullptr) {
               //column->dictionary = std::make_shared<ColumnDictionary>(this);
               AllocateResizableBuffer(pool, list.size() * sizeof(T), &buffer);
           } else {
               int ret = buffer->Resize(list.size() * sizeof(T));
           }

           // 1) insert values into dictionary buffer of columnstore
           //column->dictionary->Resize(list.size() * sizeof(T) );

           sz_u = list.size() * sizeof(T);
           n_records = list.size();
           n_elements = list.size();
           T* dict = reinterpret_cast<T*>(buffer->mutable_data());
           for(int i = 0; i < list.size(); ++i)
               dict[i] = list[i];

           //for(int i = 0; i < n_records; ++i)
           //    std::cerr << ", " << (int)dict[i];
           //std::cerr << std::endl;


           uint32_t* d = new uint32_t[column->n_records];

           for(uint32_t i = 0; i < column->n_records; ++i) {
               if(column->IsValid(i) == false) {
                   // place a 0
                   d[i] = 0;
                   continue;
               }
               //std::cerr << "Map=" << in[i] << "->" << map[in[i]] << std::endl;
               d[i] = map[in[i]];
           }
           int64_t n_in = column->uncompressed_size;
           if(column->buffer.capacity() < column->n_records*sizeof(uint32_t)) {
               column->buffer.Resize(column->n_records*sizeof(uint32_t), false);
           }
           memcpy(column->buffer.mutable_data(), d, column->n_records*sizeof(uint32_t));
           column->buffer.UnsafeSetLength(column->n_records*sizeof(uint32_t));
           column->uncompressed_size = column->n_records*sizeof(uint32_t);
           column->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_ENCODE_DICT, n_in, column->buffer.length()));
           delete[] d;
           return(1);
       }

       //std::cerr << "b buffer=" << b->buffer.length() << " and src=" << column->buffer.length() << std::endl;
       //memcpy(dst->mutable_data(), dat, b->buffer.length());
       //column->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_ENCODE_DICT, b->buffer.length(), b->buffer.length()));
       //std::cerr << "map=" << list.size() << " out of " << src->n << std::endl;
       //std::cerr << "dst=" << dst->n << " sz=" << dst->uncompressed_size << std::endl;

       return(0);
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

       typedef std::unordered_map<uint64_t, uint32_t> map_type;
       map_type map;
       std::vector< uint64_t > hash_list; // list of lists of type T
       std::vector< std::vector<T> > list;
       int64_t sz_list = 0;
       T* in = reinterpret_cast<T*>(column->mutable_data());
       const uint32_t* s = reinterpret_cast<const uint32_t*>(strides->mutable_data());
       const uint32_t n_s = strides->n_records - 1;

       // Iterate over values in N
       int64_t n_valid = 0;
       //int64_t cum_offset = 0;
       //T* local_buffer = new T[8196];

      // bool die = false;
       uint64_t hash = 0;
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
           //memcpy(local_buffer, &in[s[i]], l);
           hash = XXH64(&in[s[i]], l, 123718);

           if(map.find(hash) == map.end()) {
               map[hash] = hash_list.size();
               //buffer_->Append(list.size());
               hash_list.push_back(hash);
               sz_list += l * sizeof(T);
               list.push_back(std::vector<T>(&in[s[i]], &in[s[i] + l]));
           }

           ++n_valid;
       }
       std::cerr << "Valid records=" << n_valid << "/" << column->n_records << " unique=" << hash_list.size() << std::endl;
       const double ratio_cardinality = (double)hash_list.size() / n_valid;

       if(ratio_cardinality < 0.3) { // 30% or less and we store a dictionary for the data
          std::cerr << "DICT ENCODE unique=" << hash_list.size() << "/" << n_valid << " (" << ratio_cardinality << ")" << std::endl;

          column->have_dictionary = true;

          /*
          for(int i = 0; i < list.size(); ++i) {
              std::cerr << ",";
              for(int j = 0; j < list[i].size(); ++j) {
                  std::cerr << (char)list[i][j];
              }
          }
          std::cerr << std::endl;
            */

          // Step 1: Set dict buffer size to columndata size
          if(buffer.get() == nullptr) {
              //column->dictionary = std::make_shared<ColumnDictionary>(this);
              AllocateResizableBuffer(pool, list.size()*sizeof(uint32_t) + sz_list, &buffer);
          } else {
             int ret = buffer->Resize(list.size()*sizeof(uint32_t) + sz_list);
          }

          // 1) insert values into dictionary buffer of columnstore
          //column->dictionary->Resize(list.size() * sizeof(T) );


          sz_u = list.size()*sizeof(uint32_t) + sz_list;
          n_records = list.size();
          n_elements = sz_list / sizeof(T);
          T* dict = reinterpret_cast<T*>(buffer->mutable_data());
          size_t dict_offset = 0;

          // Store as {length1,length2,...,lengthN}Z + {data}
          for(int i = 0; i < list.size(); ++i) {
              *reinterpret_cast<uint32_t*>(&dict[dict_offset]) = (uint32_t)list[i].size();
              dict_offset += sizeof(uint32_t);
              memcpy(&dict[dict_offset], list[i].data(), list[i].size());
              dict_offset += list[i].size();
          }

          //for(int i = 0; i < n_records; ++i)
          //    std::cerr << ", " << (int)dict[i];
          //std::cerr << std::endl;


          uint32_t* d = new uint32_t[n_s];

          uint64_t hash = 0;
         for(uint32_t i = 0; i < n_s; ++i) {
             // If value is not in the map we add it to the hash-map and
             // the list of values.
             // If the position is invalid (using nullity map) then don't use it.
             if(strides->IsValid(i) == false) {
                 //std::cerr << "skipping: " << i << " l=" << s[i] << std::endl;
                 d[i] = 0;
                 continue;
             }

             const int64_t l = s[i + 1] - s[i];
             //if(l < 0) exit(1);
             //die += (l < 0);
             //std::cerr << i << "/" << n_s << " with " << s[i] << "->" << s[i+1] << " length is = " << l << " go from " << s[i] << "->" << s[i]+l << std::endl;
             //memcpy(local_buffer, &in[s[i]], l);
             hash = XXH64(&in[s[i]], l, 123718);

             d[i] = map[hash];
         }

          int64_t n_in = column->uncompressed_size;
          if(column->buffer.capacity() < n_s*sizeof(uint32_t)) {
              column->buffer.Resize(n_s*sizeof(uint32_t), false);
          }
          memcpy(column->buffer.mutable_data(), d, n_s*sizeof(uint32_t));
          column->uncompressed_size = n_s*sizeof(uint32_t);
          column->buffer.UnsafeSetLength(n_s*sizeof(uint32_t));
          column->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_ENCODE_DICT, n_in, column->buffer.length()));
          std::cerr << "DICT for string: shrink " << n_in << "->" << column->buffer.length() << std::endl;
          std::cerr << "DICT meta=" << sz_list << "b" << std::endl;
          delete[] d;
          return(1);
      }

       //delete[] local_buffer;
       //if(die) exit(1);

       return(0);
   }

protected:

};


}


#endif /* DICTIONARY_BUILDER_H_ */
