#ifndef DICTIONARY_BUILDER_H_
#define DICTIONARY_BUILDER_H_

#include "../column_dictionary.h"
#include "../buffer_builder.h"
#include "../table_dict.h"
#include "../columnstore.h"

namespace pil {

// Class for constructing Dictionaries from ColumnStore data.
// These are used during import steps only.
class DictionaryBuilder : public ColumnDictionary {
public:
   virtual ~DictionaryBuilder(){}

   /**<
    * Serialize the Dictionary to a destination output stream.
    * @param stream Destination output stream inheriting from std::ostream.
    * @return Returns -1 if stream is bad or the total written size in bytes otherwise.
    */
   int Serialize(std::ostream& stream);
};

template <class T>
class NumericDictionaryBuilder : public DictionaryBuilder {
public:
    /**<
     * Wrapper function for the specific Encode functions.
     *
     * Encodes the target ColumnSet given the provided DictionaryFieldType.
     * If the input ColumnSet is stored as PIL_CSTORE_COLUMN then each column
     * gets dictionary encoded separately.
     * If the ColumnSet is of type PIL_CSTORE_TENSOR then the standard two-column
     * {strides, data} interpretation is used.
     *
     * Return values:
     *    All positive values correspond to successful encoding.
     *    Failed codes (PIL_DICTIONARY_ERRORS):
     *       -1 ColumnSet is a nullpointer
     *       -2 A ColumnStore is a nullpointer
     *       -3 Unknown PIL_CSTORE_TYPE
     *       -4 No Nullity
     *       -5 Malformed data
     *
     * @param cset  Input and destination ColumnSet.
     * @param field DictionaryFieldType matching the proided ColumnSet.
     * @param force Set force=TRUE to force construction of the dictionary even when expensive.
     * @return Negative return values are errors (see above) and positive values are success.
     */
    int Encode(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field, const bool force = false);

    /**<
     * Dictionary encode the provided PIL_CSTORE_COLUMN ColumnStore.
     * @param column
     * @param force
     * @return
     */
     int Encode(std::shared_ptr<ColumnStore> column, const bool force = false);

   /**<
    * Dictionary encode the provided PIL_CSTORE_TENSOR ColumnStore using the
    * additional information from the provided stride ColumnStore.
    * @param column
    * @param strides
    * @param force
    * @return
    */
   int Encode(std::shared_ptr<ColumnStore> column, std::shared_ptr<ColumnStore> strides, const bool force = false);

};

template <class T>
struct VectorHasher {
    uint64_t operator()(const std::vector<T>& v) const {
        uint64_t hash = 0;
        hash = XXH64(&v[0], v.size(), 123718);
        return hash;
    }
};

// impl

template <class T>
int NumericDictionaryBuilder<T>::Encode(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field, const bool force) {
    if(cset.get() == nullptr) return(PIL_DICT_SET_NULLPTR);
    if(cset->size() == 0) return(PIL_DICT_NO_DATA);

    if(field.cstore == PIL_CSTORE_COLUMN) {
        int ret = 0;
        for(int i = 0; i < cset->size(); ++i) {
            assert(cset->columns[i].get() != nullptr);
            ret = Encode(cset->columns[i], force);
        }
        return(ret);
    } else if(field.cstore == PIL_CSTORE_TENSOR) {
        if(cset->size() != 2) return(PIL_DICT_MALFORMED);
        if(cset->columns[0].get() == nullptr) return(PIL_DICT_STORE_NULLPTR);
        if(cset->columns[1].get() == nullptr) return(PIL_DICT_STORE_NULLPTR);
        int ret = Encode(cset->columns[1], cset->columns[0], force);
        return(ret);
    } else {
        std::cerr << "unknown cstroe type!" << std::endl;
        return(PIL_DICT_ILLEGAL_CSTORE);
    }
}

template <class T>
int NumericDictionaryBuilder<T>::Encode(std::shared_ptr<ColumnStore> column, const bool force) {
   if(column.get() == nullptr) return(-1);

   if(column->nullity.get() == nullptr) {
       return(PIL_DICT_MISSING_NULLITY);
   }

   if(column->n_elements * sizeof(T) != column->buffer.length()) {
       return(PIL_DICT_MALFORMED);
   }

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
           list.push_back(in[i]);
       }
       ++n_valid;
   }


   const double ratio_cardinality = (double)list.size() / n_valid;

   if(ratio_cardinality < 0.2 || force) { // 20% or less and we store a dictionary for the data
       std::cerr << "DICT ENCODE unique=" << list.size() << "/" << n_valid << " (" << ratio_cardinality << ")" << std::endl;

       column->have_dictionary = true;

       // Step 1: Set dict buffer size to columndata size
       if(buffer.get() == nullptr) {
           //column->dictionary = std::make_shared<ColumnDictionary>(this);
           int ret = AllocateResizableBuffer(pool, list.size() * sizeof(T), &buffer);
           if(ret == -1) return(-1);
       } else {
           int ret = buffer->Resize(list.size() * sizeof(T));
           if(ret == -1) return(-1);
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

   return(0);
}

template <class T>
int NumericDictionaryBuilder<T>::Encode(std::shared_ptr<ColumnStore> column, std::shared_ptr<ColumnStore> strides, const bool force) {
   if(column.get() == nullptr) return(PIL_DICT_STORE_NULLPTR);
   if(strides.get() == nullptr) return(PIL_DICT_STORE_NULLPTR);

   if(strides->nullity.get() == nullptr) {
       return(PIL_DICT_MISSING_NULLITY);
   }

   if(column->n_elements * sizeof(T) != column->buffer.length()) {
       return(PIL_DICT_MALFORMED);
   }

   if(strides->nullity.get() == nullptr) { return(PIL_DICT_MALFORMED); }

   typedef std::unordered_map<std::vector<T>, bool, VectorHasher<T>> map_type;
   map_type map;
   std::vector< std::vector<T> > list;
   int64_t sz_list = 0;
   const T* in = reinterpret_cast<const T*>(column->mutable_data());
   const uint32_t* s = reinterpret_cast<const uint32_t*>(strides->mutable_data());
   const uint32_t n_s = strides->n_records - 1;

   // Iterate over values in N
   int64_t n_valid = 0;

   for(uint32_t i = 0; i < n_s; ++i) {
       // If value is not in the map we add it to the hash-map and
       // the list of values.
       // If the position is invalid (using nullity map) then don't use it.
       if(strides->IsValid(i) == false) {
           //std::cerr << "skipping: " << i << " l=" << s[i] << std::endl;
           continue;
       }

       const int64_t l = s[i + 1] - s[i];
       std::vector<T> vec(&in[s[i]], &in[s[i+1]]);

       if(map.find(vec) == map.end()) {
           map[vec] = list.size();
           sz_list += l * sizeof(T);
           list.push_back(vec);
       }
       ++n_valid;
   }
   //std::cerr << "Valid records=" << n_valid << "/" << column->n_records << " unique=" << hash_list.size() << std::endl;
   const double ratio_cardinality = (double)list.size() / n_valid;

   if(ratio_cardinality < 0.3 || force) { // 30% or less and we store a dictionary for the data
      std::cerr << "DICT ENCODE unique=" << list.size() << "/" << n_valid << " (" << ratio_cardinality << ")" << std::endl;

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
          int ret = AllocateResizableBuffer(pool, sz_list, &buffer);
          if(ret == -1) return(-1);
      } else {
         int ret = buffer->Resize(sz_list, false);
         if(ret == -1) return(-1);
      }

      if(lengths.get() == nullptr) {
        int ret = AllocateResizableBuffer(pool, list.size()*sizeof(uint32_t), &lengths);
        if(ret == -1) return(-1);
    } else {
       int ret = lengths->Resize(list.size()*sizeof(uint32_t));
       if(ret == -1) return(-1);
    }

      // 1) insert values into dictionary buffer of columnstore
      //column->dictionary->Resize(list.size() * sizeof(T) );

      have_lengths = true;
      sz_u = sz_list;
      n_records = list.size();
      n_elements = sz_list / sizeof(T);
      sz_lu = list.size()*sizeof(uint32_t);
      T* dict = reinterpret_cast<T*>(buffer->mutable_data());
      uint32_t* ls = reinterpret_cast<uint32_t*>(lengths->mutable_data());

      // Store as {length1,length2,...,lengthN}Z + {data}
      size_t dict_offset = 0;
      for(int i = 0; i < list.size(); ++i) {
          ls[i] = list[i].size();
          //std::cerr << "len=" << list[i].size() << std::endl;
          for(int k = 0; k < list[i].size(); ++k, ++dict_offset) {
              //std::cerr << "," << list[i][k];
              dict[dict_offset] = list[i][k];
          }
         //std::cerr << std::endl;
      }
      //std::cerr << "done insert" << std::endl;

      //for(int i = 0; i < n_records; ++i)
      //    std::cerr << ", " << (int)dict[i];
      //std::cerr << std::endl;

      int64_t dict_o = 0;
      for(int i = 0; i < n_records; ++i) {
          //std::cerr << ls[i] << ": ";
          //for(int j = 0; j < ls[i]; ++j) {
          //    std::cerr << "," << dict[dict_o + j];
          //}
          //std::cerr << std::endl;
          //std::cerr << ls[i] << ": " << std::string(reinterpret_cast<char*>(&dict[dict_o]), ls[i]) << std::endl;
          dict_o += ls[i];
      }
      //std::cerr << std::endl;

      uint32_t* d = new uint32_t[n_s];

      //uint64_t hash = 0;
    for(uint32_t i = 0; i < n_s; ++i) {
        // If value is not in the map we add it to the hash-map and
        // the list of values.
        // If the position is invalid (using nullity map) then don't use it.
        if(strides->IsValid(i) == false) {
            //std::cerr << "skipping: " << i << " l=" << s[i] << std::endl;
            d[i] = 0;
            continue;
        }
        std::vector<T> vec(&in[s[i]], &in[s[i+1]]);

        d[i] = map[vec];
    }

    int64_t n_in = column->uncompressed_size;
    if(column->buffer.capacity() < n_s*sizeof(uint32_t)) {
        column->buffer.Resize(n_s*sizeof(uint32_t), false);
    }
    memcpy(column->buffer.mutable_data(), d, n_s*sizeof(uint32_t));
    column->uncompressed_size = n_s*sizeof(uint32_t);
    column->buffer.UnsafeSetLength(n_s*sizeof(uint32_t));
    column->transformation_args.push_back(std::make_shared<TransformMeta>(PIL_ENCODE_DICT, n_in, column->buffer.length()));
    //std::cerr << "DICT for string: shrink " << n_in << "->" << column->buffer.length() << std::endl;
    //std::cerr << "DICT meta=" << sz_list << "b" << std::endl;
    delete[] d;
    return(1);
   }

   return(0);
}


}


#endif /* DICTIONARY_BUILDER_H_ */
