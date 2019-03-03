#include "dictionary_builder.h"

namespace pil {

int DictionaryBuilder::Encode(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field, const bool force) {
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

int DictionaryBuilder::Encode(std::shared_ptr<ColumnStore> cstore, const DictionaryFieldType& field, bool force) {
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

int DictionaryBuilder::Encode(std::shared_ptr<ColumnStore> cstore, std::shared_ptr<ColumnStore> strides, const DictionaryFieldType& field, const bool force) {
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

int DictionaryBuilder::Serialize(std::ostream& stream) {
   if(stream.good() == false) return(-1);

   stream.write(reinterpret_cast<char*>(&have_lengths), sizeof(bool));
   stream.write(reinterpret_cast<char*>(&n_records),    sizeof(int64_t));
   stream.write(reinterpret_cast<char*>(&n_elements),   sizeof(int64_t));
   stream.write(reinterpret_cast<char*>(&sz_u),  sizeof(int64_t));
   stream.write(reinterpret_cast<char*>(&sz_c),  sizeof(int64_t));
   stream.write(reinterpret_cast<char*>(&sz_lu), sizeof(int64_t));
   stream.write(reinterpret_cast<char*>(&sz_lc), sizeof(int64_t));

   int ret = sizeof(bool) + sizeof(int64_t)*6;

   if(sz_c != 0) {
       stream.write(reinterpret_cast<char*>(buffer->mutable_data()), sz_c);
       ret += sz_c;
   }
   else {
       stream.write(reinterpret_cast<char*>(buffer->mutable_data()), sz_u);
       ret += sz_u;
   }

   if(have_lengths) {
       if(sz_lc != 0) {
           stream.write(reinterpret_cast<char*>(lengths->mutable_data()), sz_lc);
           ret += sz_lc;
       }
       else {
           stream.write(reinterpret_cast<char*>(lengths->mutable_data()), sz_lu);
           ret += sz_lu;
       }
   }

   return(stream.good() ? ret : -1);
}

}
