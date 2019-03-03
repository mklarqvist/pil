#include "column_store.h"
#include "transform/dictionary_builder.h"

namespace pil {

int ColumnStore::Serialize(std::ostream& stream) {
   stream.write(reinterpret_cast<char*>(&have_dictionary), sizeof(bool));
   stream.write(reinterpret_cast<char*>(&n_records), sizeof(uint32_t));
   stream.write(reinterpret_cast<char*>(&n_elements), sizeof(uint32_t));
   stream.write(reinterpret_cast<char*>(&n_null), sizeof(uint32_t));
   stream.write(reinterpret_cast<char*>(&uncompressed_size), sizeof(uint32_t));
   stream.write(reinterpret_cast<char*>(&compressed_size),   sizeof(uint32_t));
   stream.write(reinterpret_cast<char*>(&nullity_u), sizeof(uint32_t));
   stream.write(reinterpret_cast<char*>(&nullity_c), sizeof(uint32_t));

   // Nullity vector
   //const uint32_t n_nullity = std::ceil((float)n / 32);
   if(nullity.get() != nullptr) {
       const uint32_t* nulls = reinterpret_cast<const uint32_t*>(nullity->mutable_data());
       stream.write(reinterpret_cast<const char*>(nulls), nullity_c);
   }

   // Dictionary encoding
   stream.write(reinterpret_cast<char*>(&have_dictionary), sizeof(bool));
   if(have_dictionary) {
       assert(dictionary.get() != nullptr);

       std::static_pointer_cast<DictionaryBuilder>(dictionary)->Serialize(stream);

   // Todo
   uint32_t n_transforms = transformation_args.size();
   stream.write(reinterpret_cast<char*>(&n_transforms), sizeof(uint32_t));
   for(int i = 0; i < n_transforms; ++i) transformation_args[i]->Serialize(stream);

   // Write md5 checksum of uncompressed data
   stream.write(reinterpret_cast<char*>(md5_checksum), 16);

   // If the data has been transformed we write out the compressed data
   // otherwise we write out the uncompressed data.
   if(n_transforms != 0) {
       stream.write(reinterpret_cast<char*>(mutable_data()), compressed_size);
   } else {
       stream.write(reinterpret_cast<char*>(mutable_data()), uncompressed_size);
   }

   }

   return(1);
}

}
