#include "columnstore.h"
#include "transform/dictionary_builder.h"

namespace pil {

int ColumnStore::Serialize(std::ostream& stream) {
   stream.write(reinterpret_cast<char*>(&n_records), sizeof(uint32_t));
   stream.write(reinterpret_cast<char*>(&uncompressed_size), sizeof(uint32_t));
   stream.write(reinterpret_cast<char*>(&compressed_size),   sizeof(uint32_t));
   stream.write(reinterpret_cast<char*>(&nullity_u), sizeof(uint32_t));
   stream.write(reinterpret_cast<char*>(&nullity_c), sizeof(uint32_t));

   // Todo
   /*
   uint32_t n_transforms = transformations.size();
   stream.write(reinterpret_cast<char*>(&n_transforms), sizeof(uint32_t));

   //assert(transformations.size() != 0);
   for(size_t i = 0; i < transformations.size(); ++i) {
       uint32_t t_type = transformations[i];
       stream.write(reinterpret_cast<char*>(&t_type), sizeof(uint32_t));
   }
   */

   //if(buffer.get() != nullptr) {
       // If the data has been transformed we write out the compressed data
       // otherwise we write out the uncompressed data.
       if(transformation_args.size() != 0) {
           //std::cerr << "writing transformed n= " << size() << " c=" << compressed_size << std::endl;
           stream.write(reinterpret_cast<char*>(mutable_data()), compressed_size);
       } else {
           //std::cerr << "writing untransformed data= " << size() << " u=" << uncompressed_size << std::endl;
           stream.write(reinterpret_cast<char*>(mutable_data()), uncompressed_size);
       }
   //}

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
   }

   return(1);
}

}
