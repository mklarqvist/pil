#include "dictionary_builder.h"

namespace pil {

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
