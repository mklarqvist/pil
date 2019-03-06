#ifndef ALGORITHM_DIGEST_VARIANT_DIGEST_MANAGER_H_
#define ALGORITHM_DIGEST_VARIANT_DIGEST_MANAGER_H_

#include <cstdint>
#include <openssl/md5.h>

#include "../buffer.h"

namespace pil {

class Digest {
public:
	template <class T>
	static void GenerateMd5(const T& value, uint8_t* dst){
	   // uint8_t hash[MD5_DIGEST_LENGTH];
		MD5_CTX md5;
		MD5_Init(&md5);
		MD5_Update(&md5, value, sizeof(T));
		MD5_Final(dst, &md5);
	}

	static void GenerateMd5(const uint8_t* data, const uint32_t l_data, uint8_t* dst){
	    // uint8_t hash[MD5_DIGEST_LENGTH];
        MD5_CTX md5;
        MD5_Init(&md5);
        MD5_Update(&md5, data, l_data);
        MD5_Final(dst, &md5);
	}

	static void GenerateMd5(std::shared_ptr<ResizableBuffer>& data, uint8_t* dst) {
	    MD5_CTX md5;
        MD5_Init(&md5);
        MD5_Update(&md5, data->mutable_data(), data->size());
        MD5_Final(dst, &md5);
	}
};

}


#endif /* ALGORITHM_DIGEST_VARIANT_DIGEST_MANAGER_H_ */
