#ifndef ALGORITHM_DIGEST_H_
#define ALGORITHM_DIGEST_H_

#include <fstream>
#include <openssl/sha.h>

namespace pil {

struct DigitalDigest {
private:
	typedef DigitalDigest self_type;

public:
	DigitalDigest(void) :
		hasInitialized(true),
		hasFinished(false)
	{
		this->initialize();
	}

	~DigitalDigest(void){}

	/**<
	 * Initializes the SHA512 context. Calling this function
	 * is mandatory!
	 * @return
	 */
	bool initialize(){
		this->hasInitialized = true;

		if(!SHA512_Init(&this->context)) return false;

		return true;
	}

	bool update(const uint8_t* data, uint32_t n_data){
		if(!this->hasInitialized) {
		    if(this->initialize() == false)
		        return false;
		}

		if(!SHA512_Update(&this->context, (const uint8_t*)data, n_data))
			return false;

		return true;
	}

	bool finalize(){
		if(!this->hasInitialized) return false;
		if(this->hasFinished) return true;

		if(!SHA512_Final(&this->digest[0], &this->context))
			return false;

		return true;
	}


	void clear(void){
		this->hasFinished = false;
		this->finalize();
		memset(&this->digest[0], 0, 64);
		this->initialize();
		this->hasInitialized = true;
	}

public:
	bool       hasInitialized;
	bool       hasFinished;
	SHA512_CTX context;
	uint8_t    digest[64];
};


}



#endif /* ALGORITHM_DIGEST_H_ */
