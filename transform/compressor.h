/*
* Copyright (c) 2019 Marcus D. R. Klarqvist
* Author(s): Marcus D. R. Klarqvist
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
/*
 * Copyright (c) 2013-2019 Genome Research Ltd.
 * Author(s): James Bonfield
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above
 *       copyright notice, this list of conditions and the following
 *       disclaimer in the documentation and/or other materials provided
 *       with the distribution.
 *
 *    3. Neither the names Genome Research Ltd and Wellcome Trust Sanger
 *    Institute nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific
 *    prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY GENOME RESEARCH LTD AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL GENOME RESEARCH
 * LTD OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#ifndef TRANSFORM_COMPRESSOR_H_
#define TRANSFORM_COMPRESSOR_H_

#include "transformer.h"

#define PIL_ZSTD_DEFAULT_LEVEL 1

namespace pil {

class Compressor : public Transformer {
public:
    Compressor(){}
    Compressor(std::shared_ptr<ResizableBuffer> data) : Transformer(data){}
    ~Compressor(){}

    inline std::shared_ptr<ResizableBuffer> data() const { return(buffer); }
};

class ZstdCompressor : public Compressor {
public:
    int Compress(std::shared_ptr<ColumnSet> cset, const PIL_CSTORE_TYPE& field_type, const int compression_level = 1);
    int Compress(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field, const int compression_level = 1);
    int Compress(const uint8_t* src, const uint32_t n_src, const int compression_level = 1);
    /**<
     * Decompression requires that `compressed_size` is properly set to the
     * correct value. Unsafe method as we do not know the uncompressed size
     * or the correctness of decompressor.
     * @param cstore
     * @param back_copy
     * @return
     */
    int UnsafeDecompress(std::shared_ptr<ColumnStore> cstore, const bool back_copy = true);

    /**<
     * Safe decompression of the target ColumnStore.
     * @param cstore
     * @param meta
     * @param back_copy
     * @return
     */
    int Decompress(std::shared_ptr<ColumnStore> cstore, std::shared_ptr<TransformMeta> meta, const bool back_copy = true);
};

class QualityCompressor : public Compressor {
public:
    int Compress(std::shared_ptr<ColumnSet> cset, PIL_CSTORE_TYPE cstore);
    int Decompress(std::shared_ptr<ColumnSet> cset, PIL_CSTORE_TYPE cstore);

    /**<
     * We use generic maps to turn 0-M into 0-N where N <= M
     * before adding these into the context.  These are used
     * for positions, running-diffs and quality values.
     *
     * This can be used as a simple divisor, eg pos/24 to get
     * 2 bits of positional data for each quarter along a 100bp
     * read, or it can be tailored for specific such as noting
     * the first 5 cycles are poor, then we have stability and
     * a gradual drop off in the last 20 or so.  Perhaps we then
     * map pos 0-4=0, 5-79=1, 80-89=2, 90-99=3.
     *
     * We don't need to specify how many bits of data we are
     * using (2 in the above example), as that is just implicit
     * in the values in the map.  Specify not to use a map simply
     * disables that context type (our map is essentially 0-M -> 0).
     * @param vers
     * @param level
     * @param quals
     * @param out
     * @param out_size
     * @return
     */
    int Compress(int vers, int level, std::shared_ptr<ColumnSet> quals, std::shared_ptr<ResizableBuffer> out, size_t& out_size);
    int Decompress(std::shared_ptr<ColumnSet> quals, std::shared_ptr<ResizableBuffer> out, size_t& out_size);
};

class SequenceCompressor : public Compressor {
public:
    int Compress(std::shared_ptr<ColumnSet> cset, PIL_CSTORE_TYPE cstore);
    int Compress(const uint8_t* bases, const uint32_t n_src, const uint32_t* lengths, const uint32_t n_lengths);
    int Decompress(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field);
    int DecompressStrides(std::shared_ptr<ColumnSet> cset, const DictionaryFieldType& field);
};

}

#endif /* TRANSFORM_COMPRESSOR_H_ */
