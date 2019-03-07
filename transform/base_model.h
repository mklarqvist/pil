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

/*
 * Code rewritten to templated C++11 and commented by Marcus D. R. Klarqvist
 */
#ifndef BASE_MODEL_H_
#define BASE_MODEL_H_

#include "range_coder.h"

namespace pil {

// Small enough to not overflow uint16_t even after +STEP
#define PIL_BASE_MODEL_M4(a) ((a)[0]>(a)[1]?((a)[0]>(a)[2]?((a)[0]>(a)[3]?(a)[0]:(a)[3]):((a)[2]>(a)[3]?(a)[2]:(a)[3])):((a)[1]>(a)[2]?((a)[1]>(a)[3]?(a)[1]:(a)[3]):((a)[2]>(a)[3]?(a)[2]:(a)[3])))

/*
 * A fixed alphabet encoder for symbols 0 to 5 inclusive.
 * We have no escape symbol or resorting of data. It simply
 * accumulates stats and encodes in proportion.
 *
 * The intention is to use this per column for encoding
 * bases that differ to the consensus.
 */

/*
 * Fast mode uses 8-bit counters. It's generally 40% faster or so, but less
 * accurate and so larger file size (maybe only 1% though unless deep data).
 */

template <typename T>
class BaseModel {
private:
    static constexpr int STEP = sizeof(T) == 1 ? 1 : 8;
    static constexpr int WSIZ = (1 << 8*sizeof(T)) - 2*STEP;

public:
    BaseModel();
    BaseModel(int* start);
    void reset();
    void reset(int* start);
    inline void EncodeSymbol(RangeCoder* rc, uint32_t sym);
    inline void UpdateSymbol(uint32_t sym);
    inline uint32_t DecodeSymbol(RangeCoder* rc);

    /**<
     * Returns the bias of the best symbol compared to all other symbols.
     * This is a measure of how well adapted this model thinks it is to the
     * incoming probabilities.
     *
     * @return
     */
    inline uint32_t GetTopSym(void);
    inline uint32_t GetSummFreq(void);
    void rescaleRare();

public:
    T Stats[4];
};

// impl

template <typename T>
BaseModel<T>::BaseModel()
{
    reset();
}

template <typename T>
BaseModel<T>::BaseModel(int* start) {
    for (int i = 0; i < 4; i++) {
        Stats[i] =  start[i];
    }
}

template <typename T>
void BaseModel<T>::reset() {
    for ( int i=0; i<4; i++ )
    Stats[i] = 3*STEP;
}

template <typename T>
void BaseModel<T>::reset(int* start) {
    for (int i = 0; i < 4; i++) {
        Stats[i] =  start[i];
    }
}

template <typename T>
void BaseModel<T>::rescaleRare()
{
    Stats[0] -= (Stats[0] >> 1);
    Stats[1] -= (Stats[1] >> 1);
    Stats[2] -= (Stats[2] >> 1);
    Stats[3] -= (Stats[3] >> 1);
}

template <typename T>
inline void BaseModel<T>::EncodeSymbol(RangeCoder *rc, uint32_t sym) {
    int SummFreq = (Stats[0] + Stats[1]) + (Stats[2] + Stats[3]);
    if ( SummFreq>=WSIZ ) {
        rescaleRare();
        SummFreq = (Stats[0] + Stats[1]) + (Stats[2] + Stats[3]);
    }

    switch(sym) {
    case 0:
    rc->Encode(0,                              Stats[0], SummFreq);
    Stats[0] += STEP;
    break;
    case 1:
    rc->Encode(Stats[0],                       Stats[1], SummFreq);
    Stats[1] += STEP;
    break;
    case 2:
    rc->Encode(Stats[0] + Stats[1],            Stats[2], SummFreq);
    Stats[2] += STEP;
    break;
    case 3:
    rc->Encode((Stats[0] + Stats[1]) + Stats[2], Stats[3], SummFreq);
    Stats[3] += STEP;
    break;
    }

    /*
     * Scary but marginally faster due to removing branching:
     *
     * uint32_t y = ((Stats[0]<<8)) |
     *              ((Stats[0] + Stats[1]) * 0x01010000) |
     *               (Stats[2]<<24);
     * rc->Encode((y&(0xff<<(sym<<3)))>>(sym<<3), Stats[sym], SummFreq);
     * Stats[sym] += STEP;
     */

    return;
}

template <typename T>
inline void BaseModel<T>::UpdateSymbol(uint32_t sym) {
    int SummFreq = (Stats[0] + Stats[1]) + (Stats[2] + Stats[3]);
    if ( SummFreq>=WSIZ ) {
        rescaleRare();
    }

    /* known symbol */
    Stats[sym] += STEP;
}

template <typename T>
inline uint32_t BaseModel<T>::GetTopSym(void) {
    return PIL_BASE_MODEL_M4(Stats);
}

template <typename T>
inline uint32_t BaseModel<T>::GetSummFreq(void) {
    int SummFreq = (Stats[0] + Stats[1]) + (Stats[2] + Stats[3]);
    return SummFreq;
}

template <typename T>
inline uint32_t BaseModel<T>::DecodeSymbol(RangeCoder *rc) {
    int SummFreq = (Stats[0] + Stats[1]) + (Stats[2] + Stats[3]);
    if ( SummFreq>=WSIZ) {
        rescaleRare();
        SummFreq = (Stats[0] + Stats[1]) + (Stats[2] + Stats[3]);
    }

    uint32_t count=rc->GetFreq(SummFreq);
    uint32_t HiCount=0;

    T* p=Stats;
    if ((HiCount += *p) > count) {
        rc->Decode(0, *p, SummFreq);
        Stats[0] += STEP;
        return 0;
    }

    if ((HiCount += *++p) > count) {
        rc->Decode(HiCount-*p, *p, SummFreq);
        Stats[1] += STEP;
        return 1;
    }

    if ((HiCount += *++p) > count) {
        rc->Decode(HiCount-*p, *p, SummFreq);
        Stats[2] += STEP;
        return 2;
    }

    rc->Decode(HiCount, Stats[3], SummFreq);
    Stats[3] += STEP;

    return 3;
}


}



#endif /* BASE_MODEL_H_ */
