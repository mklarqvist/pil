/*
 *--------------------------------------------------------------------------
 * A simple frequency model.
 *
 * Define NSYM to be an integer value before including this file.
 * It will then generate types and functions specific to that
 * maximum number of symbols.
 *
 * This keeps a list of symbols and their frequencies, approximately
 * sorted by symbol frequency. We allow for a single symbol to periodically
 * move up the list when emitted, effectively doing a single step of
 * bubble sort periodically. This means it's largely the same complexity
 * irrespective of alphabet size.
 * It's more efficient on strongly biased distributions than random data.
 *
 * There is no escape symbol, so the model is tailored to relatively
 * stationary samples (although we do have occasional normalisation to
 * avoid frequency counters getting too high).
 *--------------------------------------------------------------------------
 */
#ifndef FREQUENCY_MODEL_H_
#define FREQUENCY_MODEL_H_

#include <xmmintrin.h>
#include "range_coder.h"

namespace pil {

#define MAX_FREQ (1<<16)-16
#define STEP 8
typedef struct {
    uint16_t Freq;
    uint16_t Symbol;
} SymFreqs;

template <int NSYM>
class FrequencyModel {
public:
    FrequencyModel() : TotFreq(0) { Initiate(NSYM); }

    FrequencyModel(int max_sym) {
        int i;

        for (i=0; i<max_sym; i++) {
            F[i].Symbol = i;
            F[i].Freq   = 1;
        }

        for (; i<NSYM; i++) {
            F[i].Symbol = i;
            F[i].Freq   = 0;
        }

        TotFreq         = max_sym;
        sentinel.Symbol = 0;
        sentinel.Freq   = MAX_FREQ; // Always first; simplifies sorting.

        F[NSYM].Freq    = 0; // terminates normalize() loop. See below.
    }

    void Initiate(int max_sym) {
        int i;

        for (i=0; i<max_sym; i++) {
            F[i].Symbol = i;
            F[i].Freq   = 1;
        }

        for (; i<NSYM; i++) {
            F[i].Symbol = i;
            F[i].Freq   = 0;
        }

        TotFreq         = max_sym;
        sentinel.Symbol = 0;
        sentinel.Freq   = MAX_FREQ; // Always first; simplifies sorting.

        F[NSYM].Freq    = 0; // terminates normalize() loop. See below.
    }

    void Normalize() {
        SymFreqs *s;

        /* Faster than F[i].Freq for 0 <= i < NSYM */
        TotFreq=0;
        for (s = F; s->Freq; s++) {
            s->Freq -= s->Freq>>1;
            TotFreq += s->Freq;
        }
    }

    #ifdef __SSE__
    #   include <xmmintrin.h>
    #else
    #   define _mm_prefetch(a,b)
    #endif

    void EncodeSymbol(RangeCoder *rc, uint16_t sym) {
        SymFreqs *s = F;
        uint32_t AccFreq  = 0;

        while (s->Symbol != sym) {
            AccFreq += s++->Freq;
            _mm_prefetch((const char *)(s+1), _MM_HINT_T0);
        }

        rc->Encode(AccFreq, s->Freq, TotFreq);
        s->Freq += STEP;
        TotFreq += STEP;

        if (TotFreq > MAX_FREQ)
            Normalize();

        /* Keep approx sorted */
        if (s[0].Freq > s[-1].Freq) {
            SymFreqs t = s[0];
            s[0] = s[-1];
            s[-1] = t;
        }
    }

    uint16_t DecodeSymbol(RangeCoder *rc) {
        SymFreqs* s = F;
        uint32_t freq = rc->GetFreq(TotFreq);
        uint32_t AccFreq;

        for (AccFreq = 0; (AccFreq += s->Freq) <= freq; s++)
            _mm_prefetch((const char *)s, _MM_HINT_T0);

        AccFreq -= s->Freq;

        rc->Decode(AccFreq, s->Freq, TotFreq);
        s->Freq += STEP;
        TotFreq += STEP;

        if (TotFreq > MAX_FREQ)
            Normalize();

        /* Keep approx sorted */
        if (s[0].Freq > s[-1].Freq) {
            SymFreqs t = s[0];
            s[0] = s[-1];
            s[-1] = t;
            return t.Symbol;
        }

        return s->Symbol;
    }

public:
    uint32_t TotFreq;  // Total frequency

    // Array of Symbols approximately sorted by Freq.
    SymFreqs sentinel, F[NSYM+1];
};


}

#endif /* FREQUENCY_MODEL_H_ */
