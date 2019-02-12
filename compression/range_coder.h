#ifndef RANGE_CODER_H_
#define RANGE_CODER_H_

#include <cstdlib>

/*
 * Note it is up to the calling code to ensure that no overruns on input and
 * output buffers occur.
 *
 * Call the input() and output() functions to set and query the current
 * buffer locations.
 */

#define  DO(n) for (int _=0; _<n; _++)
#define  TOP   (1<<24)

namespace pil {

static unsigned short div_lookup[256] = {
    0, 65535, 32768, 21845, 16384, 13107, 10922,  9362,
 8192,  7281,  6553,  5957,  5461,  5041,  4681,  4369,
 4096,  3855,  3640,  3449,  3276,  3120,  2978,  2849,
 2730,  2621,  2520,  2427,  2340,  2259,  2184,  2114,
 2048,  1985,  1927,  1872,  1820,  1771,  1724,  1680,
 1638,  1598,  1560,  1524,  1489,  1456,  1424,  1394,
 1365,  1337,  1310,  1285,  1260,  1236,  1213,  1191,
 1170,  1149,  1129,  1110,  1092,  1074,  1057,  1040,
 1024,  1008,   992,   978,   963,   949,   936,   923,
  910,   897,   885,   873,   862,   851,   840,   829,
  819,   809,   799,   789,   780,   771,   762,   753,
  744,   736,   728,   720,   712,   704,   697,   689,
  682,   675,   668,   661,   655,   648,   642,   636,
  630,   624,   618,   612,   606,   601,   595,   590,
  585,   579,   574,   569,   564,   560,   555,   550,
  546,   541,   537,   532,   528,   524,   520,   516,
  512,   508,   504,   500,   496,   492,   489,   485,
  481,   478,   474,   471,   468,   464,   461,   458,
  455,   451,   448,   445,   442,   439,   436,   434,
  431,   428,   425,   422,   420,   417,   414,   412,
  409,   407,   404,   402,   399,   397,   394,   392,
  390,   387,   385,   383,   381,   378,   376,   374,
  372,   370,   368,   366,   364,   362,   360,   358,
  356,   354,   352,   350,   348,   346,   344,   343,
  341,   339,   337,   336,   334,   332,   330,   329,
  327,   326,   324,   322,   321,   319,   318,   316,
  315,   313,   312,   310,   309,   307,   306,   304,
  303,   302,   300,   299,   297,   296,   295,   293,
  292,   291,   289,   288,   287,   286,   284,   283,
  282,   281,   280,   278,   277,   276,   275,   274,
  273,   271,   270,   269,   268,   267,   266,   265,
  264,   263,   262,   261,   260,   259,   258,   257
};

#if 1
class RangeCoder {
private:
 uint64_t low;
 uint32_t range;
 uint32_t code;
 uint32_t cl;

public:
    char *in_buf;
    char *out_buf;

    void input (char *in)  { out_buf = in_buf = in;  }
    void output(char *out) { in_buf = out_buf = out; }
    char *input(void)  {return in_buf;}
    char *output(void) {return out_buf;}
    int size_out(void) {return out_buf - in_buf;}
    int size_in(void)  {return in_buf - out_buf;}

    void StartEncode( void ) {
        low   = 0;
        range = (uint32_t) - 1;
    }

    void StartDecode( void ) {
        uint8_t c;
        cl  = 0;
        range = (uint32_t)-1;
        DO(8) c = *in_buf++, code = (code<<8) | c, cl = (cl<<1) + (c==0xFF);
    }

    void FinishEncode( void ) {
        DO(8) *out_buf++ = low>>56, low<<=8;
    }

    void FinishDecode( void ) {}

    void Encode (uint32_t cumFreq, uint32_t freq, uint32_t totFreq) {
        range /= totFreq;
        low  += cumFreq * range;
        range*= freq;

        if( range<0x01000000 ) {
            do {
                *out_buf++ = uint8_t(low>>56);
                if( range<0x00010000 ) {
                    *out_buf++ = uint8_t(low>>48);
                    if( range<0x00000100 ) {
                        *out_buf++ = uint8_t(low>>40);
                        range<<=24, low<<=24;
                    } else range<<=16, low<<=16;
                } else range<<=8, low<<=8;
            } while( (int(low>>32)==-1) && (range=0xFF,1) );
        }
    }

    void Encode256 (uint32_t cumFreq, uint32_t freq, uint32_t totFreq) {
        range = ((uint64_t)range * div_lookup[totFreq]) >> 16;
        low  += cumFreq * range;
        range*= freq;

        if( range<0x01000000 ) {
            do {
                *out_buf++ = uint8_t(low>>56);
                if( range<0x00010000 ) {
                    *out_buf++ = uint8_t(low>>48);
                    if( range<0x00000100 ) {
                        *out_buf++ = uint8_t(low>>40);
                        range<<=24, low<<=24;
                    } else range<<=16, low<<=16;
                } else range<<=8, low<<=8;
            } while( (int(low>>32)==-1) && (range=0xFF,1) );
        }
    }

    uint32_t GetFreq (uint32_t totFreq) { return code/(range/=totFreq); }

    uint32_t GetFreq256 (uint32_t totFreq) {
        range = ((uint64_t)range * div_lookup[totFreq]) >> 16;
        return code / range;
    }

    void Decode (uint32_t cumFreq, uint32_t freq, uint32_t totFreq) {
        code -= cumFreq * range;
        range*= freq;

        if( range<0x01000000 ) {
            uint8_t c;
            do {
                code=(code<<8)|(c=*in_buf++),cl=(cl<<1)+((c+1U)>>8);
                if( range<0x00010000 ) {
                    code=(code<<8)|(c=*in_buf++),cl=(cl<<1)+((c+1U)>>8);
                    if( range<0x00000100 ) {
                        code=(code<<8)|(c=*in_buf++),cl=(cl<<1)+((c+1U)>>8);
                        range<<=24;
                    } else range<<=16;
                } else range<<=8;
            } while( (uint8_t(cl)>=0xF0) && (range=0xFF,1) );
        }

    }

};
# else
class RangeCoder
{
    uint64_t low;
    uint  range, code;

public:

 uint8_t *in_buf;
 uint8_t *out_buf;

 void input (char *in)  { out_buf = in_buf = (uint8_t*)in;  }
 void output(char *out) { in_buf = out_buf = (uint8_t*)out; }
 char *input(void)  {return (char *)in_buf;}
 char *output(void) {return (char *)out_buf;}
 int size_out(void) {return out_buf - in_buf;}
 int size_in(void)  {return in_buf - out_buf;}

 void StartEncode ( void )
 {
   low=0;
   range=(uint)-1;
 }

 void StartDecode( void )
 {
   low=0;
   range=(uint)-1;
   DO(8) code = (code<<8) | *in_buf++;
 }

 void FinishEncode( void )
 {
     DO(8) (*out_buf++ = low>>56), low<<=8;
 }

 void FinishDecode( void ) {}

 void Encode (uint cumFreq, uint freq, uint totFreq)
 {
     low  += cumFreq * (range/= totFreq);
     range*= freq;

     if (cumFreq + freq > totFreq)
     abort();

     while( range<TOP ) {
     // range = 0x00ffffff..
     // low/high may be matching
     //       eg 88332211/88342211 (range 00010000)
     // or differing
     //       eg 88ff2211/89002211 (range 00010000)
     //
     // If the latter, we need to reduce range down
     // such that high=88ffffff.
     // Eg. top-1      == 00ffffff
     //     low|top-1  == 88ffffff
     //     ...-low    == 0000ddee
     if ( uint8_t((low^(low+range))>>56) )
         range = ((uint(low)|(TOP-1))-uint(low));
     *out_buf++ = low>>56, range<<=8, low<<=8;
     }
 }

 uint GetFreq (uint totFreq) {
     return code/(range/=totFreq);
 }

 void Decode (uint cumFreq, uint freq, uint totFreq)
 {
   uint temp = cumFreq*range;
   low  += temp;
   code -= temp;
   range*= freq;

   while( range<TOP ) {
       if ( uint8_t((low^(low+range))>>56) )
       range = ((uint(low)|(TOP-1))-uint(low));
       code = (code<<8) | *in_buf++, range<<=8, low<<=8;
   }
 }

};
#endif

}

#endif /* RANGE_CODER_H_ */
