#include "pil.h"
#include "memory_pool.h"
#include "table.h"

#include <fstream>
#include <iostream>
#include <sstream>

// test
#include "table_test.h"
#include "buffer_builder_test.h"
#include "column_store.h"
#include "column_store_test.h"
#include "transform/transformer_test.h"
#include "record_builder_test.h"
#include "transform/dictionary_builder_test.h"
#include "table_meta_test.h"
#include "transform/compressor_test.h"

//
#include <random>
#include <chrono>

// temp
#include <immintrin.h>

#ifdef _mm_popcnt_u64
#define PIL_POPCOUNT   _mm_popcnt_u64
#else
#define PIL_POPCOUNT   __builtin_popcountll
#endif

__attribute__((always_inline))
static inline void PIL_POPCOUNT_SSE(uint64_t& a, const __m128i n) {
    a += PIL_POPCOUNT(_mm_cvtsi128_si64(n)) + PIL_POPCOUNT(_mm_cvtsi128_si64(_mm_unpackhi_epi64(n, n)));
}

#define PIL_POPCOUNT_AVX2(A, B) {                  \
    A += PIL_POPCOUNT(_mm256_extract_epi64(B, 0)); \
    A += PIL_POPCOUNT(_mm256_extract_epi64(B, 1)); \
    A += PIL_POPCOUNT(_mm256_extract_epi64(B, 2)); \
    A += PIL_POPCOUNT(_mm256_extract_epi64(B, 3)); \
}

uint32_t flag_stats_avx2_popcnt(const uint16_t* __restrict__ data, uint32_t n, uint32_t* __restrict__ flags) {
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    // 1 load data
    // 2 x | (((data[x] & mask[i]) >> i) << j)
    // 3 popcount
    __m256i masks[16];
    __m256i stubs[16];
    for(int i = 0; i < 16; ++i) {
        masks[i] = _mm256_set1_epi16(1 << i);
        stubs[i] = _mm256_set1_epi16(0);
    }

    uint32_t out_counters[16];
    memset(out_counters, 0, sizeof(uint32_t)*16);

    const __m256i* data_vectors = reinterpret_cast<const __m256i*>(data);
    const uint32_t n_cycles = n / 16;
    const uint32_t n_cycles_updates = n_cycles / 16;

#define UPDATE(idx, shift) stubs[idx] = _mm256_or_si256(stubs[idx], _mm256_slli_epi16(_mm256_srli_epi16(_mm256_and_si256(data_vectors[pos], masks[idx]),  idx), shift));
#define ITERATION(idx) {                                               \
        UPDATE(idx,0);  UPDATE(idx,1);  UPDATE(idx,2);  UPDATE(idx,3); \
        UPDATE(idx,4);  UPDATE(idx,5);  UPDATE(idx,6);  UPDATE(idx,7); \
        UPDATE(idx,8);  UPDATE(idx,9);  UPDATE(idx,10); UPDATE(idx,11);\
        UPDATE(idx,12); UPDATE(idx,13); UPDATE(idx,14); UPDATE(idx,15);\
        ++pos;                                                         \
}
#define BLOCK() {                                                  \
        ITERATION(0);  ITERATION(1);  ITERATION(2);  ITERATION(3); \
        ITERATION(4);  ITERATION(5);  ITERATION(6);  ITERATION(7); \
        ITERATION(8);  ITERATION(9);  ITERATION(10); ITERATION(11);\
        ITERATION(12); ITERATION(13); ITERATION(14); ITERATION(15);\
}

    uint32_t pos = 0;
    for(int i = 0; i < n_cycles_updates; ++i) {
        BLOCK() // unrolled

        /*
        // Not unrolled
        for(int c = 0; c < 16; ++c, ++pos) { // 16 iterations per register
            for(int j = 0; j < 16; ++j) { // each 1-hot per register
                UPDATE(j,c)
            }
        }
        */

        for(int j = 0; j < 16; ++j) {
            PIL_POPCOUNT_AVX2(out_counters[j], stubs[j])
            stubs[j] = _mm256_set1_epi16(0);
        }
    }

    // residual
    for(int i = pos*16; i < n; ++i) {
        for(int j = 0; j < 16; ++j) {
            out_counters[j] += ((data[i] & (1 << j)) >> j);
        }
    }

    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    auto time_span = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

    for(int i = 0; i < 16; ++i) flags[i] = out_counters[i];

    //std::cerr << "popcnt=";
    //for(int i = 0; i < 16; ++i) std::cerr << " " << out_counters[i];
    //std::cerr << std::endl;

#undef BLOCK
#undef ITERATION
#undef UPDATE

    return(time_span.count());
}

uint32_t flag_stats_scalar_naive(const uint16_t* __restrict__ data, uint32_t n, uint32_t* __restrict__ flags) {
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();

    memset(flags, 0, 16*sizeof(uint32_t));

    for(int i = 0; i < n; ++i) {
        for(int j = 0; j < 16; ++j) {
            flags[j] += ((data[i] & (1 << j)) >> j);
        }
    }

    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    auto time_span = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

    //std::cerr << "truth=";
    //for(int i = 0; i < 16; ++i) std::cerr << " " << flags[i];
    //std::cerr << std::endl;

    return(time_span.count());
}

uint32_t flag_stats_scalar_partition(const uint16_t* __restrict__ data, uint32_t n, uint32_t* __restrict__ flags) {
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();

    uint32_t low[256], high[256];
    memset(low,  0, 256*sizeof(uint32_t));
    memset(high, 0, 256*sizeof(uint32_t));

    for(int i = 0; i < n; ++i) {
        ++low[data[i] & 255];
        ++high[(data[i] >> 8) & 255];
    }

    for(int i = 0; i < 256; ++i) {
        for(int k = 0; k < 8; ++k) {
            flags[k] += ((i & (1 << k)) >> k) * low[i];
        }
    }

    for(int i = 0; i < 256; ++i) {
        for(int k = 0; k < 8; ++k) {
            flags[k+8] += ((i & (1 << k)) >> k) * high[i];
        }
    }

    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    auto time_span = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

    //std::cerr << "truth=";
    //for(int i = 0; i < 16; ++i) std::cerr << " " << flag_truth[i];
    //std::cerr << std::endl;

    return(time_span.count());
}

uint32_t flag_stats_avx2(const uint16_t* __restrict__ data, uint32_t n, uint32_t* __restrict__ flags) {
    // 1 load data
    // 2 data[x] & mask[i]
    // 3 data[x] == 0
    // 4 add count + (data[x] == 0)
    // 5: if mod val then popcount
    // _mm256_and_si256
    // _mm256_cmpeq_epi16()
    // _mm256_add_epi16 OR _mm256_slli_si256
    // _mm_popcnt_u64

    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();

    __m256i masks[16];
    __m256i counters[16];
    for(int i = 0; i < 16; ++i) {
        masks[i]    = _mm256_set1_epi16(1 << i);
        counters[i] = _mm256_set1_epi16(0);
    }
    uint32_t out_counters[16];
    memset(out_counters, 0, sizeof(uint32_t)*16);

    const __m256i hi_mask = _mm256_set1_epi32(0xFFFF0000);
    const __m256i lo_mask = _mm256_set1_epi32(0x0000FFFF);
    const __m256i* data_vectors = reinterpret_cast<const __m256i*>(data);
    const uint32_t n_cycles = n / 16;
    const uint32_t n_update_cycles = std::floor((double)n_cycles / 65536);
    //std::cerr << n << " values and " << n_cycles << " cycles " << n_residual << " residual cycles" << std::endl;

#define UPDATE(idx) counters[idx]  = _mm256_add_epi16(counters[idx],  _mm256_srli_epi16(_mm256_and_si256(data_vectors[pos], masks[idx]),  idx))
#define ITERATION  {                                   \
        UPDATE(0);  UPDATE(1);  UPDATE(2);  UPDATE(3); \
        UPDATE(4);  UPDATE(5);  UPDATE(6);  UPDATE(7); \
        UPDATE(8);  UPDATE(9);  UPDATE(10); UPDATE(11);\
        UPDATE(12); UPDATE(13); UPDATE(14); UPDATE(15);\
        ++pos; ++k;                                    \
}

    uint32_t pos = 0;
    for(int i = 0; i < n_update_cycles; ++i) { // each block of 2^16 values
        for(int k = 0; k < 65536; ) { // max sum of each 16-bit value in a register
            ITERATION // unrolled
        }

        // Compute vector sum
        for(int k = 0; k < 16; ++k) { // each flag register
            // Accumulator
            // ((16-bit high & 16 high) >> 16) + (16-bit low & 16-low)
            __m256i x = _mm256_add_epi32(
                           _mm256_srli_epi32(_mm256_and_si256(counters[k], hi_mask), 16),
                           _mm256_and_si256(counters[k], lo_mask));
            __m256i t1 = _mm256_hadd_epi32(x,x);
            __m256i t2 = _mm256_hadd_epi32(t1,t1);
            __m128i t4 = _mm_add_epi32(_mm256_castsi256_si128(t2),_mm256_extractf128_si256(t2,1));
            out_counters[k] += _mm_cvtsi128_si32(t4);

            /*
            // Naive counter
            uint16_t* d = reinterpret_cast<uint16_t*>(&counters[k]);
            for(int j = 0; j < 16; ++j) { // each uint16_t in the register
                out_counters[k] += d[j];
            }
            */

            counters[k] = _mm256_set1_epi16(0);
        }
    }

    // residual
    for(int i = pos*16; i < n; ++i) {
        for(int j = 0; j < 16; ++j) {
            out_counters[j] += ((data[i] & (1 << j)) >> j);
        }
    }

    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    auto time_span = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);

    for(int i = 0; i < 16; ++i) flags[i] = out_counters[i];

    //std::cerr << "simd=";
    //for(int i = 0; i < 16; ++i) std::cerr << " " << out_counters[i];
    //std::cerr << std::endl;

#undef ITERATION
#undef UPDATE

    return(time_span.count());

}

void flag_test(uint32_t n, uint32_t cycles = 1) {
    std::cerr << "Generating flags: " << n << std::endl;

    std::random_device rd; // obtain a random number from hardware
    std::mt19937 eng(rd()); // seed the generator

    //uint16_t* vals = new uint16_t[n];
    uint16_t* vals;
    assert(!posix_memalign((void**)&vals, 32, n*sizeof(uint16_t)));

    uint64_t times[5] = {0};
    uint64_t times_local[5];

    std::vector<uint32_t> ranges = {16, 64, 256, 512, 1024, 4096, 65536};
    for(int r = 0; r < ranges.size(); ++r) {
        std::uniform_int_distribution<uint16_t> distr(1, ranges[r]); // right inclusive

        for(int c = 0; c < cycles; ++c) {
            std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();

            for(uint32_t i = 0; i < n; ++i) {
                vals[i] = distr(eng);
            }
            std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
            auto time_span = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1);
            times[0] += time_span.count();
            times_local[0] = time_span.count();

            // start tests
            // scalar naive
            uint32_t flags[16];
            uint32_t time_naive = flag_stats_scalar_naive(vals, n, &flags[0]);
            times[1] += time_naive;
            times_local[1] = time_naive;

            // scalar partition
            uint32_t flags2[16]; memset(flags2, 0, sizeof(uint32_t)*16);
            uint32_t time_partition = flag_stats_scalar_partition(vals, n, &flags2[0]);
            times[2] += time_partition;
            times_local[2] = time_partition;

            // avx2 aggl
            memset(flags2, 0, sizeof(uint32_t)*16);
            uint32_t avx2_timing = flag_stats_avx2(vals, n, &flags2[0]);
            times[3] += avx2_timing;
            times_local[3] = avx2_timing;

            // avx2 popcnt
            memset(flags2, 0, sizeof(uint32_t)*16);
            uint32_t popcnt_timing = flag_stats_avx2_popcnt(vals, n, &flags2[0]);
            times[4] += popcnt_timing;
            times_local[4] = popcnt_timing;

            std::cerr << ranges[r] << "\t" << c << "\t" << times_local[0] << "\t" << times_local[1] << "\t" << times_local[2] << "\t" << times_local[3] << "\t" << times_local[4] << std::endl;
        }
        std::cerr << "average times=" << (double)times[0]/cycles << " " << (double)times[1]/cycles << " " << (double)times[2]/cycles << " " << (double)times[3]/cycles << " " << (double)times[4]/cycles << std::endl;
        memset(times, 0, sizeof(uint64_t)*5);
    }

    delete[] vals;
}

std::vector<std::string> inline StringSplit(const std::string &source, const char *delimiter = " ", bool keepEmpty = false)
{
    std::vector<std::string> results;

    size_t prev = 0;
    size_t next = 0;

    while ((next = source.find_first_of(delimiter, prev)) != std::string::npos)
    {
        if (keepEmpty || (next - prev != 0))
        {
            results.push_back(source.substr(prev, next - prev));
        }
        prev = next + 1;
    }

    if (prev < source.size())
    {
        results.push_back(source.substr(prev));
    }

    return results;
}

int google_test(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

int main(int argc, char **argv) {
    flag_test(100000000, 10);
    return(1);
    //

    // todo: fix tests in makefile
    int test_return = google_test(argc, argv);
    //std::cerr << "gtest returned=" << test_return << std::endl;
    if(test_return != 0) return(1);
    //

    pil::TableConstructor table;
    pil::RecordBuilder rbuild;
    uint64_t file_size = 0;

    // Set to 1 for FASTQ test
    if(0) {
        std::ifstream ss;
        //ss.open("/Users/Mivagallery/Desktop/ERR194146.fastq");
        //ss.open("/media/mdrk/NVMe/NA12878J_HiSeqX_R1_50mil.fastq", std::ios::ate | std::ios::in);
        //ss.open("/home/mk819/Downloads/NA12878J_HiSeqX_R1.40m.fastq", std::ios::ate | std::ios::in);
        ss.open("/home/mk819/Downloads/229b_ont.fq", std::ios::ate | std::ios::in);
        if(ss.good() == false){
            std::cerr << "not good: " << ss.badbit << std::endl;
            return 1;
        }
        file_size = ss.tellg();
        ss.seekg(0);

        //table.single_archive = true;
        //table.out_stream.open("/media/mdrk/NVMe/test.pil", std::ios::binary | std::ios::out);
        //table.out_stream.open("/Users/Mivagallery/Desktop/pil/test.pil", std::ios::binary | std::ios::out);
        table.out_stream.open("/home/mk819/Desktop/test.pil", std::ios::binary | std::ios::out);

        if(table.out_stream.good() == false) {
            std::cerr << "failed to open output file" << std::endl;
            return 1;
        }

        std::string line;
        uint32_t ltype = 0;

        //pil::BaseBitEncoder enc;

        std::vector<pil::PIL_COMPRESSION_TYPE> ctypes;
        ctypes.push_back(pil::PIL_COMPRESS_RC_QUAL);
        assert(table.SetField("QUAL", pil::PIL_TYPE_BYTE_ARRAY, pil::PIL_TYPE_UINT8, ctypes) == 1);

        ctypes.clear();
        ctypes.push_back(pil::PIL_COMPRESS_RC_BASES);
        //ctypes.push_back(pil::PIL_ENCODE_BASES_2BIT);
        assert(table.SetField("BASES", pil::PIL_TYPE_BYTE_ARRAY, pil::PIL_TYPE_UINT8, ctypes) == 1);

        //ctypes.clear();
        //ctypes.push_back(pil::PIL_ENCODE_DICT);
        //ctypes.push_back(pil::PIL_COMPRESS_ZSTD);
        //ctypes.push_back(pil::PIL_COMPRESS_RANS1);
        //table.SetField("NAME-7", pil::PIL_TYPE_UINT32, ctypes);
        //table.SetField("NAME-8", pil::PIL_TYPE_UINT32, ctypes);
        //table.SetField("NAME-9", pil::PIL_TYPE_UINT32, ctypes);

        //ctypes.clear();
        //ctypes.push_back(pil::PIL_ENCODE_DELTA);
        //ctypes.push_back(pil::PIL_COMPRESS_ZSTD);
        //table.SetField("NAME-2", pil::PIL_TYPE_UINT32, ctypes);

        //table.single_archive = false;

        // We control wether we create a Tensor-model or Column-split-model ColumnStore
        // by using either Add (split-model) or AddArray (Tensor-model).
        while(std::getline(ss, line)){
            //std::cerr << (int)ltype << " " << line << std::endl;
            if(ltype == 0) {
                // @ERR194146.812444544 HSQ1008:141:D0CC8ACXX:3:2204:14148:71629/1


#define never 0
#if never == 1
                // 1: Split by space
                // 2: Split first by .
                // 3: Split second by :
                // 4: Split second last by /
                std::vector<std::string> left_right = StringSplit(line, " ", false);
                std::vector<std::string> left  = StringSplit(left_right[0], ".", false);
                std::vector<std::string> right = StringSplit(left_right[1], ":", false);
                std::vector<std::string> last  = StringSplit(right.back(), "/", false);
                // @ERR194146, 812444541
                // HSQ1008, 141, D0CC8ACXX, 2, 1204, 13288, 78171/2
                // 78171 and 2

                /*
                std::cerr << left_right[0] << ", " << left_right[1] << std::endl;
                std::cerr << left[0] << ", " << left[1] << std::endl;
                std::cerr << right[0];
                for(int i = 1; i < right.size(); ++i) {
                    std::cerr << ", " << right[i];
                }
                std::cerr << std::endl;
                std::cerr << last[0] << " and " << last[1] << std::endl;
                */

                rbuild.AddArray<uint8_t>("NAME-1", pil::PIL_TYPE_UINT8, reinterpret_cast<uint8_t*>(&left[0]), left[0].size());

                uint32_t name2 = std::atoi(left[1].c_str());
                rbuild.Add<uint32_t>("NAME-2", pil::PIL_TYPE_UINT32, name2);

                rbuild.AddArray<uint8_t>("NAME-3", pil::PIL_TYPE_UINT8, reinterpret_cast<uint8_t*>(&right[0][0]), right[0].size());

                uint8_t name4 = std::atoi(right[1].c_str());
                rbuild.Add<uint8_t>("NAME-4", pil::PIL_TYPE_UINT8, name4);

                rbuild.AddArray<uint8_t>("NAME-5", pil::PIL_TYPE_UINT8, reinterpret_cast<uint8_t*>(&right[2][0]), right[2].size());

                uint8_t name5 = std::atoi(right[3].c_str());
                rbuild.Add<uint32_t>("NAME-6", pil::PIL_TYPE_UINT32, name5);

                uint32_t name6 = std::atoi(right[4].c_str());
                rbuild.Add<uint32_t>("NAME-7", pil::PIL_TYPE_UINT32, name6);

                uint32_t name7 = std::atoi(right[5].c_str());
                rbuild.Add<uint32_t>("NAME-8", pil::PIL_TYPE_UINT32, name7);

                uint32_t name8 = std::atoi(last[0].c_str());
                rbuild.Add<uint32_t>("NAME-9", pil::PIL_TYPE_UINT32, name8);

                uint8_t name9 = std::atoi(last[1].c_str());
                rbuild.Add<uint8_t>("NAME-10", pil::PIL_TYPE_UINT8, name9);
#else
                // @ST-E00106:108:H03M0ALXX:1:1101:2248:1238
                /*
                std::vector<std::string> parts = StringSplit(line, ":", false);

                rbuild.AddArray<uint8_t>("NAME-1", pil::PIL_TYPE_UINT8, reinterpret_cast<uint8_t*>(&parts[0]), parts[0].size());
                uint32_t name = std::atoi(parts[1].c_str());
                rbuild.Add<uint32_t>("NAME-2", pil::PIL_TYPE_UINT32, name);
                rbuild.AddArray<uint8_t>("NAME-3", pil::PIL_TYPE_UINT8, reinterpret_cast<uint8_t*>(&parts[2]), parts[2].size());
                for(int k = 4; k < 8; ++k) {
                    name = std::atoi(parts[k - 1].c_str());
                    rbuild.Add<uint32_t>("NAME-" + std::to_string(k), pil::PIL_TYPE_UINT32, name);
                }
                */
#endif
            }

            if(ltype == 1) {
                assert(line.size() > 0);
                rbuild.AddArray<uint8_t>("BASES", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(line.data()), line.size());
            }

            if(ltype == 3) {
                assert(line.size() > 0);
                rbuild.AddArray<uint8_t>("QUAL", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(line.data()), line.size());
            }

            ++ltype;
            if(ltype == 4){
                table.Append(rbuild);
                ltype = 0;
            }
        }
        table.Finalize();
        table.Describe(std::cerr);
    }

    // Set to 1 for SAM test
    if(1) {
        std::ifstream ss;
        //ss.open("/Users/Mivagallery/Desktop/ERR194146.fastq", std::ios::ate | std::ios::in);
        //ss.open("/Users/Mivagallery/Downloads/NA12877_S1_10m.sam", std::ios::ate | std::ios::in);
        //ss.open("/media/mdrk/NVMe/NA12886_S1_10m_complete.sam", std::ios::ate | std::ios::in);
        //ss.open("/media/mdrk/NVMe/NA12878J_HiSeqX_R1_50mil.fastq.sam", std::ios::ate | std::ios::in);
        ss.open("/media/mdrk/NVMe/NA12878J_HiSeqX_R1_50mil.fastq.aligned.sam", std::ios::ate | std::ios::in);
        //ss.open("/media/mdrk/08dcb478-5359-41f4-97c8-469190c8a034/NA12878/nanopore/rel5-guppy-0.3.0-chunk10k.sorted.sam", std::ios::ate | std::ios::in);
        //ss.open("/home/mk819/Downloads/NA12878J_HiSeqX_R1.40m.fastq.sam", std::ios::ate | std::ios::in);
        //ss.open("/home/mk819/Downloads/ont_bwa_Cd630_62793_sort.sam", std::ios::ate | std::ios::in);


        if(ss.good() == false) {
            std::cerr << "not good: " << ss.badbit << std::endl;
            return 1;
        }
        file_size = ss.tellg();
        ss.seekg(0);

        //table.single_archive = true;
        //table.batch_size = 65536;
        table.out_stream.open("/media/mdrk/NVMe/test.pil", std::ios::binary | std::ios::out);
        //table.out_stream.open("/Users/Mivagallery/Desktop/test.pil", std::ios::binary | std::ios::out);
        //table.out_stream.open("/home/mk819/Desktop/test.pil", std::ios::binary | std::ios::out);

        if(table.out_stream.good() == false) {
            std::cerr << "failed to open output file" << std::endl;
            return 1;
        }

        std::string line;

        //pil::DictionaryEncoder enc;

        std::vector<pil::PIL_COMPRESSION_TYPE> ctypes;
        ctypes.push_back(pil::PIL_COMPRESS_RC_QUAL);
        table.SetField("QUAL", pil::PIL_TYPE_BYTE_ARRAY, pil::PIL_TYPE_UINT8, ctypes);

        ctypes.clear();
        ctypes.push_back(pil::PIL_COMPRESS_RC_BASES);
        //ctypes.push_back(pil::PIL_ENCODE_BASES_2BIT);
        table.SetField("BASES", pil::PIL_TYPE_BYTE_ARRAY, pil::PIL_TYPE_UINT8, ctypes);

        //ctypes.clear();
        //ctypes.push_back(pil::PIL_ENCODE_DICT);
        //ctypes.push_back(pil::PIL_COMPRESS_ZSTD);
        //table.SetField("RNAME", pil::PIL_TYPE_UINT32, ctypes);

        //ctypes.clear();
        //ctypes.push_back(pil::PIL_ENCODE_CIGAR_NIBBLE);
        //ctypes.push_back(pil::PIL_COMPRESS_ZSTD);
        //table.SetField("CIGAR", pil::PIL_TYPE_BYTE_ARRAY, pil::PIL_TYPE_UINT8, ctypes);

        //ctypes.clear();
        //ctypes.push_back(pil::PIL_ENCODE_DICT);
        //ctypes.push_back(pil::PIL_COMPRESS_ZSTD);
        //table.SetField("POS", pil::PIL_TYPE_UINT32, ctypes);

        std::unordered_map<std::string, uint32_t> RNAME_map;
        std::vector<std::string> RNAME_dict;

        // We control wether we create a Tensor-model or Column-split-model ColumnStore
        // by using either Add (split-model) or AddArray (Tensor-model).
        while(std::getline(ss, line)){
            if(line[0] == '@') continue;
            //std::cerr << line << std::endl;

            std::stringstream ss(line);
            std::string s;
            uint32_t l = 0;
            while (std::getline(ss, s, '\t')) {
                if(l == 0) {

                    std::stringstream ss2(s);
                    std::string s2;

                    std::vector<std::string> tk2;
#define nanopore 0
#if nanopore != 1
                    while (std::getline(ss2, s2, ':')) {
                        //std::cerr << s2 << ", ";
                        tk2.push_back(s2);
                    }


                   // std::cerr << std::endl;
                    //std::cerr << "tk2: " << tk2.size() << std::endl;
                    rbuild.AddArray<uint8_t>("NAME-1", pil::PIL_TYPE_UINT8, reinterpret_cast<uint8_t*>(&tk2[0][0]), tk2[0].size());
                    uint32_t name2 = std::atoi(tk2[1].data());
                    rbuild.Add<uint32_t>("NAME-2", pil::PIL_TYPE_UINT32, name2);
                    rbuild.AddArray<uint8_t>("NAME-3", pil::PIL_TYPE_UINT8, reinterpret_cast<uint8_t*>(&tk2[2][0]), tk2[2].size());
                    uint32_t name4 = std::atoi(tk2[3].data());
                    rbuild.Add<uint32_t>("NAME-4", pil::PIL_TYPE_UINT32, name4);
                    uint32_t name5 = std::atoi(tk2[4].data());
                    rbuild.Add<uint32_t>("NAME-5", pil::PIL_TYPE_UINT32, name5);
                    uint32_t name6 = std::atoi(tk2[5].data());
                    rbuild.Add<uint32_t>("NAME-6", pil::PIL_TYPE_UINT32, name6);
                    uint32_t name7 = std::atoi(tk2[6].data());
                    rbuild.Add<uint32_t>("NAME-7", pil::PIL_TYPE_UINT32, name7);

                    //rbuild.AddArray<uint8_t>("NAME", pil::PIL_TYPE_UINT8, reinterpret_cast<uint8_t*>(&line[0]), line.size());
#else
                    while (std::getline(ss2, s2, '-')) {
                        //std::cerr << s2 << ", ";
                        tk2.push_back(s2);
                    }
                    for(int k = 0; k < tk2.size(); ++k) {
                        rbuild.AddArray<uint8_t>("NAME-" + std::to_string(k), pil::PIL_TYPE_UINT8, reinterpret_cast<uint8_t*>(&tk2[k][0]), tk2[k].size());
                    }
#endif
                }

                if(l == 1) {
                    uint16_t flag = std::atoi(s.data());
                    rbuild.Add<uint16_t>("FLAG", pil::PIL_TYPE_UINT16, flag);
                }

                if(l == 2) {
                    uint32_t rname = 0;
                    auto rname_hit = RNAME_map.find(s);
                    if(rname_hit == RNAME_map.end()) {
                       RNAME_map[s] = RNAME_dict.size();
                       rname = RNAME_dict.size();
                       RNAME_dict.push_back(s);
                    } else rname = rname_hit->second;

                    rbuild.Add<uint32_t>("RNAME", pil::PIL_TYPE_UINT32, rname);
                }

                if(l == 3) {
                   uint32_t pos = std::atoi(s.data());
                   rbuild.Add<uint32_t>("POS", pil::PIL_TYPE_UINT32, pos);
               }

                if(l == 4) {
                   uint8_t mapq = std::atoi(s.data());
                   rbuild.Add<uint8_t>("MAPQ", pil::PIL_TYPE_UINT8, mapq);
               }

                if(l == 5) {
                   rbuild.AddArray<uint8_t>("CIGAR", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(s.data()), s.size());
               }

                if(l == 6) {
                    uint32_t rnext = 0;
                    auto rnext_hit = RNAME_map.find(s);
                    if(rnext_hit == RNAME_map.end()) {
                       RNAME_map[s] = RNAME_dict.size();
                       rnext = RNAME_dict.size();
                       RNAME_dict.push_back(s);
                    } else rnext = rnext_hit->second;
                    rbuild.Add<uint32_t>("RNEXT", pil::PIL_TYPE_UINT32, rnext);
                   //rbuild.AddArray<uint8_t>("RNEXT", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(s.data()), s.size());
               }

                if(l == 7) {
                   int32_t pnext = std::atoi(s.data());
                   rbuild.Add<int32_t>("PNEXT", pil::PIL_TYPE_INT32, pnext);
               }

                if(l == 8) {
                   int32_t tlen = std::atoi(s.data());
                   rbuild.Add<int32_t>("TLEN", pil::PIL_TYPE_INT32, tlen);
               }

                if(l == 9) {
                    //std::cout << s << std::endl;
                    //int rec = enc.Encode(reinterpret_cast<const uint8_t*>(line.data()), line.size());
                    rbuild.AddArray<uint8_t>("BASES", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(s.data()), s.size());
                    //rbuild.AddArray<uint8_t>("BASES", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(enc.data()->mutable_data()), rec);
                }

                if(l == 10) {
                    //std::cout << s << std::endl;
                    rbuild.AddArray<uint8_t>("QUAL", pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(s.data()), s.size());
                    //std::cerr << line << std::endl;
                }

                if(l >= 11) {
                    // Tags in SAM is stored AS NN:N:<data>
                    // we will store only

                   // std::cerr << "token: " << s << std::endl;
                   const char* end_pos = s.data() + s.size();
                   size_t cur_pos = 0;
                   const char* position = std::find(s.data() + cur_pos, end_pos, ':');
                   uint32_t n_found = 0;
                   std::string field_name;
                   char field_type;
                   const char* start_data;
                   while(true) {
                       if(position == end_pos || n_found == 2) break;
                       if(n_found == 0) {
                           field_name = std::string(s.data(), position - s.data());
                           //std::cerr << "field_name=" << field_name << std::endl;
                       } else if(n_found == 1) {
                           field_type = s.data()[position - s.data() + 1];
                           //std::cerr << "field_type=" << field_type << std::endl;
                           start_data = position + 3;
                       }
                       ++n_found;
                   }

                    //std::cerr << std::endl;
                    if(field_type == 'i') {
                        //std::cerr << "is integer type" << std::endl;
                        uint32_t val = std::atoi(start_data);
                        rbuild.Add<uint32_t>(field_name, pil::PIL_TYPE_UINT32, val);
                    } else {
                        //std::cerr << "length of data =" << (s.data() + s.length()) - start_data << std::endl;
                        uint32_t l_data = (s.data() + s.length()) - start_data;
                       // std::cerr << std::string(start_data, l_data) << std::endl;
                        // const std::string& id, PIL_PRIMITIVE_TYPE ptype, const T* value, uint32_t n_values
                        rbuild.AddArray<uint8_t>(field_name, pil::PIL_TYPE_UINT8, reinterpret_cast<const uint8_t*>(start_data), l_data);
                    }
                }

                ++l;
            }
            table.Append(rbuild);
        }

        table.Finalize();
        table.Describe(std::cerr);
        std::cerr << "recordbuilder: " << rbuild.n_added << " and slots=" << rbuild.slots.size() << std::endl;
    }

    // Set to 1 for VCF test
    if(0) {
        std::ifstream ss;
        //ss.open("/Users/Mivagallery/Desktop/ERR194146.fastq");
        //ss.open("/media/mdrk/NVMe/NA12886_S1_10m_complete.sam", std::ios::ate | std::ios::in);
         ss.open("/media/mdrk/NVMe/1kgp3_chr20_50k.vcf", std::ios::ate | std::ios::in);
        //ss.open("/home/mk819/Downloads/ont_bwa_Cd630_62793_sort.sam", std::ios::ate | std::ios::in);

        if(ss.good() == false){
            std::cerr << "not good: " << ss.badbit << std::endl;
            return 1;
        }
        file_size = ss.tellg();
        ss.seekg(0);

        //table.out_stream.open("/home/mk819/Desktop/test.pil", std::ios::binary | std::ios::out);
        table.out_stream.open("/media/mdrk/NVMe/test.pil", std::ios::binary | std::ios::out);
        //table.out_stream.open("/Users/Mivagallery/Desktop/test.pil", std::ios::binary | std::ios::out);
        if(table.out_stream.good() == false) {
            std::cerr << "failed to open output file" << std::endl;
            return 1;
        }

        std::string line;

        //pil::DictionaryEncoder enc;
        // std::vector<pil::PIL_COMPRESSION_TYPE> ctypes;

        std::unordered_map<std::string, uint32_t> RNAME_map;
        std::vector<std::string> RNAME_dict;

        std::unordered_map<std::string, uint32_t> FILTER_map;
        std::vector<std::string> FILTER_dict;

        std::unordered_map<std::string, uint32_t> GT_map;
        std::vector<std::string> GT_dict;

        // We control wether we create a Tensor-model or Column-split-model ColumnStore
        // by using either Add (split-model) or AddArray (Tensor-model).
        /*
         * Test file tokens looks like this:
         * 0: 20
         * 1: 62783
         * 2: rs189195684
         * 3: A
         * 4: G
         * 5: 100
         * 6: PASS
         * 7: AC=3;AF=0.000599042;AN=5008;NS=2504;DP=16623;EAS_AF=0;AMR_AF=0;AFR_AF=0.0023;EUR_AF=0;SAS_AF=0;AA=.|||
         * 8: GT
         * 9: 0|0
         * ...
         * 2512: 0|0
         */
        while(std::getline(ss, line)) {
            if(line[0] == '#') continue;
            //std::cerr << line << std::endl;

            std::stringstream ss(line);
            std::string s;
            uint32_t l = 0;
            uint32_t sample = 0;
            while (std::getline(ss, s, '\t')) {
                //std::cerr << l << ": " << s << std::endl;
                if(l == 0) {
                    uint32_t rname = 0;
                    auto rname_hit = RNAME_map.find(s);
                    if(rname_hit == RNAME_map.end()) {
                       RNAME_map[s] = RNAME_dict.size();
                       rname = RNAME_dict.size();
                       RNAME_dict.push_back(s);
                    } else rname = rname_hit->second;

                    rbuild.Add<uint32_t>("RNAME", pil::PIL_TYPE_UINT32, rname);
                }
                if(l == 1) {
                    uint32_t pos = std::atoi(s.data());
                    rbuild.Add<uint32_t>("POS", pil::PIL_TYPE_UINT32, pos);
                }
                if(l == 2) {
                    rbuild.AddArray<uint8_t>("NAME", pil::PIL_TYPE_UINT8, reinterpret_cast<uint8_t*>(&s[0]), s.size());
                }
                if(l == 3) {
                    rbuild.AddArray<uint8_t>("REF", pil::PIL_TYPE_UINT8, reinterpret_cast<uint8_t*>(&s[0]), s.size());
                }
                if(l == 4) {
                    rbuild.AddArray<uint8_t>("ALT", pil::PIL_TYPE_UINT8, reinterpret_cast<uint8_t*>(&s[0]), s.size());
                }
                if(l == 5) {
                    uint32_t qual = std::atof(s.data());
                    rbuild.Add<float>("QUAL", pil::PIL_TYPE_FLOAT, qual);
                }
                if(l == 6) {
                    uint32_t filter = 0;
                    auto filter_hit = FILTER_map.find(s);
                    if(filter_hit == FILTER_map.end()) {
                       FILTER_map[s] = FILTER_dict.size();
                       filter = FILTER_dict.size();
                       FILTER_dict.push_back(s);
                    } else filter = filter_hit->second;

                    rbuild.Add<uint32_t>("FILTER", pil::PIL_TYPE_UINT32, filter);
                }

                if(l > 8) {
                    std::stringstream ss2(s);
                    std::string s2;
                    std::vector<std::string> tk2;
                    while (std::getline(ss2, s2, '|')) {
                        //std::cerr << s2 << " and ";
                        tk2.push_back(s2);
                    }

                    if(l == 9) std::cerr << (int)std::atoi(tk2[0].data());
                    rbuild.Add<uint8_t>("GT-" + std::to_string(sample), pil::PIL_TYPE_UINT8, (uint8_t)std::atoi(tk2[0].data()));
                    rbuild.Add<uint8_t>("GT-" + std::to_string(sample + 1), pil::PIL_TYPE_UINT8, (uint8_t)std::atoi(tk2[1].data()));
                    sample += 2;
                }

                ++l;
            }
            table.Append(rbuild);
        }

        table.Finalize();
        table.Describe(std::cerr);
    }

    std::cerr << "memory in: " << table.c_in << "(actual=" << file_size << ")" << " memory out: " << table.c_out << " -> " << (float)table.c_in / table.c_out << std::endl;

    return(0);
}
