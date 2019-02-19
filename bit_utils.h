#ifndef BIT_UTILS_H_
#define BIT_UTILS_H_

#include <cstdint>

#if defined(__GNUC__)
#define PIL_PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define PIL_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#define PIL_NORETURN __attribute__((noreturn))
#define PIL_PREFETCH(addr) __builtin_prefetch(addr)
#elif defined(_MSC_VER)
#define PIL_NORETURN __declspec(noreturn)
#define PIL_PREDICT_FALSE(x) x
#define PIL_PREDICT_TRUE(x) x
#define PIL_PREFETCH(addr)
#else
#define PIL_NORETURN
#define PIL_PREDICT_FALSE(x) x
#define PIL_PREDICT_TRUE(x) x
#define PIL_PREFETCH(addr)
#endif

namespace pil {

namespace BitUtils {

// Returns the ceil of value/divisor
constexpr int64_t CeilDiv(int64_t value, int64_t divisor) {
    return value / divisor + (value % divisor != 0);
}

constexpr int64_t BytesForBits(int64_t bits) { return (bits + 7) >> 3; }

// Returns the smallest power of two that contains v. If v is already a
// power of two, it is returned as is.
static inline int64_t NextPower2(int64_t n) {
    // Taken from
    // http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    n++;
    return n;
}

constexpr bool IsMultipleOf64(int64_t n) { return (n & 63) == 0; }

constexpr bool IsMultipleOf8(int64_t n) { return (n & 7) == 0; }

// Returns 'value' rounded up to the nearest multiple of 'factor'
constexpr int64_t RoundUp(int64_t value, int64_t factor) {
    return (value + (factor - 1)) / factor * factor;
}

// Returns 'value' rounded up to the nearest multiple of 'factor' when factor
// is a power of two.
// The result is undefined on overflow, i.e. if `value > 2**64 - factor`,
// since we cannot return the correct result which would be 2**64.
constexpr int64_t RoundUpToPowerOf2(int64_t value, int64_t factor) {
    // DCHECK((factor > 0) && ((factor & (factor - 1)) == 0));
    //assert((factor > 0) && ((factor & (factor - 1)) == 0));
    return (value + (factor - 1)) & ~(factor - 1);
}

constexpr int64_t RoundUpToMultipleOf8(int64_t num) { return RoundUpToPowerOf2(num, 8); }

constexpr int64_t RoundUpToMultipleOf64(int64_t num) {
    return RoundUpToPowerOf2(num, 64);
}

// Returns the 'num_bits' least-significant bits of 'v'.
static inline uint64_t TrailingBits(uint64_t v, int num_bits) {
    if (PIL_PREDICT_FALSE(num_bits == 0)) return 0;
    if (PIL_PREDICT_FALSE(num_bits >= 64)) return v;
    int n = 64 - num_bits;
    return (v << n) >> n;
}

/// \brief Count the number of leading zeros in an unsigned integer.
static inline int CountLeadingZeros(uint32_t value) {
    #if defined(__clang__) || defined(__GNUC__)
    if (value == 0) return 32;
    return static_cast<int>(__builtin_clz(value));
    #elif defined(_MSC_VER)
        unsigned long index;                                               // NOLINT
    if (_BitScanReverse(&index, static_cast<unsigned long>(value))) {  // NOLINT
        return 31 - static_cast<int>(index);
    } else {
        return 32;
    }
    #else
    int bitpos = 0;
    while (value != 0) {
        value >>= 1;
        ++bitpos;
    }
    return 32 - bitpos;
    #endif
}

static inline int CountLeadingZeros(uint64_t value) {
    #if defined(__clang__) || defined(__GNUC__)
    if (value == 0) return 64;
    return static_cast<int>(__builtin_clzll(value));
    #elif defined(_MSC_VER)
    unsigned long index;                     // NOLINT
    if (_BitScanReverse64(&index, value)) {  // NOLINT
        return 63 - static_cast<int>(index);
    } else {
        return 64;
    }
    #else
    int bitpos = 0;
    while (value != 0) {
        value >>= 1;
        ++bitpos;
    }
    return 64 - bitpos;
    #endif
}

// Returns the minimum number of bits needed to represent an unsigned value
static inline int NumRequiredBits(uint64_t x) { return 64 - CountLeadingZeros(x); }

// Returns ceil(log2(x)).
static inline int Log2(uint64_t x) {
    // DCHECK_GT(x, 0);
    assert(x > 0);
    return NumRequiredBits(x - 1);
}

}

}



#endif /* BIT_UTILS_H_ */
