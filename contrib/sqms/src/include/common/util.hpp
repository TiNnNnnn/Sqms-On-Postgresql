#ifndef UTILS_UTIL_H_
#define UTILS_UTIL_H_
#include <stdint.h>
#include <vector>
#include <string>

//单位是ms
uint64_t GetCurrentTime();
void GetCurrentTimeString(std::string&output);
int64_t GetCurrentTid();
int64_t GetCurrentPid();
bool CheckLittleEndian();
uint32_t DecodeFixed32(const char* ptr);
uint32_t SimMurMurHash(const char *data, uint32_t len);
int64_t Next2Power(uint64_t value);

// MurmurHash3 32位实现
uint32_t murmurhash3_32(const void* key, size_t len, uint32_t seed) {
    const uint8_t* data = static_cast<const uint8_t*>(key);
    const int nblocks = len / 4;

    uint32_t h1 = seed;
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    // Body
    const uint32_t* blocks = reinterpret_cast<const uint32_t*>(data);
    for (int i = 0; i < nblocks; i++) {
        uint32_t k1 = blocks[i];
        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> (32 - 15));
        k1 *= c2;

        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> (32 - 13));
        h1 = h1 * 5 + 0xe6546b64;
    }

    // Tail
    const uint8_t* tail = data + nblocks * 4;
    uint32_t k1 = 0;
    switch (len & 3) {
        case 3: k1 ^= tail[2] << 16;
        case 2: k1 ^= tail[1] << 8;
        case 1: k1 ^= tail[0];
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >> (32 - 15));
                k1 *= c2;
                h1 ^= k1;
    }

    // Finalization
    h1 ^= len;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;

    return h1;
}

uint32_t hash_array(const std::vector<int>& array, uint32_t seed = 42) {
    size_t len = array.size() * sizeof(int);
    const void* data = static_cast<const void*>(array.data());

    return murmurhash3_32(data, len, seed);
}

template <typename T>
void generateCombinations(const std::vector<std::vector<T>>& arrays, 
                          std::vector<T>& currentCombination, 
                          size_t depth, 
                          std::vector<std::vector<T>>& results) {
    if (depth == arrays.size()) {
        results.push_back(currentCombination);
        return;
    }
    for (const T& element : arrays[depth]) {
        currentCombination[depth] = element; 
        generateCombinations(arrays, currentCombination, depth + 1, results); 
    }
}


#endif