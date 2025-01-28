#ifndef UTILS_UTIL_H_
#define UTILS_UTIL_H_
#include <stdint.h>
#include <vector>
#include <string>
#include "tbb/concurrent_hash_map.h"
#include <unordered_map>
#include <unordered_set>
#include <map>

extern "C" {
    #include "postgres.h"
    #include "common/config.h"
    #include "storage/shmem.h"
}
//单位是ms
uint64_t GetCurrentTime();
void GetCurrentTimeString(std::string&output);
int64_t GetCurrentTid();
int64_t GetCurrentPid();
bool CheckLittleEndian();
uint32_t DecodeFixed32(const char* ptr);
uint32_t SimMurMurHash(const char *data, uint32_t len);
int64_t Next2Power(uint64_t value);

/*shared memory allocator based on postgres shemem interfaces*/
template <typename T>
struct SharedMemoryAllocator {
    using value_type = T;

    SharedMemoryAllocator() {}
    template <typename U>
    SharedMemoryAllocator(const SharedMemoryAllocator<U>&) {}

    T* allocate(std::size_t n) {
        void* ptr = ShmemAlloc(n * sizeof(T));
        if (!ptr) {
            throw std::bad_alloc();
        }
        return static_cast<T*>(ptr);
    }

    void deallocate(T* p, std::size_t n) {
    }

    bool operator==(const SharedMemoryAllocator& other) const noexcept {
        return true; 
    }

    bool operator!=(const SharedMemoryAllocator& other) const noexcept {
        return !(*this == other);
    }
};

template <typename Key, typename Value,typename Hash = std::hash<Key>,typename EqualTo = std::equal_to<Key>>
using SMUnorderedMap = std::unordered_map<
    Key,
    Value,
    Hash,
    EqualTo,
    SharedMemoryAllocator<std::pair<const Key, Value>>
>;

template <typename Key, typename Value,typename Hash = tbb::tbb_hash_compare<Key>>
using SMConcurrentHashMap = tbb::concurrent_hash_map<
    Key,
    Value,
    Hash,
    SharedMemoryAllocator<std::pair<const Key, Value>>
>;

template <typename Key, typename Value,typename Compare = std::less<Key>>
using SMMap = std::map<
    Key,
    Value,
    Compare,
    SharedMemoryAllocator<std::pair<const Key, Value>>
>;

template <typename Key, typename Hash = std::hash<Key>,typename EqualTo = std::equal_to<Key>>
using SMUnorderedSet = std::unordered_set<
    Key,
    Hash,
    EqualTo,
    SharedMemoryAllocator<Key>
>;


template <typename Key>
using SMVector = std::vector<Key,SharedMemoryAllocator<Key>>;


template <typename T>
void generateCombinations(const std::vector<std::vector<T>>& arrays, 
                          std::vector<T>& currentCombination, 
                          size_t depth, 
                          std::vector<std::vector<T>>& results);

template <typename T>
void generateCombinations(const SMVector<SMVector<T>>& arrays, 
                          SMVector<T>& currentCombination, 
                          size_t depth, 
                          SMVector<SMVector<T>>& results);

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

template <typename T>
void generateCombinations(const SMVector<SMVector<T>>& arrays, 
                          SMVector<T>& currentCombination, 
                          size_t depth, 
                          SMVector<SMVector<T>>& results) {
    if (depth == arrays.size()) {
        results.push_back(currentCombination);
        return;
    }
    for (const T& element : arrays[depth]) {
        currentCombination[depth] = element; 
        generateCombinations(arrays, currentCombination, depth + 1, results); 
    }
}

uint32_t hash_array(const std::vector<int>& array);
uint32_t hash_array(const SMVector<int>& array);

#endif