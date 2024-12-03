#pragma once
#include <mutex>
#include <chrono>
#include <atomic>
#include <iostream>

class CounterStat {
public:
    void update(int count) {
        count_.fetch_add(count, std::memory_order_relaxed);
    }
    int get() const {
        return count_.load(std::memory_order_relaxed); 
    }
private:
    std::atomic<int> count_{0};
};

class TimeStat {
public:
    explicit TimeStat(long unit) : timeUnit(unit) {}

    class BlockTimer {
    public:
        BlockTimer(TimeStat* timeStat) : timeStat(timeStat), start(std::chrono::high_resolution_clock::now()) {}

        ~BlockTimer() {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            timeStat->update(duration);
        }

    private:
        TimeStat* timeStat;
        std::chrono::high_resolution_clock::time_point start;
    };

    void update(long duration) {
        std::cout << "Time taken: " << duration << "ms" << std::endl;
    }

    BlockTimer time() {
        return BlockTimer(this);
    }

private:
    long timeUnit;
};
