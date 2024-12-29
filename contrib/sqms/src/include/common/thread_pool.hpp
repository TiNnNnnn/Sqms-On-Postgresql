#pragma once
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <future>


#ifndef THREAD_POOL_HPP
#define THREAD_POOL_HPP

/**
 * global singletons are guaranteed
 */
class ThreadPool {
public:
    explicit ThreadPool(size_t threadCount) : stopFlag(false) {
        for (size_t i = 0; i < threadCount; ++i) {
            workers.emplace_back(&ThreadPool::workerThread, this);
            workers.back().detach();
        }
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    static std::thread::id GetTid() {
        return std::this_thread::get_id();
    }

    /**
     * submit: push a task into the thread_pool
     */
    template <typename Func, typename... Args>
    auto submit(Func&& func, Args&&... args) -> std::future<decltype(func(args...))> {
        using ReturnType = decltype(func(args...));

        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<Func>(func), std::forward<Args>(args)...)
        );

        std::future<ReturnType> result = task->get_future();
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            if (stopFlag) {
                throw std::runtime_error("ThreadPool has been stopped!");
            }
            tasks.emplace([task]() { (*task)(); });
        }

        condition.notify_one();
        return result;
    }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            stopFlag = true;
        }
        condition.notify_all();
    }

private:
    void workerThread() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(queueMutex);
                condition.wait(lock, [this]() { return stopFlag || !tasks.empty(); });

                if (stopFlag && tasks.empty()) {
                    return;
                }

                task = std::move(tasks.front());
                tasks.pop();
            }
            task();
        }
    }

    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;

    std::mutex queueMutex;
    std::condition_variable condition;
    std::atomic<bool> stopFlag;
};

#endif // THREAD_POOL_HPP