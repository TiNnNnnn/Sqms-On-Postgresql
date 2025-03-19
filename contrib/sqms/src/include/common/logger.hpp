#pragma once
#include "spdlog/spdlog.h"
#include "spdlog/sinks/sink.h"
#include <fstream>
#include <map>
#include <mutex>
#include <memory>
#include <iostream>
#include "util.hpp"

#include "spdlog/spdlog.h"
#include "spdlog/sinks/sink.h"
#include <fstream>
#include <map>
#include <mutex>
#include <memory>
#include <iostream>
#include <filesystem>
#include "util.hpp"

extern "C" {
    #include "storage/lwlock.h"
    #include "utils/guc.h"
    #include "miscadmin.h"
}

class TaggedFileSink {
public:
    TaggedFileSink(const SMString& log_dir,const SMString& time_str,LWLock* shmem_lock)
        :log_dir_(log_dir),time_str_(time_str),shmem_lock_(shmem_lock){}

    void log(const SMString& tag, const SMString& message) {
        std::lock_guard<std::mutex> lock(mutex_);
        if(log_files_.find(tag) == log_files_.end()){
            auto log_filename = log_dir_ + "/" + time_str_ + "_" + tag + ".log";
            LWLockAcquire(shmem_lock_, LW_EXCLUSIVE);
            auto sink_ptr = (spdlog::sinks::sink*)ShmemAlloc(sizeof(spdlog::sinks::basic_file_sink_mt));
            LWLockRelease(shmem_lock_);
            new (sink_ptr) spdlog::sinks::basic_file_sink_mt(log_filename.c_str(), true);

            LWLockAcquire(shmem_lock_, LW_EXCLUSIVE);
            log_files_[tag] = (spdlog::logger*)ShmemAlloc(sizeof(spdlog::logger));
            LWLockRelease(shmem_lock_);
            new (log_files_[tag]) spdlog::logger(tag.c_str(), std::shared_ptr<spdlog::sinks::sink>(sink_ptr));
            log_files_[tag]->set_level(spdlog::level::info);
        }
        log_files_[tag]->info(message);
        log_files_[tag]->flush();
    }
private:
    SMMap<SMString, spdlog::logger*> log_files_;
    SMString log_dir_;
    SMString time_str_;
    std::mutex mutex_;
    LWLock* shmem_lock_;
};

class SqmsLogger {
public:
    SqmsLogger(LWLock* shmem_lock)
        : shmem_lock_(shmem_lock){
        static char time_str[20];
        time_t now = time(NULL);
        struct tm *tm_info = localtime(&now);
        strftime(time_str, sizeof(time_str), "%Y%m%d_%H%M%S", tm_info);

        LWLockAcquire(shmem_lock_, LW_EXCLUSIVE);
        tagged_sink_ = (TaggedFileSink*)ShmemAlloc(sizeof(TaggedFileSink));
        LWLockRelease(shmem_lock_);
        assert(tagged_sink_);
        new (tagged_sink_) TaggedFileSink(log_dir,SMString(time_str),shmem_lock_);
    }

    void Logger(const char* tag, const char* msg) {
        tagged_sink_->log(SMString(tag),SMString(msg));
    }
private:
    TaggedFileSink* tagged_sink_;
    SMString log_dir = "/home/yyk/Sqms-On-Postgresql/log";
    LWLock* shmem_lock_;
};
