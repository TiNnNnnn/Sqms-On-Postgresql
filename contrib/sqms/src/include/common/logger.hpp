#pragma once
#include "spdlog/spdlog.h"
#include "spdlog/sinks/sink.h"
#include <fstream>
#include <map>
#include <mutex>
#include <memory>
#include <vector>
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
    TaggedFileSink(const SMString& log_dir,const SMString& time_str)
        :log_dir_(log_dir),time_str_(time_str){}

    void log(const SMString& tag, const SMString& message) {
        if(log_files_.find(tag) == log_files_.end()){
            auto log_filename = log_dir_ + "/" + time_str_ + "_" + tag + ".log";
            auto sink_ptr = (spdlog::sinks::sink*)ShmemAlloc(sizeof(spdlog::sinks::basic_file_sink_mt));
            log_files_[tag] = (spdlog::logger*)ShmemAlloc(sizeof(spdlog::logger));

            new (sink_ptr) spdlog::sinks::basic_file_sink_mt(log_filename.c_str(), true);            
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
};

class SqmsLogger {
public:
    SqmsLogger(){
        static char time_str[20];
        time_t now = time(NULL);
        struct tm *tm_info = localtime(&now);
        strftime(time_str, sizeof(time_str), "%Y%m%d_%H%M%S", tm_info);
        
        tagged_sink_ = (TaggedFileSink*)ShmemAlloc(sizeof(TaggedFileSink));
        assert(tagged_sink_);
        new (tagged_sink_) TaggedFileSink(log_dir,SMString(time_str));

        const std::vector<SMString> log_tags = {"comming","slow"};
        for(const auto& tag : log_tags){
            auto log_filename = SMString(log_dir) + "/" + SMString(time_str) + "_" + tag + ".log";
            auto sink_ptr = (spdlog::sinks::sink*)ShmemAlloc(sizeof(spdlog::sinks::basic_file_sink_mt));
            log_files_[tag] = (spdlog::logger*)ShmemAlloc(sizeof(spdlog::logger));

            new (sink_ptr) spdlog::sinks::basic_file_sink_mt(log_filename.c_str(), true);            
            new (log_files_[tag]) spdlog::logger(tag.c_str(), std::shared_ptr<spdlog::sinks::sink>(sink_ptr));
            log_files_[tag]->set_level(spdlog::level::info);
        }
    }

    void Logger(const char* tag, const char* msg) {
        //tagged_sink_->log(SMString(tag),SMString(msg));
        std::lock_guard<std::mutex> lock(mutex_);
        if(SMString(tag) == "comming" || SMString(tag) == "slow"){
            log_files_[tag]->info(msg);
            log_files_[tag]->flush();
        }else{
            std::cerr<<"unknow logger type"<<std::endl;
            exit(-1);
        }
    }
private:
    std::mutex mutex_;
    TaggedFileSink* tagged_sink_;
    SMString log_dir = "/home/yyk/Sqms-On-Postgresql/log";
    SMMap<SMString, spdlog::logger*> log_files_;
};
