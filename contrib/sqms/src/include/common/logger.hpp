// /**
//  * TODO: sqms need a sepearte log system spearte from postgresql
//  */
// #include "spdlog/spdlog.h"
// #include "spdlog/sinks/sink.h"
// #include <fstream>
// #include <map>
// #include <mutex>
// #include <memory>
// #include <iostream>
// #include "util.hpp"
// class TaggedFileSink : public spdlog::sinks::sink {
// public:
//     void log(const spdlog::details::log_msg &msg) override {
//         std::lock_guard<std::mutex> lock(mutex);

//         // fetch log msg
//         SMString log_message(msg.payload.begin(), msg.payload.end());

//         /* fetch tag（format："[TAG] message..."）*/
//         SMString tag = extract_tag(log_message);

//         /*fetch target log file*/
//         if (log_files.find(tag) == log_files.end()) {
//             log_files[tag].open((tag + SMString(".log")).c_str(), std::ios::app);
//         }

//         /*write logs*/
//         if (log_files[tag].is_open()) {
//             log_files[tag] << log_message << std::endl;
//         }
//     }

//     void flush() override {
//         std::lock_guard<std::mutex> lock(mutex);
//         for (auto &entry : log_files) {
//             entry.second.flush();
//         }
//     }

// private:
//     SMString extract_tag(SMString &message) {
//         size_t start = message.find("[");
//         size_t end = message.find("]");
//         if (start != SMString::npos && end != SMString::npos && end > start) {
//             SMString tag = message.substr(start + 1, end - start - 1);
//             message = message.substr(end + 2); 
//             return tag;
//         }
//         return "default";
//     }
// private:
//     SMMap<SMString, std::ofstream> log_files;
//     std::mutex mutex;
//     SMString default_filename = "default.log";
// };

// // int main() {
// //     auto tagged_sink = std::make_shared<TaggedFileSink>();

// //     // 绑定到 SPDLOG
// //     spdlog::logger logger("tagged_logger", tagged_sink);
// //     logger.set_level(spdlog::level::info);

// //     // 记录不同类别的日志
// //     logger.info("[SQL] Executing query: SELECT * FROM users;");
// //     logger.info("[Network] Received packet from 192.168.1.1");
// //     logger.info("[Auth] User login success");

// //     return 0;
// // }
