#pragma once
#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <cstring>
#include <bits/syscall.h>

#ifndef SYS_perf_event_open
    #define SYS_perf_event_open 298
#endif

class PerfEventMonitor {
public:
    PerfEventMonitor(pid_t pid) : pid_(pid), fd_cpu_usage(-1), fd_context_switch(-1) {
        // 配置 CPU 周期事件
        memset(&attr_cpu_usage, 0, sizeof(struct perf_event_attr));
        attr_cpu_usage.type = PERF_TYPE_HARDWARE;
        attr_cpu_usage.size = sizeof(struct perf_event_attr);
        attr_cpu_usage.config = PERF_COUNT_HW_CPU_CYCLES;
        attr_cpu_usage.disabled = 1;
        // 配置上下文切换事件
        memset(&attr_context_switch, 0, sizeof(struct perf_event_attr));
        attr_context_switch.type = PERF_TYPE_SOFTWARE;
        attr_context_switch.size = sizeof(struct perf_event_attr);
        attr_context_switch.config = PERF_COUNT_SW_CONTEXT_SWITCHES;
        attr_context_switch.disabled = 1;
    }

    ~PerfEventMonitor() {
        if (fd_cpu_usage != -1) {
            close(fd_cpu_usage);
        }
        if (fd_context_switch != -1) {
            close(fd_context_switch);
        }
    }

    bool openCounters() {
        // 打开 CPU 使用计数器
        fd_cpu_usage = syscall(SYS_perf_event_open, &attr_cpu_usage, pid_, -1, -1, 0);
        if (fd_cpu_usage == -1) {
            std::cerr << "Error opening perf_event for CPU usage: " << strerror(errno) << std::endl;
            return false;
        }
        // 打开上下文切换计数器
        fd_context_switch = syscall(SYS_perf_event_open, &attr_context_switch, pid_, -1, -1, 0);
        if (fd_context_switch == -1) {
            std::cerr << "Error opening perf_event for context switches: " << strerror(errno) << std::endl;
            return false;
        }
        return true;
    }

    void startCounters() {
        if (fd_cpu_usage != -1) {
            ioctl(fd_cpu_usage, PERF_EVENT_IOC_ENABLE);
        }
        if (fd_context_switch != -1) {
            ioctl(fd_context_switch, PERF_EVENT_IOC_ENABLE);
        }
    }

    void stopCounters() {
        if (fd_cpu_usage != -1) {
            ioctl(fd_cpu_usage, PERF_EVENT_IOC_DISABLE);
        }
        if (fd_context_switch != -1) {
            ioctl(fd_context_switch, PERF_EVENT_IOC_DISABLE);
        }
    }

    void readCounters() {
        //long long cpu_usage = 0, context_switches = 0;
        if (fd_cpu_usage != -1) {
            read(fd_cpu_usage, &cpu_usage, sizeof(cpu_usage));
        }
        if (fd_context_switch != -1) {
            read(fd_context_switch, &context_switches, sizeof(context_switches));
        }
    }

    

private:
    pid_t pid_;
    int fd_cpu_usage;
    int fd_context_switch;
    struct perf_event_attr attr_cpu_usage;
    struct perf_event_attr attr_context_switch;

    long long cpu_usage;
    long long context_switches;
};