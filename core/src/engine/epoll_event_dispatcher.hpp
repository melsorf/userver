#ifdef __linux__
#pragma once

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <fcntl.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <queue>
#include <utility>
#include <map>

#include <userver/logging/log.hpp>
#include <userver/utils/fast_scope_guard.hpp>
#include <engine/task/task_context.hpp>
#include <engine/task/task_queue.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

struct FdCallbackInfo {
    std::function<void(uint32_t)> callback;
    uint32_t requested_events;
};

class EpollEventDispatcher {
public:
    EpollEventDispatcher(size_t thread_count);
    ~EpollEventDispatcher();

    // Posts an event to wake up a thread for task processing
    void PostEvent();
    
    // Schedule a timer event
    bool ScheduleTimer(std::chrono::steady_clock::duration delay, 
                      std::function<void()> callback);
    
    // Register a file descriptor with EPOLLET
    std::size_t RegisterFd(int fd, uint32_t events, 
                          std::function<void(uint32_t)> callback);
    
    // Unregister a file descriptor
    void UnregisterFd(int fd);
    
    // Process events from the epoll queue (called by worker threads)
    void ProcessEvents(std::size_t thread_index, TaskQueue& queue);
    
    // Initiate shutdown
    void Shutdown();

    // Indicates if shutdown is requested
    bool IsShuttingDown() const { return is_shutting_down_.load(std::memory_order_acquire); }

    // Method to update the timerfd with the earliest deadline
    void UpdateTimerFd();

private:
    // Wake up an appropriate worker thread
    void WakeupWorkerThread(std::size_t thread_index);

    // Select which thread to wake up based on current state
    std::size_t SelectThreadToWakeup();

    // Timer handling
    void ProcessTimerEvents();
    
    // Main epoll file descriptor
    int epoll_fd_{-1};
    
    // For task notifications
    int task_event_fd_{-1};
    
    // For timer events
    int timer_fd_{-1};
    
    // Thread states
    size_t thread_count_{0};
    std::unique_ptr<std::atomic<bool>[]> thread_spinning_;
    std::unique_ptr<std::atomic<uint64_t>[]> thread_sleep_start_time_;
    
    // Mutex for fd operations
    std::mutex fd_mutex_;
    std::unordered_map<int, FdCallbackInfo> fd_callbacks_;
    
    // Timer callbacks and their mutex
    std::mutex timers_mutex_;
    std::multimap<std::chrono::steady_clock::time_point, std::function<void()>> timers_;
    
    std::atomic<bool> is_shutting_down_{false};
};

}  // namespace engine

USERVER_NAMESPACE_END

#endif // __linux__