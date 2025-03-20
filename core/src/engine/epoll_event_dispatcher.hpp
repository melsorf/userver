#pragma once

#ifdef __linux__

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <fcntl.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <engine/task/task_queue.hpp>
#include <userver/logging/log.hpp>
#include <userver/utils/fast_scope_guard.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

struct FdCallbackInfo {
    std::function<void(uint32_t)> callback;
    uint32_t requested_events;
};

class EpollEventDispatcher {
public:
    explicit EpollEventDispatcher(size_t thread_count);
    ~EpollEventDispatcher();

    // Process events using epoll (called by worker threads)
    void ProcessEvents(std::size_t thread_index, TaskQueue& queue);
    
    // Register a file descriptor with EPOLLET
    std::size_t RegisterFd(int fd, uint32_t events, std::function<void(uint32_t)> callback);
    
    // Unregister a file descriptor
    void UnregisterFd(int fd);
    
    // Schedule a timer event
    bool ScheduleTimer(std::chrono::steady_clock::duration delay, std::function<void()> callback);
    
    // Post an event to wake up a worker thread
    void PostEvent();
    
    // Post an event to wake up a specific worker thread
    void PostEvent(std::size_t thread_index);
    
    // Initiate shutdown
    void Shutdown();
    
    // Check if shutdown is requested
    bool IsShuttingDown() const { return is_shutting_down_.load(std::memory_order_acquire); }
    
    // Returns the best thread to handle this task
    std::size_t SelectThreadToWakeup();

private:
    // Wake up a specific worker thread
    void WakeupWorkerThread(std::size_t thread_index);
    
    // Process expired timers
    void ProcessTimerEvents();
    
    // Update timerfd with the earliest deadline
    void UpdateTimerFd();

    // Thread count
    size_t thread_count_{0};
    
    // Per-thread epoll fds
    std::vector<int> thread_epoll_fds_;
    
    // Per-thread notification eventfds
    std::vector<int> thread_notify_fds_;
    
    // For timer events
    int timer_fd_{-1};
    
    // Thread states
    std::unique_ptr<std::atomic<bool>[]> thread_spinning_;
    std::unique_ptr<std::atomic<uint64_t>[]> thread_sleep_start_time_;
    
    // Mutex for fd operations
    std::mutex fd_mutex_;
    std::unordered_map<int, FdCallbackInfo> fd_callbacks_;
    
    // Timer callbacks and their mutex
    std::mutex timers_mutex_;
    std::multimap<std::chrono::steady_clock::time_point, std::function<void()>> timers_;
    
    // Shutdown flag
    std::atomic<bool> is_shutting_down_{false};
};

}  // namespace engine

USERVER_NAMESPACE_END

#endif  // __linux__