#pragma once

#ifdef __linux__
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <fcntl.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <userver/logging/log.hpp>
#include <userver/utils/assert.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

namespace impl {
class TaskProcessorPools;
}

class TaskQueue;

// Thread-safe manager for epoll-based IO events that integrates with TaskProcessor
class EpollEventDispatcher final {
public:
    using EventCallback = std::function<void(uint32_t)>;
    
    explicit EpollEventDispatcher(size_t thread_count);
    ~EpollEventDispatcher();

    EpollEventDispatcher(const EpollEventDispatcher&) = delete;
    EpollEventDispatcher(EpollEventDispatcher&&) = delete;
    EpollEventDispatcher& operator=(const EpollEventDispatcher&) = delete;
    EpollEventDispatcher& operator=(EpollEventDispatcher&&) = delete;

    void ProcessEvents(std::size_t thread_index, TaskQueue& queue, 
                        std::shared_ptr<impl::TaskProcessorPools> pools);
    
    // Register a file descriptor with EPOLLET mode
    std::size_t RegisterFd(int fd, uint32_t events, EventCallback callback, 
                            std::weak_ptr<void> owner = {});
    
    // Unregister a file descriptor
    void UnregisterFd(int fd);
    
    // Post an event to wake up threads
    void PostEvent();
    
    // Signal shutdown
    void Shutdown();

private:
  // Information about registered callbacks
    struct FdRegistration {
        EventCallback callback;
        uint32_t events;
        std::weak_ptr<void> owner;
        std::chrono::steady_clock::time_point last_activity;
        std::size_t thread_index;
    };

    // Add fd to epoll for the specified thread
    bool AddToEpoll(std::size_t thread_index, int fd, uint32_t events);
    
    // Remove fd from epoll for the specified thread
    bool RemoveFromEpoll(std::size_t thread_index, int fd);
    
    // Modify epoll entry for the specified thread
    bool ModifyEpoll(std::size_t thread_index, int fd, uint32_t events);
    
    // Wake up a specific thread
    void WakeupThread(std::size_t thread_index);
    
    // Choose the best thread to wake up based on thread state
    std::optional<std::size_t> SelectThreadToWakeup() const;
    
    // Create eventfd for thread notification
    void SetupThreadFds();
    
    // Clean up resources
    void CleanupThreadFds();
    
    // Process one epoll_wait result
    void ProcessOneEpollEvent(std::size_t thread_index, int fd, uint32_t events);

    // Store thread count for bounds checking
    size_t thread_count_{0};
    
    // Per-thread epoll file descriptors
    std::vector<int> epoll_fds_;
    
    // Per-thread eventfd for notifications 
    std::vector<int> notify_fds_;
    
    // Track which threads are busy/idle
    std::unique_ptr<std::atomic<bool>[]> thread_active_;
    
    // Timestamp when threads went idle
    std::unique_ptr<std::atomic<uint64_t>[]> thread_idle_since_;
    
    // Thread-safe fd registration storage
    std::mutex fd_mutex_;
    std::unordered_map<int, FdRegistration> fd_registrations_;
    
    // Flag to signal shutdown
    std::atomic<bool> is_shutting_down_{false};
    
    // Epoll events buffer reused across calls
    static constexpr int kMaxEvents = 64;

    enum class ThreadState {
        kActive, // Thread is processing tasks
        kSpinning, // No tasks, thread is spinning
        kSleeping // No tasks, thread is sleeping in epoll_wait
    };

    // Spin result
    enum class SpinResult {
        kTaskProcessed,  // Task was processed
        kEventsProcessed, // Epoll events were processed
        kSpinningFailed   // Spinning failed, go to epoll_wait
    };

    // Spin waiting for tasks or events
    SpinResult SpinForTaskOrEvent(std::size_t thread_index, TaskQueue& queue, 
        std::shared_ptr<impl::TaskProcessorPools> pools,
        struct epoll_event* events);
    
    std::unique_ptr<std::atomic<ThreadState>[]> thread_state_;
    
    // Spinning settings
    static constexpr std::chrono::milliseconds kSpinningDuration{1};
    static constexpr int kSpinningIterations = 10000;
};

}  // namespace engine

USERVER_NAMESPACE_END

#endif  // __linux__