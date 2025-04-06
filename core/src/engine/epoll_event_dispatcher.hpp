#pragma once

#ifdef __linux__

#include <cstring> 
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#include <fcntl.h>
#include <x86intrin.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <engine/task/task_queue.hpp>
#include <userver/logging/log.hpp>
#include <userver/utils/fast_scope_guard.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

namespace impl {
class TaskProcessorPools;
}

class EpollEventDispatcher {
public:
    explicit EpollEventDispatcher(size_t thread_count);
    
    ~EpollEventDispatcher();
    
    EpollEventDispatcher(const EpollEventDispatcher&) = delete;
    EpollEventDispatcher& operator=(const EpollEventDispatcher&) = delete;
    EpollEventDispatcher(EpollEventDispatcher&&) = delete;
    EpollEventDispatcher& operator=(EpollEventDispatcher&&) = delete;

    /// @brief Process events using epoll (called by worker threads)
    void ProcessEvents(
        std::size_t thread_index, 
        TaskQueue& queue, 
        std::shared_ptr<impl::TaskProcessorPools> pools);
    
    /// @brief Register a file descriptor with epoll
    std::size_t RegisterFd(
        int fd, 
        uint32_t events, 
        std::function<void(uint32_t)> callback, 
        std::weak_ptr<void> owner = {});
    
    /// @brief Unregister a file descriptor
    void UnregisterFd(int fd);
    
    /// @brief Wake up a worker thread to process pending events
    /// Automatically selects the best thread to wake up
    void PostEvent();
    
    /// @brief Wake up a specific worker thread
    void PostEvent(std::size_t thread_index);
    
    /// @brief Initiate shutdown of the event dispatcher
    void Shutdown();
    
    /// @brief Check if shutdown is in progress
    bool IsShuttingDown() const { 
        return is_shutting_down_.load(std::memory_order_acquire); 
    }

private:
    /// @brief Information about registered file descriptors
    struct FdCallbackInfo {
        std::function<void(uint32_t)> callback;
        uint32_t requested_events;
        size_t owner_thread;
    };

    /// @brief Thread state for efficient load balancing
    enum class ThreadState {
        kSpinning,   /// Thread is actively processing
        kSleeping,   /// Thread is blocking in epoll_wait
        kBusy        /// Thread is executing a task
    };

    /// @brief Get the current state of a worker thread
    ThreadState GetThreadState(std::size_t thread_index) const;

    /// @brief Select the best thread to handle a new event
    std::optional<std::size_t> SelectThreadToWakeup();

    /// @brief Runs a single task
    void ExecuteTask(impl::TaskContext* task, std::size_t thread_index);
    
    /// @brief Performs spinning phase, returns true if events detected
    bool PerformSpinning(std::size_t thread_index, epoll_event* events, int epoll_fd);
    
    /// @brief Waits for events in blocking mode
    void WaitForEvents(std::size_t thread_index, epoll_event* events, int epoll_fd);
    
    /// @brief Processes events received from epoll
    void ProcessEpollEvents(std::size_t thread_index, epoll_event* events, int nevents);
    
    /// @brief Processes a single file descriptor event
    void ProcessFdEvent(int fd, uint32_t events);
    
    /// @brief Handles epoll errors
    void HandleEpollError();

    /// @brief Create an epoll instance
    int CreateEpollInstance() const;

    /// @brief Create a notification channel (eventfd)
    int CreateNotificationChannel() const;

    /// @brief Add a file descriptor to an epoll instance
    bool AddToEpoll(int epoll_fd, int fd, uint32_t events, uint64_t data) const;

    /// @brief Remove a file descriptor from an epoll instance
    bool RemoveFromEpoll(int epoll_fd, int fd) const;

    /// @brief Periodically clean up dead owners from the registry
    void CleanupDeadOwners();

    /// Number of worker threads
    const size_t thread_count_;
    
    /// Per-thread epoll file descriptors
    std::vector<int> thread_epoll_fds_;
    
    /// Per-thread notification eventfds
    std::vector<int> thread_notify_fds_;
    
    /// Thread spinning state (actively processing vs waiting)
    std::vector<std::atomic<bool>> thread_spinning_;
    
    /// Thread sleep start time (for fair load balancing)
    std::vector<std::atomic<uint64_t>> thread_sleep_start_time_;
    
    /// Mutex for file descriptor operations
    std::mutex fd_mutex_;
    
    /// Map of registered file descriptors to their callbacks
    std::unordered_map<int, FdCallbackInfo> fd_callbacks_;
    
    /// Shutdown flag
    std::atomic<bool> is_shutting_down_{false};

    /// Mutex for the fd ownership registry
    static inline std::mutex registry_mutex_;
    
    /// Map of file descriptors to their owners (for lifetime management)
    static inline std::unordered_map<int, std::weak_ptr<void>> fd_to_owner_;

    int spin_nevents_{0};
};

}  // namespace engine

USERVER_NAMESPACE_END

#endif  // __linux__