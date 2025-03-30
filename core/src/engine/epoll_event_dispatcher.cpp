#include "epoll_event_dispatcher.hpp"

#ifdef __linux__
#include <algorithm>
#include <chrono>
#include <thread>

#include <engine/task/task_context.hpp>
#include <engine/task/task_processor_pools.hpp>
#include <engine/task/task_queue.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

EpollEventDispatcher::EpollEventDispatcher(size_t thread_count)
    : thread_count_(thread_count),
      epoll_fds_(thread_count, -1),
      notify_fds_(thread_count, -1),
      thread_idle_since_(std::make_unique<std::atomic<uint64_t>[]>(thread_count)),
      thread_state_(std::make_unique<std::atomic<ThreadState>[]>(thread_count)) {
      
    for (size_t i = 0; i < thread_count_; ++i) {
        thread_idle_since_[i].store(0, std::memory_order_relaxed);
        thread_state_[i].store(ThreadState::kActive, std::memory_order_relaxed);
    }
    
    SetupThreadFds();
}

EpollEventDispatcher::~EpollEventDispatcher() {
    Shutdown();
    CleanupThreadFds();
}

void EpollEventDispatcher::SetupThreadFds() {
    try {
        for (size_t i = 0; i < thread_count_; ++i) {
            // Create epoll fd
            epoll_fds_[i] = epoll_create1(EPOLL_CLOEXEC);
            if (epoll_fds_[i] == -1) {
                throw std::system_error(errno, std::system_category(), "epoll_create1 failed");
            }
            
            // Create eventfd for waking up the thread
            notify_fds_[i] = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            if (notify_fds_[i] == -1) {
                throw std::system_error(errno, std::system_category(), "eventfd failed");
            }
            
            // Add eventfd to epoll set
            struct epoll_event event{};
            event.events = EPOLLIN | EPOLLET;
            event.data.fd = notify_fds_[i];
            
            if (epoll_ctl(epoll_fds_[i], EPOLL_CTL_ADD, notify_fds_[i], &event) == -1) {
                throw std::system_error(errno, std::system_category(), "epoll_ctl add notify_fd failed");
            }
        }
    } catch (const std::exception& ex) {
        LOG_ERROR() << "Failed to initialize epoll: " << ex.what();
        CleanupThreadFds();
        throw;
    }
}

void EpollEventDispatcher::CleanupThreadFds() {
    for (size_t i = 0; i < epoll_fds_.size(); ++i) {
        if (epoll_fds_[i] != -1) {
            close(epoll_fds_[i]);
            epoll_fds_[i] = -1;
        }
        
        if (notify_fds_[i] != -1) {
            close(notify_fds_[i]);
            notify_fds_[i] = -1;
        }
    }
}

void EpollEventDispatcher::Shutdown() {
    is_shutting_down_.store(true, std::memory_order_release);
    for (size_t i = 0; i < thread_count_; ++i) {
        WakeupThread(i);
    }
}

void EpollEventDispatcher::WakeupThread(std::size_t thread_index) {
    if (thread_index >= thread_count_ || notify_fds_[thread_index] == -1) {
        return;
    }
    
    // Write to eventfd to wake up thread
    const uint64_t value = 1;
    ssize_t bytes_written = 0;
    
    const int max_retries = 10;
    int retry_count = 0;
    
    do {
        bytes_written = write(notify_fds_[thread_index], &value, sizeof(value));
        if (bytes_written == -1 && 
            (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) && 
            !is_shutting_down_.load(std::memory_order_acquire) &&
            retry_count++ < max_retries) {
            continue;
        }
        break;
    } while (true);
    
    if (bytes_written != sizeof(value) && bytes_written != -1 && !is_shutting_down_.load(std::memory_order_acquire)) {
        LOG_ERROR() << "Incomplete write to eventfd: " << bytes_written << " of " << sizeof(value) << " bytes";
    }
}

void EpollEventDispatcher::PostEvent() {
    auto thread_idx = SelectThreadToWakeup();
    if (thread_idx) {
        WakeupThread(*thread_idx);
    }
}

std::optional<std::size_t> EpollEventDispatcher::SelectThreadToWakeup() const {
    // Search for the spinning thread
    for (size_t i = 0; i < thread_count_; ++i) {
        if (thread_state_[i].load(std::memory_order_relaxed) == ThreadState::kSpinning) {
            // If there are spinning threads, there is no need to wake up idle threads
            return std::nullopt;
        }
    }
    // Looking for a sleeping thread that sleeps the longest
    uint64_t max_idle_time = 0;
    std::optional<std::size_t> best_thread;
    
    for (size_t i = 0; i < thread_count_; ++i) {
        if (hread_state_[i].load(std::memory_order_relaxed) == ThreadState::kSleeping) {
            uint64_t idle_since = thread_idle_since_[i].load(std::memory_order_relaxed);
            if (idle_since > max_idle_time) {
                max_idle_time = idle_since;
                best_thread = i;
            }
        }
    }
    
    return best_thread;
}

std::size_t EpollEventDispatcher::RegisterFd(int fd, uint32_t events, EventCallback callback, 
                                            std::weak_ptr<void> owner) {
    if (fd < 0) {
        LOG_ERROR() << "Cannot register invalid fd: " << fd;
        return std::numeric_limits<std::size_t>::max();
    }
    
    if (!callback) {
        LOG_ERROR() << "Cannot register fd " << fd << " with null callback";
        return std::numeric_limits<std::size_t>::max();
    }
    
    events |= EPOLLET;
    
    // Select thread
    static std::atomic<size_t> next_thread{0};
    const size_t thread_index = next_thread.fetch_add(1, std::memory_order_relaxed) % thread_count_;
    
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        
        // First remove existing registration if any
        auto it = fd_registrations_.find(fd);
        if (it != fd_registrations_.end()) {
            // Update existing registration
            RemoveFromEpoll(it->second.thread_index, fd);
            it->second.callback = std::move(callback);
            it->second.events = events;
            it->second.owner = std::move(owner);
            it->second.last_activity = std::chrono::steady_clock::now();
            it->second.thread_index = thread_index;
        } else {
            // Create new registration
            fd_registrations_.emplace(fd, FdRegistration{
                std::move(callback),
                events,
                std::move(owner),
                std::chrono::steady_clock::now(),
                thread_index
            });
        }
    }
    
    // Add to epoll
    if (!AddToEpoll(thread_index, fd, events)) {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        fd_registrations_.erase(fd);
        return std::numeric_limits<std::size_t>::max();
    }
    WakeupThread(thread_index);
    return thread_index;
}

void EpollEventDispatcher::UnregisterFd(int fd) {
    if (fd < 0) return;
    
    size_t thread_index = 0;
    bool found = false;
    
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        auto it = fd_registrations_.find(fd);
        if (it != fd_registrations_.end()) {
            thread_index = it->second.thread_index;
            fd_registrations_.erase(it);
            found = true;
        }
    }
    if (found) {
        RemoveFromEpoll(thread_index, fd);
        WakeupThread(thread_index);
    }
}

bool EpollEventDispatcher::AddToEpoll(std::size_t thread_index, int fd, uint32_t events) {
    if (thread_index >= thread_count_ || epoll_fds_[thread_index] == -1) {
        return false;
    }
    
    struct epoll_event event{};
    event.events = events;
    event.data.fd = fd;
    
    int result = epoll_ctl(epoll_fds_[thread_index], EPOLL_CTL_ADD, fd, &event);
    if (result == -1) {
        if (errno == EEXIST) {
            // Fd already in epoll, try to modify instead
            return ModifyEpoll(thread_index, fd, events);
        }
        
        LOG_ERROR() << "Failed to add fd " << fd << " to epoll: " << strerror(errno);
        return false;
    }
    return true;
}

bool EpollEventDispatcher::RemoveFromEpoll(std::size_t thread_index, int fd) {
    if (thread_index >= thread_count_ || epoll_fds_[thread_index] == -1) {
        return false;
    }
    
    int result = epoll_ctl(epoll_fds_[thread_index], EPOLL_CTL_DEL, fd, nullptr);
    if (result == -1 && errno != ENOENT && errno != EBADF) {
        LOG_ERROR() << "Failed to remove fd " << fd << " from epoll: " << strerror(errno);
        return false;
    }
    
    return true;
}

bool EpollEventDispatcher::ModifyEpoll(std::size_t thread_index, int fd, uint32_t events) {
    if (thread_index >= thread_count_ || epoll_fds_[thread_index] == -1) {
        return false;
    }
    
    struct epoll_event event{};
    event.events = events;
    event.data.fd = fd;
    
    int result = epoll_ctl(epoll_fds_[thread_index], EPOLL_CTL_MOD, fd, &event);
    if (result == -1) {
        LOG_ERROR() << "Failed to modify fd " << fd << " in epoll: " << strerror(errno);
        return false;
    }
    
    return true;
}

void EpollEventDispatcher::ProcessOneEpollEvent(std::size_t thread_index, int fd, uint32_t events) {
    if (fd == notify_fds_[thread_index]) {
        // Just a wakeup notification, read from eventfd to clear it
        CheckAndDrainWakeup(thread_index);
        return;
    }
    
    // Look up the callback
    EventCallback callback;
    bool expired = false;
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        auto it = fd_registrations_.find(fd);
        if (it != fd_registrations_.end()) {
            callback = it->second.callback;
            it->second.last_activity = std::chrono::steady_clock::now();
            
            // Check if owner is still alive
            if (it->second.owner.expired()) {
                // Owner is gone, unregister
                LOG_DEBUG() << "Removing fd " << fd << " with expired owner";
                expired = true;
                fd_registrations_.erase(it);
            }
        }
    }

    if (expired) {
        RemoveFromEpoll(thread_index, fd);
        return;
    }
    
    // Execute the callback
    if (callback) {
        try {
            callback(events);
        } catch (const std::exception& ex) {
            LOG_ERROR() << "Exception in fd callback for fd " << fd << ": " << ex.what();
        }
    }
}

void EpollEventDispatcher::ProcessEvents(std::size_t thread_index, TaskQueue& queue,
                                        std::shared_ptr<impl::TaskProcessorPools> pools) {
    if (thread_index >= thread_count_) {
        LOG_ERROR() << "Invalid thread index: " << thread_index;
        return;
    }

    int epoll_fd = epoll_fds_[thread_index];
    if (epoll_fd == -1) {
        LOG_ERROR() << "Invalid epoll fd for thread " << thread_index;
        return;
    }
    struct epoll_event events[kMaxEvents];

    while (!is_shutting_down_.load(std::memory_order_acquire)) {
        // Mark thread as active
        thread_state_[thread_index].store(ThreadState::kActive, std::memory_order_relaxed);

        bool processed_any_tasks = false;
        while (true) {
            auto task_opt = queue.PopNonBlocking();
            if (!task_opt || !*task_opt) {
                break;  // No more tasks
            }
            processed_any_tasks = true;
            auto& task_ptr = *task_opt;
            // Process task
            bool has_failed = false;
            try {
                impl::TaskCounter::RunningToken token{task_ptr->GetTaskCounter()};
                task_ptr->DoStep();
            } catch (const std::exception& ex) {
                LOG_ERROR() << "Exception in task: " << ex.what();
                has_failed = true;
            }
            pools->GetCoroPool().AccountStackUsage();
            if (has_failed || task_ptr->IsFinished()) {
                task_ptr->FinishDetached();
            }
        }
        if (is_shutting_down_.load(std::memory_order_acquire)) break;
        if (processed_any_tasks) {
            continue;
        }

        // No tasks, start spinning
        SpinResult spin_result = SpinForTaskOrEvent(thread_index, queue, events);

        if (spin_result == SpinResult::kSpinningFailed) {
            std::atomic_thread_fence(std::memory_order_seq_cst);
            // Update thread state before sleeping
            auto prev_state = ThreadState::kActive;
            if (!thread_state_[thread_index].compare_exchange_strong(
                    prev_state, ThreadState::kSleeping, 
                    std::memory_order_release, std::memory_order_relaxed)) {
                // State was changed by another thread
                continue;
            }
            thread_idle_since_[thread_index].store(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count(),
                std::memory_order_release);
                
            std::atomic_thread_fence(std::memory_order_seq_cst);

            // Check if we got a wakeup signal before going to sleep
            if (CheckAndDrainWakeup(thread_index) || 
                is_shutting_down_.load(std::memory_order_acquire) ||
                !queue.IsEmpty()) {
                thread_state_[thread_index].store(ThreadState::kActive, std::memory_order_release);
                continue;
            }

            int ready = epoll_wait(epoll_fd, events, kMaxEvents, -1);

            thread_state_[thread_index].store(ThreadState::kActive, std::memory_order_relaxed);
            if (is_shutting_down_.load(std::memory_order_acquire)) break;

            if (ready > 0) {
                // Process events
                for (int i = 0; i < ready && !is_shutting_down_.load(std::memory_order_acquire); ++i) {
                    ProcessOneEpollEvent(thread_index, events[i].data.fd, events[i].events);
                }
            } else if (ready < 0 && errno != EINTR) {
                LOG_ERROR() << "epoll_wait failed: " << strerror(errno);
            }
        } else {
            // Process events or tasks that were found during spinning
            if (spin_result == SpinResult::kEventsFound) {
                int ready = epoll_wait(epoll_fd, events, kMaxEvents, 0); // Non-blocking
                if (ready > 0) {
                    for (int i = 0; i < ready; ++i) {
                        ProcessOneEpollEvent(thread_index, events[i].data.fd, events[i].events);
                    }
                } else if (ready < 0 && errno != EINTR) {
                    LOG_ERROR() << "epoll_wait failed after spin: " << strerror(errno);
                }
            }
            thread_state_[thread_index].store(ThreadState::kActive, std::memory_order_release);
            continue;
        }
    }
}

bool EpollEventDispatcher::CheckAndDrainWakeup(std::size_t thread_index) {
    if (thread_index >= thread_count_ || notify_fds_[thread_index] == -1) {
        return false;
    }
    bool wakeup_received = false;
    // Properly drain the eventfd
    while (true) {
        uint64_t value;
        int ret = read(notify_fds_[thread_index], &value, sizeof(value));
        if (ret > 0) {
            wakeup_received = true;
            // Continue reading to drain completely
        } else if (ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Drained all notifications
                break;
            }
            if (errno != EINTR) {
                if (!is_shutting_down_.load(std::memory_order_acquire)) {
                    LOG_ERROR() << "Error reading from eventfd: " << strerror(errno);
                }
                break;
            }
            // EINTR - retry
        } else {
            // ret == 0, should not happen with eventfd
            break;
        }
    }
    return wakeup_received;
}

EpollEventDispatcher::SpinResult EpollEventDispatcher::SpinForTaskOrEvent(
    std::size_t thread_index, 
    TaskQueue& queue, 
    struct epoll_event* events) {
    
    thread_state_[thread_index].store(ThreadState::kSpinning, std::memory_order_relaxed);
    auto spin_start = std::chrono::steady_clock::now();

    // Spin
    for (int spin_count = 0; spin_count < kSpinningIterations &&
        std::chrono::steady_clock::now() - spin_start < kSpinningDuration; ++spin_count) {
        
        if (is_shutting_down_.load(std::memory_order_acquire)) {
            thread_state_[thread_index].store(ThreadState::kActive, std::memory_order_release);
            return SpinResult::kEventsFound;
        }
        // Check for wakeups before doing anything else
        if (CheckAndDrainWakeup(thread_index)) {
            thread_state_[thread_index].store(ThreadState::kActive, std::memory_order_release);
            return SpinResult::kEventsFound; // Treat as event processed to re-check queue
        }
        // Check for new tasks
        auto task_opt = queue.PopNonBlocking();
        if (task_opt && *task_opt) {
            thread_state_[thread_index].store(ThreadState::kActive, std::memory_order_relaxed);
            return SpinResult::kTaskFound; // Task found, let main loop process
        }

        // No tasks, check for epoll events without blocking
        int epoll_fd = epoll_fds_[thread_index];
        int ready = epoll_wait(epoll_fd, events, kMaxEvents, 0);

        if (ready > 0) {
            thread_state_[thread_index].store(ThreadState::kActive, std::memory_order_relaxed);
            return SpinResult::kEventsFound; // Events found, let main loop process
        } else if (ready < 0 && errno != EINTR) {
            LOG_ERROR() << "epoll_wait failed during spinning: " << strerror(errno);
        }

        std::this_thread::yield();
    }
    thread_state_[thread_index].store(ThreadState::kActive, std::memory_order_release);
    return SpinResult::kSpinningFailed;
}

}  // namespace engine

USERVER_NAMESPACE_END

#endif  // __linux__