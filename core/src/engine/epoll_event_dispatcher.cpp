#ifdef __linux__
#include "epoll_event_dispatcher.hpp"

#include <sys/eventfd.h>
#include <unistd.h>
#include <fcntl.h>

#include <cstring>
#include <stdexcept>

#include <fmt/core.h>
#include <userver/logging/log.hpp>

#include <engine/task/task_context.hpp>
#include <engine/task/task_counter.hpp>
#include <engine/task/task_processor.hpp> 
#include <engine/task/task_processor_pools.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

namespace {
int CreateEpollFd() {
    int fd = epoll_create1(EPOLL_CLOEXEC);
    if (fd == -1) {
        throw std::runtime_error("Failed to create epoll instance: " + 
                                std::string(strerror(errno)));
    }
    return fd;
}

int CreateEventFd() {
    int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd == -1) {
        throw std::runtime_error("Failed to create eventfd: " + 
                                std::string(strerror(errno)));
    }
    return fd;
}
}  // namespace

EpollEventDispatcher::EpollEventDispatcher(size_t thread_count)
    : thread_count_(thread_count) {
    try {
        thread_epoll_fds_.resize(thread_count_);
        thread_notify_fds_.resize(thread_count_);
        
        for (size_t i = 0; i < thread_count_; ++i) {
            thread_epoll_fds_[i] = CreateEpollFd();
            thread_notify_fds_[i] = CreateEventFd();
            
            struct epoll_event ev{};
            ev.events = EPOLLIN | EPOLLET;
            ev.data.fd = thread_notify_fds_[i];
            
            if (epoll_ctl(thread_epoll_fds_[i], EPOLL_CTL_ADD, thread_notify_fds_[i], &ev) == -1) {
                throw std::runtime_error("Failed to add notification fd to thread epoll");
            }
        }
        
        thread_spinning_ = std::make_unique<std::atomic<bool>[]>(thread_count_);
        thread_sleep_start_time_ = std::make_unique<std::atomic<uint64_t>[]>(thread_count_);
        
        for (size_t i = 0; i < thread_count_; ++i) {
            thread_spinning_[i].store(false, std::memory_order_relaxed);
            thread_sleep_start_time_[i].store(0, std::memory_order_relaxed);
        }
    } catch (...) {
        for (int fd : thread_epoll_fds_) {
            if (fd >= 0) close(fd);
        }
        for (int fd : thread_notify_fds_) {
            if (fd >= 0) close(fd);
        }
        throw;
    }
}

EpollEventDispatcher::~EpollEventDispatcher() {
    Shutdown();
    
    for (int fd : thread_epoll_fds_) {
        if (fd >= 0) close(fd);
    }
    for (int fd : thread_notify_fds_) {
        if (fd >= 0) close(fd);
    }
}

void EpollEventDispatcher::Shutdown() {
    is_shutting_down_.store(true, std::memory_order_release);
    
    // Wake up all threads
    for (size_t i = 0; i < thread_count_; ++i) {
        PostEvent(i);
    }
}

void EpollEventDispatcher::PostEvent() {
    // Wake up a single worker thread by choosing the best candidate
    const auto thread_to_wake_up = SelectThreadToWakeup();
    if (thread_to_wake_up != std::nullopt) {
        PostEvent(*thread_to_wake_up);
    }
}

void EpollEventDispatcher::PostEvent(std::size_t thread_index) {
    if (is_shutting_down_.load(std::memory_order_relaxed) || thread_index >= thread_count_) {
        return;
    }

    int fd_to_notify = thread_notify_fds_[thread_index];
    if (fd_to_notify >= 0) {
        uint64_t increment = 1;
        ssize_t bytes_written = write(fd_to_notify, &increment, sizeof(increment));
        if (bytes_written < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) { // EAGAIN/EWOULDBLOCK is fine, means it's already signaled
                LOG_WARNING() << "Failed to write to notify_fd " << fd_to_notify
                              << " for thread " << thread_index << ": " << strerror(errno);
            }
        }
    }
}

std::size_t EpollEventDispatcher::RegisterFd(
    int fd, uint32_t events, std::function<void(uint32_t)> callback,
    std::weak_ptr<void> owner_weak_ptr) {
    if (is_shutting_down_.load(std::memory_order_relaxed) || thread_count_ == 0) {
        return std::numeric_limits<std::size_t>::max();
    }
    // Select a thread for this FD, e.g., based on fd % thread_count_
    // This FdCallbackInfo.owner_thread is the thread that will handle epoll events for this fd
    std::size_t target_thread_idx = static_cast<std::size_t>(fd) % thread_count_;
    int target_epoll_fd = thread_epoll_fds_[target_thread_idx];

    struct epoll_event ev{};
    ev.events = events | EPOLLET;
    ev.data.fd = fd;

    if (epoll_ctl(target_epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        LOG_ERROR() << "Failed to add fd " << fd << " to epoll_fd " << target_epoll_fd 
                    << " for thread " << target_thread_idx << ": " << strerror(errno);
        return std::numeric_limits<std::size_t>::max();
    }

    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        fd_callbacks_[fd] = {std::move(callback), events, target_thread_idx};
    }
    {
        std::lock_guard<std::mutex> registry_lock(registry_mutex_);
        fd_to_owner_[fd] = owner_weak_ptr;
    }
    
    return static_cast<std::size_t>(fd);
}

void EpollEventDispatcher::UnregisterFd(int fd) {
    if (is_shutting_down_.load(std::memory_order_relaxed) || thread_count_ == 0) return;

    std::optional<std::size_t> owner_thread_idx;
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        auto it = fd_callbacks_.find(fd);
        if (it != fd_callbacks_.end()) {
            owner_thread_idx = it->second.owner_thread;
            fd_callbacks_.erase(it);
        }
    }
    {
        std::lock_guard<std::mutex> registry_lock(registry_mutex_);
        fd_to_owner_.erase(fd);
    }

    if (owner_thread_idx && *owner_thread_idx < thread_count_) {
        int target_epoll_fd = thread_epoll_fds_[*owner_thread_idx];
        struct epoll_event ev{};
        if (epoll_ctl(target_epoll_fd, EPOLL_CTL_DEL, fd, &ev) == -1) {
            if (errno != ENOENT && errno != EBADF) { // ENOENT/EBADF is fine if already removed or fd closed
                 LOG_WARNING() << "Failed to remove fd " << fd << " from epoll_fd " << target_epoll_fd
                               << " for thread " << *owner_thread_idx << ": " << strerror(errno);
            }
        }
    } else {
        // FD not found in callbacks, or invalid owner_thread. Might have been unregistered already
        LOG_TRACE() << "FD " << fd << " not found in fd_callbacks_ or invalid owner thread during UnregisterFd.";
    }
}

std::optional<std::size_t> EpollEventDispatcher::SelectThreadToWakeup() {
    // Strategy:
    // 1. Sleeping thread (longest sleep first / any sleeping)
    // 2. Spinning thread (about to sleep)
    // 3. Any thread (round robin) if all are busy or none selected by above
    
    std::optional<std::size_t> best_sleeper_idx;
    uint64_t earliest_sleep_time = std::numeric_limits<uint64_t>::max();

    for (size_t i = 0; i < thread_count_; ++i) {
        uint64_t sleep_start = thread_sleep_start_time_[i].load(std::memory_order_relaxed);
        if (sleep_start != 0) { // Thread is in epoll_wait
            if (sleep_start < earliest_sleep_time) {
                earliest_sleep_time = sleep_start;
                best_sleeper_idx = i;
            }
        }
    }
    if (best_sleeper_idx) {
        return best_sleeper_idx;
    }

    for (size_t i = 0; i < thread_count_; ++i) {
        if (thread_spinning_[i].load(std::memory_order_relaxed)) {
            return i;
        }
    }
    
    // If no one is sleeping or spinning, pick one randomly
    // This thread might be busy, but the eventfd write will be noticed when it next epoll_waits.
    if (thread_count_ > 0) {
        static std::atomic<size_t> next_thread_rand{0};
        return (next_thread_rand.fetch_add(1, std::memory_order_relaxed)) % thread_count_;
    }

    return std::nullopt;
}

void EpollEventDispatcher::ProcessEvents(std::size_t thread_index, TaskQueue& queue, 
    std::shared_ptr<impl::TaskProcessorPools> pools) {
    if (thread_index >= thread_epoll_fds_.size() || thread_epoll_fds_[thread_index] < 0) {
        LOG_ERROR() << "Invalid epoll_fd for thread " << thread_index << ", falling back to regular ProcessTasks";
        return;
    }
    
    int epoll_fd = thread_epoll_fds_[thread_index];
    int notify_fd_for_this_thread = thread_notify_fds_[thread_index];

    auto& task_processor = engine::current_task::GetTaskProcessor();

    constexpr std::size_t kMaxEvents = 64;
    struct epoll_event events[kMaxEvents];

    while (!is_shutting_down_.load(std::memory_order_relaxed)) {
        bool processed_task_in_main_loop_iteration = false;
        // Process tasks from queue first
        while (true) {
            auto context_ptr_opt = queue.PopNonBlocking();
            if (!context_ptr_opt) { // No more tasks or queue empty
                break;
            }
            
            processed_task_in_main_loop_iteration = true;
            if (!context_ptr_opt.has_value()) {
                // Stop token
                is_shutting_down_.store(true, std::memory_order_release);
                break;
            }
            engine::impl::TaskContext& context = *context_ptr_opt.value();
            task_processor.CheckWaitTime(context);

            bool has_failed = false;
            
            // Process task
            try {
                engine::impl::TaskCounter::RunningToken token{task_processor.GetTaskCounter()};
                context.DoStep();
            } catch (...) {
                LOG_ERROR() << "Unhandled exception from DoStep()";
                has_failed = true;
            }
            pools->GetCoroPool().AccountStackUsage();
            
            if (has_failed || context.IsFinished()) {
                context.FinishDetached();
            }
        }

        if (is_shutting_down_.load(std::memory_order_relaxed)) break;
        if (processed_task_in_main_loop_iteration) continue;

        // Spin before sleeping
        thread_spinning_[thread_index].store(true, std::memory_order_relaxed);
        bool task_found_during_spin = false;
        constexpr int kSpinIterations = 1000; // TODO: Make configurable
        for (int k = 0; k < kSpinIterations; ++k) {
            auto context_ptr_opt = queue.PopNonBlocking();
            if (context_ptr_opt) {
                if (!context_ptr_opt.has_value()) { // Stop token
                    is_shutting_down_.store(true, std::memory_order_relaxed);
                } else {
                    engine::impl::TaskContext& context = *context_ptr_opt.value();
                    task_processor.CheckWaitTime(context);
                    bool has_failed = false;
                    try {
                        engine::impl::TaskCounter::RunningToken token{task_processor.GetTaskCounter()};
                        context.DoStep();
                    } catch (const std::exception& ex) {
                        LOG_ERROR() << "Uncaught exception from DoStep in spin for task_id="
                                    << logging::HexShort(context.GetTaskId()) << ": " << ex;
                        has_failed = true;
                    }
                    pools->GetCoroPool().AccountStackUsage();
                    if (has_failed || context.IsFinished()) {
                        context.FinishDetached();
                    }
                }
                task_found_during_spin = true;
                break; 
            }
            if (is_shutting_down_.load(std::memory_order_relaxed)) break;
            std::this_thread::yield();
        }
        thread_spinning_[thread_index].store(false, std::memory_order_relaxed);
        if (task_found_during_spin || is_shutting_down_.load(std::memory_order_relaxed)) {
            continue;
        }
        
        // Prepare to sleep (epoll_wait)
        thread_sleep_start_time_[thread_index].store(
            std::chrono::steady_clock::now().time_since_epoch().count(), std::memory_order_relaxed);
        
        // Final check before epoll_wait
        if (queue.GetSizeApproximate() > 0 || is_shutting_down_.load(std::memory_order_acquire)) {
            thread_sleep_start_time_[thread_index].store(0, std::memory_order_relaxed);
            continue;
        }
        int ready_fds = epoll_wait(epoll_fd, events, kMaxEvents, -1);
        thread_sleep_start_time_[thread_index].store(0, std::memory_order_relaxed);
        
        if (is_shutting_down_.load(std::memory_order_acquire)) break;
        
        if (ready_fds < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR() << "epoll_wait failed: " << strerror(errno);
            break;
        }
        
        for (int i = 0; i < ready_fds; ++i) {
            if (is_shutting_down_.load(std::memory_order_relaxed)) break;

            int current_fd = events[i].data.fd;
            uint32_t event_mask = events[i].events;

            if (current_fd == notify_fd_for_this_thread) {
                uint64_t value;
                while (true) { // Drain the eventfd
                    ssize_t read_rc = read(notify_fd_for_this_thread, &value, sizeof(value));
                    if (read_rc < 0) {
                        if (errno != EAGAIN && errno != EWOULDBLOCK) {
                            LOG_WARNING() << "Failed to read from notify_fd " << notify_fd_for_this_thread
                                          << " for thread " << thread_index << ": " << strerror(errno);
                        }
                        break; // Done draining or error
                    }
                    if (read_rc == 0) break; // Should not happen for eventfd
                }
            } else {
                FdCallbackInfo cb_info_copy;
                bool found_cb = false;
                {
                    std::lock_guard<std::mutex> lock(fd_mutex_);
                    auto it = fd_callbacks_.find(current_fd);
                    if (it != fd_callbacks_.end()) {
                        if (it->second.owner_thread == thread_index) { // Should always be true for per-thread epoll
                            cb_info_copy = it->second;
                            found_cb = true;
                        } else {
                             LOG_WARNING() << "FD " << current_fd << " event on thread " << thread_index
                                          << " but FdCallbackInfo owner_thread is " << it->second.owner_thread;
                        }
                    }
                }

                if (found_cb && cb_info_copy.callback) {
                    bool should_call = true;
                    int fd_to_unregister_due_to_owner_expiry = -1;
                    {
                        std::lock_guard<std::mutex> registry_lock(registry_mutex_);
                        auto owner_it = fd_to_owner_.find(current_fd);
                        if (owner_it != fd_to_owner_.end() && owner_it->second.expired()) {
                            should_call = false;
                            fd_to_unregister_due_to_owner_expiry = current_fd;
                        }
                    }

                    if (fd_to_unregister_due_to_owner_expiry >= 0) {
                        LOG_DEBUG() << "Owner of fd " << fd_to_unregister_due_to_owner_expiry
                                    << " expired, unregistering from epoll dispatcher.";
                        UnregisterFd(fd_to_unregister_due_to_owner_expiry);
                    }
                    
                    if (should_call) {
                        try {
                            uint32_t relevant_events = event_mask & (EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP | EPOLLRDHUP);
                            if (relevant_events) {
                                cb_info_copy.callback(relevant_events);
                            }
                        } catch (const std::exception& e) {
                            LOG_ERROR() << "Exception in FD (" << current_fd << ") callback on thread "
                                        << thread_index << ": " << e.what();
                        } catch (...) {
                            LOG_ERROR() << "Unknown exception in FD (" << current_fd << ") callback on thread "
                                        << thread_index;
                        }
                    }
                } else if (!found_cb) {
                    LOG_TRACE() << "No callback found for fd " << current_fd << " on thread " << thread_index;
                     if (current_fd != notify_fd_for_this_thread) {
                        struct epoll_event dummy_ev{};
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, current_fd, &dummy_ev);
                     }
                }
            }
        }
    }
}

}  // namespace engine

USERVER_NAMESPACE_END

#endif  // __linux__