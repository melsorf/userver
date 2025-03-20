#ifdef __linux__
#include "epoll_event_dispatcher.hpp"

#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <fcntl.h>

#include <cstring>
#include <stdexcept>

#include <userver/logging/log.hpp>
#include <userver/utils/rand.hpp>

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

[[maybe_unused]] int CreateEventFd() {
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
            
            thread_notify_fds_[i] = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
            if (thread_notify_fds_[i] == -1) {
                throw std::runtime_error("Failed to create thread notification eventfd");
            }
            
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

void EpollEventDispatcher::NotifyTaskAdded() {
    // First check if there are any spinning threads that can take the task
    bool has_spinning_thread = false;
    for (size_t i = 0; i < thread_count_; ++i) {
        if (thread_spinning_[i].load(std::memory_order_acquire)) {
            has_spinning_thread = true;
            break;
        }
    }
    
    // If there's a spinning thread, it will see the task in its next queue check
    // without needing an explicit notification
    if (has_spinning_thread) {
        return;
    }
    
    // Find a sleeping thread to wake up
    uint64_t oldest_sleep_time = 0;
    size_t oldest_thread = 0;
    
    for (size_t i = 0; i < thread_count_; ++i) {
        uint64_t sleep_timestamp = thread_sleep_start_time_[i].load(std::memory_order_acquire);
        if (sleep_timestamp > 0 && sleep_timestamp > oldest_sleep_time) {
            oldest_sleep_time = sleep_timestamp;
            oldest_thread = i;
        }
    }
    
    // Wake up the thread that's been sleeping the longest
    if (oldest_sleep_time > 0) {
        PostEvent(oldest_thread);
    } else {
        // No sleeping threads, just pick one
        PostEvent(utils::RandRange(thread_count_));
    }
}

void EpollEventDispatcher::PostEvent(std::size_t thread_index) {
    if (thread_index >= thread_notify_fds_.size() || thread_notify_fds_[thread_index] < 0) {
        return;
    }

    uint64_t value = 1;
    ssize_t ret;
    do {
        ret = write(thread_notify_fds_[thread_index], &value, sizeof(value));
    } while (ret == -1 && errno == EINTR);

    if (ret != sizeof(value) && errno != EAGAIN && errno != EWOULDBLOCK) {
        LOG_ERROR() << "Failed to write to thread notification fd: " << strerror(errno);
    }
}

std::size_t EpollEventDispatcher::RegisterFd(
    int fd, uint32_t events, std::function<void(uint32_t)> callback) {
    if (fd < 0) return std::numeric_limits<std::size_t>::max();
    
    // For non-task FDs (like sockets) use edge-triggered mode
    struct epoll_event ev{};
    ev.events = events | EPOLLET; 
    
    // Choose a specific thread to handle this fd
    auto target_thread = utils::RandRange(thread_count_);
    
    bool registration_successful = false;
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        
        // Store callback info
        auto it = fd_callbacks_.find(fd);
        bool has_existing = (it != fd_callbacks_.end());
        
        if (has_existing) {
            // Update existing callback
            it->second.callback = std::move(callback);
            it->second.requested_events = events;
        } else {
            // Register new callback
            fd_callbacks_.emplace(fd, FdCallbackInfo{std::move(callback), events});
        }
        
        // Register with the chosen thread's epoll instance
        ev.data.fd = fd;
        int epoll_fd = thread_epoll_fds_[target_thread];
        
        int op = has_existing ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
        int result = epoll_ctl(epoll_fd, op, fd, &ev);
        
        if (result == -1) {
            if (errno == EEXIST && op == EPOLL_CTL_ADD) {
                result = epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
            }
            if (result == -1) {
                LOG_WARNING() << "Failed to " << (op == EPOLL_CTL_ADD ? "add" : "modify") 
                            << " fd " << fd << " to thread epoll: " << strerror(errno);
                
                // Remove the callback if we just added it
                if (!has_existing) {
                    fd_callbacks_.erase(fd);
                }
                return std::numeric_limits<std::size_t>::max();
            }
        }
        
        registration_successful = true;
    }
    
    return registration_successful ? target_thread : std::numeric_limits<std::size_t>::max();
}

void EpollEventDispatcher::UnregisterFd(int fd) {
    if (fd < 0) return;
    
    std::vector<size_t> threads_to_wakeup;
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        auto it = fd_callbacks_.find(fd);
        if (it == fd_callbacks_.end()) {
            return;
        }
        
        // Remove callback
        fd_callbacks_.erase(it);
        
        // Unregister from all thread epoll instances
        for (size_t i = 0; i < thread_count_; ++i) {
            int epoll_fd = thread_epoll_fds_[i];
            if (epoll_fd >= 0) {
                int result = epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                if (result == -1 && errno != ENOENT && errno != EBADF) {
                    LOG_ERROR() << "Failed to remove fd " << fd << " from thread epoll: " 
                                << strerror(errno);
                }
                
                // Only add to wakeup list if the removal succeeded
                if (result == 0) {
                    threads_to_wakeup.push_back(i);
                }
            }
        }
    }
    
    // Wake up affected threads
    for (auto thread_idx : threads_to_wakeup) {
        PostEvent(thread_idx);
    }
}

void EpollEventDispatcher::ProcessEvents(std::size_t thread_index, TaskQueue& queue, 
    std::shared_ptr<impl::TaskProcessorPools> pools) {
    if (thread_index >= thread_epoll_fds_.size() || thread_epoll_fds_[thread_index] < 0) {
        LOG_ERROR() << "Invalid epoll_fd for thread " << thread_index << ", falling back to regular ProcessTasks";
        return;
    }
    
    int epoll_fd = thread_epoll_fds_[thread_index];
    constexpr std::size_t kMaxEvents{256};
    struct epoll_event events[kMaxEvents];

    // Mark thread as spinning.
    thread_spinning_[thread_index].store(true, std::memory_order_release);

    while (!is_shutting_down_.load(std::memory_order_acquire)) {
        // Process tasks first (non-blocking)
        bool processed_task = false;
        while (auto context_ptr = queue.PopNonBlocking()) {
            processed_task = true;
            if (!context_ptr.has_value()) {
                // "Stop" token
                is_shutting_down_.store(true, std::memory_order_release);
                break;
            }
            if (!context_ptr.value()) continue;
            auto context = context_ptr.value();
            bool has_failed = false;
            
            // Process task
            try {
                auto& task_counter = context->GetTaskCounter();
                impl::TaskCounter::RunningToken run_token{task_counter};
                context->DoStep();
            } catch (...) {
                LOG_ERROR() << "Unhandled exception from DoStep()";
                has_failed = true;
            }
            pools->GetCoroPool().AccountStackUsage();
            
            if (has_failed || context->IsFinished()) {
                context->FinishDetached();
            }
        }

        if (is_shutting_down_.load(std::memory_order_acquire)) break;
        if (processed_task) continue;

        // Check briefly before going to sleep.
        thread_spinning_[thread_index].store(true, std::memory_order_release);
        utils::FastScopeGuard spinning_guard([&] () noexcept {
            thread_spinning_[thread_index].store(false, std::memory_order_release);
        });
        constexpr int kSpinCount = 100;
        for (int i = 0; i < kSpinCount; ++i) {
            if (queue.GetSizeApproximate() > 0 || is_shutting_down_.load(std::memory_order_acquire)) {
                break;  // There's work to do, don't sleep
            }
            std::this_thread::yield();
        }
        
        if (queue.GetSizeApproximate() > 0 || is_shutting_down_.load(std::memory_order_acquire)) {
            continue;
        }
        
        // Mark thread as sleeping
        thread_sleep_start_time_[thread_index].store(
            std::chrono::steady_clock::now().time_since_epoch().count(), std::memory_order_release);
        
        // Final check before epoll_wait
        if (queue.GetSizeApproximate() > 0 || is_shutting_down_.load(std::memory_order_acquire)) {
            thread_sleep_start_time_[thread_index].store(0, std::memory_order_release);
            continue;
        }
        
        // Ensure visibility of changes before sleeping
        std::atomic_thread_fence(std::memory_order_seq_cst);
        
        int ready = epoll_wait(epoll_fd, events, kMaxEvents, -1);
        thread_sleep_start_time_[thread_index].store(0, std::memory_order_release);
        
        if (is_shutting_down_.load(std::memory_order_acquire)) break;
        
        if (ready < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR() << "epoll_wait failed: " << strerror(errno);
            break;
        }
        
        // Process events returned by epoll.
        for (int i = 0; i < ready && !is_shutting_down_.load(std::memory_order_acquire); ++i) {
            const auto fd = events[i].data.fd;
            
            if (fd == thread_notify_fds_[thread_index]) {
                // Thread notification - drain the eventfd completely
                uint64_t buffer;
                while (true) {
                    ssize_t ret = read(thread_notify_fds_[thread_index], &buffer, sizeof(buffer));
                    if (ret < 0) {
                        if (errno != EAGAIN && errno != EWOULDBLOCK) {
                            LOG_ERROR() << "Failed to read from thread notification fd: " << strerror(errno);
                        }
                        break;
                    }
                }
                continue;
            }
            
            // Handle other fd events (I/O, etc.)
            FdCallbackInfo callback_info;
            {
                std::lock_guard<std::mutex> lock(fd_mutex_);
                auto it = fd_callbacks_.find(fd);
                if (it != fd_callbacks_.end()) {
                    callback_info = it->second;
                }
            }
            
            if (callback_info.callback) {
                uint32_t event_mask = events[i].events & (EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP);
                if (event_mask) {
                    try {
                        callback_info.callback(event_mask);
                    } catch (const std::exception& ex) {
                        LOG_ERROR() << "Exception in fd callback: " << ex.what();
                    } catch (...) {
                        LOG_ERROR() << "Unknown exception in fd callback";
                    }
                }
            } else {
                LOG_DEBUG() << "Event received for fd " << fd << " with no registered callback";
            }
        }
    }
}

}  // namespace engine

USERVER_NAMESPACE_END

#endif  // __linux__