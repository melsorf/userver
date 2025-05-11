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
    int fd, uint32_t events, std::function<void(uint32_t)> callback, 
    std::weak_ptr<void> owner) {
    if (fd < 0) {
        LOG_WARNING() << "Attempt to register invalid fd " << fd;
        return std::numeric_limits<std::size_t>::max();
    }
    
    if (!callback) {
        LOG_WARNING() << "Attempt to register fd " << fd << " with null callback";
        return std::numeric_limits<std::size_t>::max();
    }
    
    struct epoll_event ev{};
    ev.events = events | EPOLLET; 
    
    // Choose a specific thread to handle this fd
    auto target_thread = utils::RandRange(thread_count_);
    
    bool registration_successful = false;
    {
        std::unique_lock<std::mutex> fd_lock(fd_mutex_);
        
        // Store callback info
        auto it = fd_callbacks_.find(fd);
        bool has_existing = (it != fd_callbacks_.end());
        if (has_existing && it->second.owner_thread != target_thread) {
            target_thread = it->second.owner_thread;
        }
        
        if (has_existing) {
            // Update existing callback
            it->second.callback = std::move(callback);
            it->second.requested_events = events;
        } else {
            // Register new callback
            fd_callbacks_.emplace(fd, FdCallbackInfo{std::move(callback), events, target_thread});
        }
        
        // Register with the chosen thread's epoll instance
        ev.data.fd = fd;
        int epoll_fd = thread_epoll_fds_[target_thread];
        
        int op = has_existing ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
        int result = epoll_ctl(epoll_fd, op, fd, &ev);
        
        if (result == -1) {
            if (errno == EEXIST && op == EPOLL_CTL_ADD) {
                // If fd was already registered, try to modify it
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
        fd_lock.unlock();
        
        // Register owner if provided (using a separate mutex to avoid deadlocks)
        if (registration_successful && owner.lock()) {
            std::lock_guard<std::mutex> registry_lock(registry_mutex_);
            fd_to_owner_[fd] = std::move(owner);
        }
    }
    
    // Notify the target thread about new registration
    if (registration_successful) {
        PostEvent(target_thread);
    }
    
    return registration_successful ? target_thread : std::numeric_limits<std::size_t>::max();
}

void EpollEventDispatcher::UnregisterFd(int fd) {
    if (fd < 0) return;
    
    size_t owner_thread = std::numeric_limits<size_t>::max();
    bool need_wakeup = false;
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        auto it = fd_callbacks_.find(fd);
        if (it == fd_callbacks_.end()) {
            return;
        }
        owner_thread = it->second.owner_thread;
        // Remove callback
        fd_callbacks_.erase(it);
        
        if (owner_thread < thread_count_) {
            int epoll_fd = thread_epoll_fds_[owner_thread];
            if (epoll_fd >= 0) {
                int result = epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                if (result == -1) {
                    if (errno != ENOENT && errno != EBADF) {
                        LOG_ERROR() << "Failed to remove fd " << fd << " from thread epoll: " << strerror(errno);
                    }
                } else {
                    // Succeeded
                    need_wakeup = true;
                }
            }
        }
    }

    // Remove from owner registry
    {
        std::lock_guard<std::mutex> registry_lock(registry_mutex_);
        fd_to_owner_.erase(fd);
    }
    
    // Wake up thread
    if (need_wakeup && owner_thread < thread_count_) {
        PostEvent(owner_thread);
    }
}

std::optional<std::size_t> EpollEventDispatcher::SelectThreadToWakeup() {
    // First pass: check for spinning threads.
    for (size_t i = 0; i < thread_count_; ++i) {
        if (thread_spinning_[i].load(std::memory_order_acquire)) {
            // One of the threads in the spin state will take the task itself
            return std::nullopt;
        }
    }
    
    // Second pass: check for sleeping threads.
    for (size_t i = 0; i < thread_count_; ++i) {
        auto sleep_timestamp = thread_sleep_start_time_[i].load(std::memory_order_acquire);
        // Skip threads with uninitialized timestamps
        if (sleep_timestamp == 0) {
            continue;
        }
        if (thread_sleep_start_time_[i].compare_exchange_strong(sleep_timestamp, 0, std::memory_order_acq_rel)) {
            return i;
        }
    }
    return std::nullopt;
}

void EpollEventDispatcher::ProcessEvents(std::size_t thread_index, TaskQueue& queue,
    std::shared_ptr<impl::TaskProcessorPools> pools) {
    const int epfd = thread_epoll_fds_[thread_index];
    constexpr int kSpin = 1000;
    constexpr int kMaxEvents = 64;
    epoll_event events[kMaxEvents];
    while (!is_shutting_down_.load(std::memory_order_acquire)) {
        bool did_task = false;
        while (auto opt = queue.PopNonBlocking()) {
            did_task = true;
            if (!opt.value()) { is_shutting_down_.store(true, std::memory_order_release); break; }
            auto ctx = std::move(opt.value());
            impl::TaskCounter::RunningToken tk{ctx->GetTaskCounter()};
            ctx->DoStep();
            pools->GetCoroPool().AccountStackUsage();
            if (ctx->IsFinished()) ctx->FinishDetached();
        }
        if (is_shutting_down_.load()) break;
        if (did_task) continue;

        thread_spinning_[thread_index].store(true, std::memory_order_release);
        for (int i = 0; i < kSpin; ++i) {
            if (queue.GetSizeApproximate() || is_shutting_down_.load()) break;
            std::this_thread::yield();
        }
        thread_spinning_[thread_index].store(false, std::memory_order_release);
        if (queue.GetSizeApproximate() || is_shutting_down_.load()) continue;

        thread_sleep_start_time_[thread_index].store(
        std::chrono::steady_clock::now().time_since_epoch().count(),
        std::memory_order_release);
        int n = epoll_wait(epfd, events, kMaxEvents, -1);

        thread_sleep_start_time_[thread_index].store(0, std::memory_order_release);

        if (n < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR() << "epoll_wait failed: " << strerror(errno);
            break;
        }

        for (int i = 0; i < n && !is_shutting_down_.load(); ++i) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;

            if (fd == thread_notify_fds_[thread_index]) {
                uint64_t cnt;
                while (read(fd, &cnt, sizeof(cnt)) == sizeof(cnt)) {}
                continue;
            }

            if (ev & (EPOLLERR | EPOLLHUP)) {
                std::lock_guard lock(fd_mutex_);
                auto it = fd_callbacks_.find(fd);
                if (it != fd_callbacks_.end()) {
                    it->second.callback(ev);
                }
                continue;
            }

            FdCallbackInfo info;
            bool forward = false;
            {
                std::lock_guard lock(fd_mutex_);
                auto it = fd_callbacks_.find(fd);
                if (it != fd_callbacks_.end()) {
                    if (it->second.owner_thread == thread_index) {
                        info = it->second;
                    } else {
                        forward = true;
                    }
                }
            }
            if (forward) {
                PostEvent(info.owner_thread);
                continue;
            }
            if (info.callback) {
            info.callback(ev & (EPOLLIN|EPOLLOUT));
            }
        }
    }
}

}  // namespace engine

USERVER_NAMESPACE_END

#endif  // __linux__