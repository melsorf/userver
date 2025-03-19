#ifdef __linux__
#include "epoll_event_dispatcher.hpp"

#include <unistd.h>
#include <cstring>
#include <stdexcept>
#include <fcntl.h>
#include <map>
#include <chrono>
#include <queue>
#include <functional>
#include <mutex>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>

#include <userver/logging/log.hpp>
#include <userver/utils/rand.hpp>

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

int CreateTimerFd() {
    int fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (fd == -1) {
        throw std::runtime_error("Failed to create timerfd: " + 
                                std::string(strerror(errno)));
    }
    return fd;
}

void CloseIfValid(int& fd) {
    if (fd >= 0) {
        close(fd);
        fd = -1;
    }
}
}  // namespace

EpollEventDispatcher::EpollEventDispatcher(size_t thread_count)
    : thread_count_(thread_count) {
    try {
        epoll_fd_ = CreateEpollFd();
        task_event_fd_ = CreateEventFd();
        timer_fd_ = CreateTimerFd();

        // Create and initialize thread state arrays
        thread_spinning_ = std::make_unique<std::atomic<bool>[]>(thread_count_);
        thread_sleep_start_time_ = std::make_unique<std::atomic<uint64_t>[]>(thread_count_);
        for (size_t i = 0; i < thread_count_; ++i) {
            thread_spinning_[i].store(false, std::memory_order_relaxed);
            thread_sleep_start_time_[i].store(0, std::memory_order_relaxed);
        }

        // Register task notification eventfd in level-triggered mode
        struct epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = task_event_fd_;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, task_event_fd_, &ev) == -1) {
            throw std::runtime_error("Failed to add task_event_fd to epoll: " + std::string(strerror(errno)));
        }

        // Register timer fd
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = timer_fd_;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, timer_fd_, &ev) == -1) {
            throw std::runtime_error("Failed to add timer_fd to epoll: " + std::string(strerror(errno)));
        }
    } catch (...) {
        // Clean up in case of failure
        CloseIfValid(timer_fd_);
        CloseIfValid(task_event_fd_);
        CloseIfValid(epoll_fd_);
        throw;
    }
}

EpollEventDispatcher::~EpollEventDispatcher() {
    Shutdown();
    CloseIfValid(timer_fd_);
    CloseIfValid(task_event_fd_);
    CloseIfValid(epoll_fd_);
}

void EpollEventDispatcher::Shutdown() {
    is_shutting_down_.store(true, std::memory_order_release);
    // Wake up all threads by writing a wakeup count into the eventfd
    const uint64_t wakeup_count = thread_count_;
    ssize_t ret = write(task_event_fd_, &wakeup_count, sizeof(wakeup_count));
    if (ret != sizeof(wakeup_count) && errno != EAGAIN && errno != EWOULDBLOCK) {
        LOG_ERROR() << "Shutdown: Failed to write wakeup_count to task_event_fd: " << strerror(errno);
    }
}

void EpollEventDispatcher::PostEvent() {
    if (task_event_fd_ < 0) return;
    uint64_t value = 1;
    ssize_t ret;
    do {
        ret = write(task_event_fd_, &value, sizeof(value));
    } while (ret == -1 && errno == EINTR);

    if (ret != sizeof(value) && errno != EAGAIN && errno != EWOULDBLOCK) {
        LOG_ERROR() << "Failed to write to task_event_fd: " << strerror(errno);
    }
    // Wake up one worker thread.
    WakeupWorkerThread(SelectThreadToWakeup());
}

void EpollEventDispatcher::UpdateTimerFd() {
    struct itimerspec new_value{};
    if (!timers_.empty()) {
        auto now = std::chrono::steady_clock::now();
        auto earliest = timers_.begin()->first;
        
        if (earliest > now) {
            auto delay = earliest - now;
            new_value.it_value.tv_sec = std::chrono::duration_cast<std::chrono::seconds>(delay).count();
            new_value.it_value.tv_nsec = 
                std::chrono::duration_cast<std::chrono::nanoseconds>(delay).count() % 1000000000;
        } else {
            // Timer already expired, set to minimal value
            new_value.it_value.tv_nsec = 1;
        }
    }
    
    timerfd_settime(timer_fd_, 0, &new_value, nullptr);
}

bool EpollEventDispatcher::ScheduleTimer(std::chrono::steady_clock::duration delay, std::function<void()> callback) {
    if (timer_fd_ < 0) return false;

    auto deadline = std::chrono::steady_clock::now() + delay;
    {
        std::lock_guard<std::mutex> lock(timers_mutex_);
        timers_.emplace(deadline, std::move(callback));
        UpdateTimerFd();
    }
    // Wake up a thread to make sure the timer is properly processed
    PostEvent();
    return true;
}

std::size_t EpollEventDispatcher::RegisterFd(
    int fd, uint32_t events, std::function<void(uint32_t)> callback) {
    if (fd < 0) return std::numeric_limits<std::size_t>::max();
    // For non-task FDs (like sockets) use edge-triggered mode
    struct epoll_event ev{};
    ev.events = events | EPOLLET; 
    ev.data.fd = fd;

    bool registration_successful = false;
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        
        int op = EPOLL_CTL_ADD;
        auto it = fd_callbacks_.find(fd);
        bool has_existing = (it != fd_callbacks_.end());
        
        if (has_existing) {
            op = EPOLL_CTL_MOD;
            // Store the events that this fd is interested in
            it->second.requested_events = events;
        }
        
        int result = epoll_ctl(epoll_fd_, op, fd, &ev);
        if (result == -1) {
            if (errno == EEXIST && op == EPOLL_CTL_ADD) {
                result = epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
            }
            if (result == -1) {
                LOG_WARNING() << "Failed to " << (op == EPOLL_CTL_ADD ? "add" : "modify") 
                            << " fd " << fd << " to unified epoll: " << strerror(errno);
                return std::numeric_limits<std::size_t>::max();
            }
        }

        if (has_existing) {
            it->second.callback = std::move(callback);
        } else {
            fd_callbacks_.emplace(fd, FdCallbackInfo{std::move(callback), events});
        }
        registration_successful = true;
    }
    // Thread selection for this fd - using a random approach for even distribution
    return registration_successful ? utils::RandRange(thread_count_) 
                                   : std::numeric_limits<std::size_t>::max();
}

void EpollEventDispatcher::UnregisterFd(int fd) {
    if (fd < 0) return;
    bool needs_wakeup = false;
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        auto it = fd_callbacks_.find(fd);
        if (it != fd_callbacks_.end()) {
            fd_callbacks_.erase(it);
            needs_wakeup = true;
        }
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) == -1) {
            if (errno != ENOENT && errno != EBADF) {
                LOG_ERROR() << "Failed to remove fd " << fd << " from unified epoll: " 
                           << strerror(errno);
            }
        }
    }
    if (needs_wakeup) {
        PostEvent();
    }
}

void EpollEventDispatcher::ProcessTimerEvents() {
    if (timer_fd_ < 0) return;
    // Drain the timer fd
    uint64_t expirations;
    ssize_t ret;
    do {
        ret = read(timer_fd_, &expirations, sizeof(expirations));
    } while (ret > 0);
    
    if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        LOG_ERROR() << "Failed to read from timer_fd: " << strerror(errno);
    }

    auto now = std::chrono::steady_clock::now();
    std::vector<std::function<void()>> expired_callbacks;
    {
        std::lock_guard<std::mutex> lock(timers_mutex_);
        
        // Find all expired timers
        auto it = timers_.begin();
        while (it != timers_.end() && it->first <= now) {
            expired_callbacks.push_back(std::move(it->second));
            it = timers_.erase(it);
        }
        // Reset the timer for the next deadline
        UpdateTimerFd();
    }
    // Execute callbacks
    for (auto& callback : expired_callbacks) {
        try {
            callback();
        } catch (const std::exception& ex) {
            LOG_ERROR() << "Exception in timer callback: " << ex.what();
        } catch (...) {
            LOG_ERROR() << "Unknown exception in timer callback";
        }
    }
}

std::size_t EpollEventDispatcher::SelectThreadToWakeup() {
    // First pass: check for spinning threads.
    for (size_t i = 0; i < thread_count_; ++i) {
        bool expected = true;
        if (thread_spinning_[i].compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
            return i;
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
    // No spinning or sleeping threads found
    // Use a random approach
    static std::atomic<size_t> next_thread_index{0};
    return next_thread_index.fetch_add(1, std::memory_order_relaxed) % thread_count_;
}

void EpollEventDispatcher::WakeupWorkerThread(std::size_t thread_index) {
    uint64_t expected_sleep_time = thread_sleep_start_time_[thread_index].load(std::memory_order_acquire);
    if (expected_sleep_time != 0) {
        thread_sleep_start_time_[thread_index].compare_exchange_strong(
            expected_sleep_time, 0, std::memory_order_acq_rel);
    }
}

void EpollEventDispatcher::ProcessEvents(std::size_t thread_index, TaskQueue& queue) {
    if (epoll_fd_ < 0) {
        LOG_ERROR() << "Invalid epoll_fd, falling back to regular ProcessTasks";
        return;
    }
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
                impl::TaskCounter::RunningToken run_token{context->GetTaskProcessor().GetTaskCounter()};
                context->DoStep();
            } catch (...) {
                LOG_ERROR() << "Unhandled exception from DoStep()";
                has_failed = true;
            }
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
        int ready = epoll_wait(epoll_fd_, events, kMaxEvents, -1);
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
            if (fd == task_event_fd_) {
                uint64_t buffer[8];
                ssize_t ret;
                do {
                    ret = read(task_event_fd_, &buffer, sizeof(buffer));
                } while (ret > 0);
                
                if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                    LOG_ERROR() << "Failed to read from task_event_fd: " << strerror(errno);
                }
                continue; 
            } else if (fd == timer_fd_) {
                ProcessTimerEvents();
            } else {
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
                    
                    // Only rearm if not EPOLLHUP or EPOLLERR
                    if (!(event_mask & (EPOLLHUP | EPOLLERR))) {
                        std::lock_guard<std::mutex> lock(fd_mutex_);
                        auto it = fd_callbacks_.find(fd);
                        if (it != fd_callbacks_.end()) {
                            struct epoll_event ev;
                            // Preserve the original requested events
                            ev.events = it->second.requested_events | EPOLLET;
                            ev.data.fd = fd;
                            if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev) == -1) {
                                if (errno != EBADF && errno != ENOENT) {
                                    LOG_ERROR() << "Failed to rearm fd " << fd << ": " << strerror(errno);
                                }
                            }
                        }
                    }
                } else {
                    LOG_DEBUG() << "Event received for fd " << fd << " with no registered callback";
                }
            }
        }
    }
}

}  // namespace engine

USERVER_NAMESPACE_END

#endif __linux__