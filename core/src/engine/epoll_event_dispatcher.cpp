#ifdef __linux__

#include "epoll_event_dispatcher.hpp"

#include <cerrno>
#include <chrono>
#include <system_error>

#include <userver/engine/sleep.hpp>
#include <userver/utils/assert.hpp>

#include <engine/task/task_context.hpp>  
#include <engine/task/task_counter.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

namespace {

constexpr int kMaxEvents = 64;
constexpr auto kMinSpinningTime = std::chrono::milliseconds(1);
constexpr uint64_t kEpollInData = 0x1000000000000000ULL;
constexpr uint64_t kEventFdMask = 0x0FFFFFFFFFFFFFFFULL;

// Converts nanoseconds to uint64_t for timestamps
uint64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Read from an eventfd to clear the notification
void ConsumeEvent(int eventfd) {
    uint64_t value;
    ssize_t res = read(eventfd, &value, sizeof(value));
    if (res != sizeof(value) && errno != EAGAIN) {
        LOG_ERROR() << "Failed to read from eventfd: " << strerror(errno);
    }
}

// Write to an eventfd to trigger notification
void WriteEvent(int eventfd) {
    const uint64_t value = 1;
    ssize_t res = write(eventfd, &value, sizeof(value));
    if (res != sizeof(value)) {
        LOG_ERROR() << "Failed to write to eventfd: " << strerror(errno);
    }
}

} // namespace

EpollEventDispatcher::EpollEventDispatcher(size_t thread_count)
    : thread_count_(thread_count),
      thread_epoll_fds_(thread_count, -1),
      thread_notify_fds_(thread_count, -1),
      thread_spinning_(thread_count),
      thread_sleep_start_time_(thread_count) {
    
    for (size_t i = 0; i < thread_count_; ++i) {
        // Initialize thread state
        thread_spinning_[i].store(false, std::memory_order_relaxed);
        thread_sleep_start_time_[i].store(0, std::memory_order_relaxed);
        
        // Create epoll instance
        int epoll_fd = CreateEpollInstance();
        if (epoll_fd == -1) {
            Shutdown();
            throw std::system_error(errno, std::system_category(), 
                                   "Failed to create epoll instance");
        }
        thread_epoll_fds_[i] = epoll_fd;
        
        // Create notification channel
        int notify_fd = CreateNotificationChannel();
        if (notify_fd == -1) {
            Shutdown();
            throw std::system_error(errno, std::system_category(), 
                                   "Failed to create notification channel");
        }
        thread_notify_fds_[i] = notify_fd;
        
        // Add notification fd to epoll
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET;
        ev.data.u64 = kEpollInData | static_cast<uint64_t>(i);
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, notify_fd, &ev) == -1) {
            Shutdown();
            throw std::system_error(errno, std::system_category(), 
                                   "Failed to add notification fd to epoll");
        }
    }
}

EpollEventDispatcher::~EpollEventDispatcher() {
    Shutdown();
    
    // Close all epoll and notification fds
    for (size_t i = 0; i < thread_count_; ++i) {
        if (thread_epoll_fds_[i] != -1) {
            close(thread_epoll_fds_[i]);
            thread_epoll_fds_[i] = -1;
        }
        
        if (thread_notify_fds_[i] != -1) {
            close(thread_notify_fds_[i]);
            thread_notify_fds_[i] = -1;
        }
    }
}

void EpollEventDispatcher::ProcessEvents(
    std::size_t thread_index, 
    TaskQueue& queue, 
    std::shared_ptr<impl::TaskProcessorPools> pools) {
    
    if (thread_index >= thread_count_) {
        LOG_ERROR() << "Invalid thread index: " << thread_index;
        return;
    }

    (void)pools;
    
    // Get thread's epoll fd
    int epoll_fd = thread_epoll_fds_[thread_index];
    if (epoll_fd == -1) {
        LOG_ERROR() << "Invalid epoll fd for thread " << thread_index;
        return;
    }

    thread_local int cleanup_counter = 0;
    constexpr int kCleanupInterval = 500;

    // Event buffer for epoll_wait
    epoll_event events[kMaxEvents];
    
    while (!IsShuttingDown()) {
        if (++cleanup_counter >= kCleanupInterval) {
            cleanup_counter = 0;
            CleanupDeadOwners();
        }
        // Check for tasks first
        auto task_opt = queue.PopNonBlocking();
        if (task_opt) {
            auto task_ptr = std::move(*task_opt);
            ExecuteTask(task_ptr.get(), thread_index);
            continue;
        }
        
        // No immediate task, try spinning phase
        bool has_events = PerformSpinning(thread_index, events, epoll_fd);
        
        // If events were detected during spinning, process them
        if (has_events) {
            ProcessEpollEvents(thread_index, events, spin_nevents_);
            
            // Check for tasks that might have been triggered by events
            task_opt = queue.PopNonBlocking();
            if (task_opt) {
                auto task_ptr = std::move(*task_opt);
                ExecuteTask(task_ptr.get(), thread_index);
                continue;
            }
        }
        
        // No tasks or events, enter blocking wait
        WaitForEvents(thread_index, events, epoll_fd);
    }
}

void EpollEventDispatcher::CleanupDeadOwners() {
    std::lock_guard<std::mutex> lock(registry_mutex_);
    
    for (auto it = fd_to_owner_.begin(); it != fd_to_owner_.end();) {
        if (it->second.expired()) {
            int fd = it->first;
            it = fd_to_owner_.erase(it);
            
            // Try to remove callback
            std::lock_guard<std::mutex> lock_cb(fd_mutex_);
            auto cb_it = fd_callbacks_.find(fd);
            if (cb_it != fd_callbacks_.end()) {
                size_t thread_idx = cb_it->second.owner_thread;
                if (thread_idx < thread_count_) {
                    int epoll_fd = thread_epoll_fds_[thread_idx];
                    if (epoll_fd != -1) {
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                    }
                }
                fd_callbacks_.erase(cb_it);
            }
        } else {
            ++it;
        }
    }
}

void EpollEventDispatcher::ExecuteTask(
    impl::TaskContext* task, 
    std::size_t thread_index) {
    
    try {
        // Mark thread as busy while executing task
        thread_spinning_[thread_index].store(false, std::memory_order_relaxed);
        thread_sleep_start_time_[thread_index].store(0, std::memory_order_relaxed);
        
        // Execute the task
        impl::TaskCounter::RunningToken token{task->GetTaskCounter()};
        task->DoStep();
        
        // Clean up if task is finished
        if (task->IsFinished()) {
            task->FinishDetached();
        }
    } catch (const std::exception& ex) {
        LOG_ERROR() << "Exception in task execution: " << ex.what();
        task->FinishDetached();
    }
}

bool EpollEventDispatcher::PerformSpinning(
    std::size_t thread_index, 
    epoll_event* events,
    int epoll_fd) {
    
    // Start spinning phase
    const auto spin_start = std::chrono::steady_clock::now();
    thread_spinning_[thread_index].store(true, std::memory_order_relaxed);
    
    // Spin - check for events during spinning
    bool has_events = false;
    spin_nevents_ = 0;
    auto now = spin_start;
    
    while (now - spin_start < kMinSpinningTime) {
        // Check for events without blocking
        spin_nevents_ = epoll_wait(epoll_fd, events, kMaxEvents, 0);
        
        if (IsShuttingDown()) {
            break;
        }

        if (spin_nevents_ > 0) {
            has_events = true;
            break;
        } else if (spin_nevents_ < 0 && errno != EINTR) {
            LOG_ERROR() << "epoll_wait failed during spin: " << strerror(errno);
        }
        
        // Small pause to reduce CPU usage during spinning
        _mm_pause();
        now = std::chrono::steady_clock::now();
    }

    thread_spinning_[thread_index].store(false, std::memory_order_relaxed);
    return has_events;
}

void EpollEventDispatcher::WaitForEvents(
    std::size_t thread_index,
    epoll_event* events,
    int epoll_fd) {
    
    // Mark thread as sleeping before wait
    thread_sleep_start_time_[thread_index].store(NowNs(), std::memory_order_relaxed);
    
    // Wait for events with infinite timeout
    int nevents = epoll_wait(epoll_fd, events, kMaxEvents, -1);
    
    // Reset sleep time regardless of whether we got events
    thread_sleep_start_time_[thread_index].store(0, std::memory_order_relaxed);
    
    if (nevents < 0) {
        HandleEpollError();
        return;
    }
    
    // Process received events
    ProcessEpollEvents(thread_index, events, nevents);
}

void EpollEventDispatcher::ProcessEpollEvents(
    std::size_t thread_index,
    epoll_event* events,
    int nevents) {
    
    for (int i = 0; i < nevents; ++i) {
        const auto& event = events[i];
        uint64_t event_data = event.data.u64;
        
        if (event_data & kEpollInData) {
            // This is a notification event, consume it
            ConsumeEvent(thread_notify_fds_[thread_index]);
            continue;
        }
        
        // Regular fd event
        ProcessFdEvent(static_cast<int>(event_data & kEventFdMask), event.events);
    }
}

void EpollEventDispatcher::ProcessFdEvent(int fd, uint32_t events) {
    // Look up callback
    std::function<void(uint32_t)> callback;
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        auto it = fd_callbacks_.find(fd);
        if (it == fd_callbacks_.end()) {
            // FD was unregistered, skip it
            return;
        }
        callback = it->second.callback;
    }
    
    // Call the callback with the event mask
    if (callback) {
        try {
            callback(events);
        } catch (const std::exception& ex) {
            LOG_ERROR() << "Exception in fd callback: " << ex.what();
        }
    }
}

void EpollEventDispatcher::HandleEpollError() {
    if (errno == EINTR) {
        // Interrupted, just continue
        return;
    }
    LOG_ERROR() << "epoll_wait failed: " << strerror(errno);
    engine::SleepFor(std::chrono::milliseconds(10));
}

std::size_t EpollEventDispatcher::RegisterFd(
    int fd, 
    uint32_t events, 
    std::function<void(uint32_t)> callback,
    std::weak_ptr<void> owner) {
    
    if (fd < 0 || !callback) {
        LOG_ERROR() << "Invalid fd or callback in RegisterFd";
        return std::numeric_limits<std::size_t>::max();
    }
    
    // First, select a thread to own this fd
    auto thread_index_opt = SelectThreadToWakeup();
    if (!thread_index_opt) {
        LOG_ERROR() << "No available thread to handle fd " << fd;
        return std::numeric_limits<std::size_t>::max();
    }
    
    size_t thread_index = *thread_index_opt;
    int epoll_fd = thread_epoll_fds_[thread_index];
    
    // Store owner if provided
    if (owner.lock()) {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        fd_to_owner_[fd] = std::move(owner);
    }
    
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        
        // Check if fd is already registered
        auto it = fd_callbacks_.find(fd);
        if (it != fd_callbacks_.end()) {
            LOG_WARNING() << "File descriptor " << fd << " already registered, updating";
            
            // Remove from old thread's epoll
            size_t old_thread = it->second.owner_thread;
            if (old_thread != thread_index && old_thread < thread_count_) {
                RemoveFromEpoll(thread_epoll_fds_[old_thread], fd);
            }
            
            // Update callback info
            it->second.callback = std::move(callback);
            it->second.requested_events = events;
            it->second.owner_thread = thread_index;
        } else {
            // Register new fd
            fd_callbacks_[fd] = {
                std::move(callback),
                events,
                thread_index
            };
        }
    }
    
    // Add to epoll
    uint32_t epoll_events = events | EPOLLET;
    epoll_event ev{};
    ev.events = epoll_events;
    ev.data.u64 = static_cast<uint64_t>(fd);
    
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        if (errno == EEXIST) {
            // FD already in epoll, modify instead
            if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev) == -1) {
                LOG_ERROR() << "Failed to modify fd " << fd << " in epoll: " 
                           << strerror(errno);
                
                // Cleanup
                std::lock_guard<std::mutex> lock(fd_mutex_);
                fd_callbacks_.erase(fd);
                
                return std::numeric_limits<std::size_t>::max();
            }
        } else {
            LOG_ERROR() << "Failed to add fd " << fd << " to epoll: " 
                       << strerror(errno);
            
            // Cleanup
            std::lock_guard<std::mutex> lock(fd_mutex_);
            fd_callbacks_.erase(fd);
            
            return std::numeric_limits<std::size_t>::max();
        }
    }
    
    return static_cast<std::size_t>(fd);
}

void EpollEventDispatcher::UnregisterFd(int fd) {
    if (fd < 0) return;
    
    size_t thread_index = 0;
    bool found = false;
    
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        auto it = fd_callbacks_.find(fd);
        if (it == fd_callbacks_.end()) {
            // Not found, nothing to do
            return;
        }
        
        thread_index = it->second.owner_thread;
        found = true;
        
        // Remove from callbacks map
        fd_callbacks_.erase(it);
    }
    
    if (found && thread_index < thread_count_) {
        // Remove from epoll
        int epoll_fd = thread_epoll_fds_[thread_index];
        if (epoll_fd != -1) {
            if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
                if (errno != ENOENT && errno != EBADF) {
                    LOG_ERROR() << "Failed to remove fd " << fd << " from epoll: " 
                               << strerror(errno);
                }
            }
        }
    }
    
    // Remove from owner registry
    {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        fd_to_owner_.erase(fd);
    }
}

void EpollEventDispatcher::PostEvent() {
    auto thread_index_opt = SelectThreadToWakeup();
    if (!thread_index_opt) {
        LOG_WARNING() << "No thread available to post event";
        return;
    }
    
    PostEvent(*thread_index_opt);
}

void EpollEventDispatcher::PostEvent(std::size_t thread_index) {
    if (thread_index >= thread_count_) {
        LOG_ERROR() << "Invalid thread index: " << thread_index;
        return;
    }
    
    int fd = thread_notify_fds_[thread_index];
    if (fd != -1) {
        WriteEvent(fd);
    }
}

void EpollEventDispatcher::Shutdown() {
    is_shutting_down_.store(true, std::memory_order_release);
    
    // Wake up all threads
    for (size_t i = 0; i < thread_count_; ++i) {
        if (thread_notify_fds_[i] != -1) {
            WriteEvent(thread_notify_fds_[i]);
        }
    }
}

EpollEventDispatcher::ThreadState EpollEventDispatcher::GetThreadState(
    std::size_t thread_index) const {
    
    if (thread_index >= thread_count_) {
        return ThreadState::kBusy;  // Invalid thread
    }
    
    if (thread_spinning_[thread_index].load(std::memory_order_relaxed)) {
        return ThreadState::kSpinning;
    }
    
    uint64_t sleep_time = thread_sleep_start_time_[thread_index].load(std::memory_order_relaxed);
    if (sleep_time > 0) {
        return ThreadState::kSleeping;
    }
    
    return ThreadState::kBusy;
}

std::optional<std::size_t> EpollEventDispatcher::SelectThreadToWakeup() {
    if (thread_count_ == 0) {
        return std::nullopt;
    }
    // First, look for spinning threads
    for (size_t i = 0; i < thread_count_; ++i) {
        if (thread_spinning_[i].load(std::memory_order_relaxed)) {
            return i;
        }
    }
    
    // Then, look for sleeping threads
    size_t longest_sleeping_thread = 0;
    uint64_t longest_sleep_time = 0;
    bool found_sleeping = false;
    
    const uint64_t now = NowNs();
    
    for (size_t i = 0; i < thread_count_; ++i) {
        uint64_t sleep_start = thread_sleep_start_time_[i].load(std::memory_order_relaxed);
        if (sleep_start > 0) {
            uint64_t sleep_duration = now - sleep_start;
            if (!found_sleeping || sleep_duration > longest_sleep_time) {
                longest_sleep_time = sleep_duration;
                longest_sleeping_thread = i;
                found_sleeping = true;
            }
        }
    }
    
    if (found_sleeping) {
        return longest_sleeping_thread;
    }
    
    return std::nullopt;  // No sleeping threads found
}

int EpollEventDispatcher::CreateEpollInstance() const {
    int fd = epoll_create1(EPOLL_CLOEXEC);
    if (fd == -1) {
        LOG_ERROR() << "Failed to create epoll instance: " << strerror(errno);
    }
    return fd;
}

int EpollEventDispatcher::CreateNotificationChannel() const {
    int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd == -1) {
        LOG_ERROR() << "Failed to create eventfd: " << strerror(errno);
    }
    return fd;
}

bool EpollEventDispatcher::AddToEpoll(
    int epoll_fd, int fd, uint32_t events, uint64_t data) const {
    
    if (epoll_fd == -1 || fd == -1) {
        return false;
    }
    
    epoll_event ev{};
    ev.events = events;
    ev.data.u64 = data;
    
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        LOG_ERROR() << "Failed to add fd " << fd << " to epoll: " 
                    << strerror(errno);
        return false;
    }
    
    return true;
}

bool EpollEventDispatcher::RemoveFromEpoll(int epoll_fd, int fd) const {
    if (epoll_fd == -1 || fd == -1) {
        return false;
    }
    
    if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
        LOG_ERROR() << "Failed to remove fd " << fd << " from epoll: " 
                    << strerror(errno);
        return false;
    }
    
    return true;
}

}  // namespace engine

USERVER_NAMESPACE_END

#endif  // __linux__