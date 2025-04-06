#ifdef __linux__

#include "epoll_event_dispatcher.hpp"

#include <cerrno>
#include <chrono>
#include <system_error>

#include <userver/engine/sleep.hpp>
#include <userver/utils/assert.hpp>

#include <engine/task/task_context.hpp>  
#include <engine/task/task_counter.hpp>
#include <engine/task/task_processor_pools.hpp>

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
    while (true) {
        uint64_t value;
        ssize_t res = read(eventfd, &value, sizeof(value));
        if (res < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No more data to read
                break;
            }
            if (errno == EINTR) {
                continue;
            }
            LOG_ERROR() << "Failed to read from eventfd: " << strerror(errno);
            break;
        }
        
        if (res != sizeof(value)) {
            LOG_ERROR() << "Partial read from eventfd: expected " << sizeof(value) 
                        << " bytes, got " << res;
            break;
        }
        // Continue reading until no more data is available
    }
}

// Write to an eventfd to trigger notification
void WriteEvent(int eventfd) {
    const uint64_t value = 1;
    while (true) {
        ssize_t res = write(eventfd, &value, sizeof(value));
        if (res == sizeof(value)) {
            // Successfully wrote the value
            break;
        }
        
        if (res < 0) {
            if (errno == EINTR) {
                continue;
            }
            
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            
            LOG_ERROR() << "Failed to write to eventfd: " << strerror(errno);
            break;
        }
    }
}

} // namespace

EpollEventDispatcher::EpollEventDispatcher(size_t thread_count)
    : thread_count_(thread_count),
      thread_epoll_fds_(thread_count, -1),
      thread_notify_fds_(thread_count, -1),
      worker_thread_ids_(thread_count, std::thread::id()) {

    thread_spinning_ = std::make_unique<std::atomic<bool>[]>(thread_count_);
    thread_sleep_start_time_ = std::make_unique<std::atomic<uint64_t>[]>(thread_count_);
    
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

    if (worker_thread_ids_.size() <= thread_index) {
        if (worker_thread_ids_.size() < thread_count_) {
            worker_thread_ids_.resize(thread_count_, std::thread::id());
        }
        worker_thread_ids_[thread_index] = std::this_thread::get_id();
    }
    
    // Get thread's epoll fd
    int epoll_fd = thread_epoll_fds_[thread_index];
    if (epoll_fd == -1) {
        LOG_ERROR() << "Invalid epoll fd for thread " << thread_index;
        return;
    }

    thread_local int cleanup_counter = 0;
    constexpr int kCleanupInterval = 500;

    thread_local int stack_monitor_counter = 0;
    constexpr int kStackMonitorInterval = 100;

    epoll_event events[kMaxEvents];
    
    while (!IsShuttingDown()) {
        if (++cleanup_counter >= kCleanupInterval) {
            cleanup_counter = 0;
            CleanupDeadOwners();
        }
        if (++stack_monitor_counter >= kStackMonitorInterval) {
            stack_monitor_counter = 0;
            if (pools) {
                pools->GetCoroPool().AccountStackUsage();
            }
        }
        // Check for tasks first
        bool processed_task = false;
        while (auto task_opt = queue.PopNonBlocking()) {
            processed_task = true;
            auto task_ptr = std::move(*task_opt);
            if (!task_ptr) continue;
            
            ExecuteTask(task_ptr.get(), thread_index);
        }

        if (IsShuttingDown()) break;
        if (processed_task) continue;
        
        // Check for epoll events without blocking
        int nevents = epoll_wait(epoll_fd, events, kMaxEvents, 0);
        if (nevents > 0) {
            ProcessEpollEvents(thread_index, events, nevents);
            continue;
        } else if (nevents < 0 && errno != EINTR) {
            HandleEpollError();
        }
        
        // If no events were detected, try spinning
        bool spin_events = PerformSpinning(thread_index, events, epoll_fd);
        if (spin_events) {
            ProcessEpollEvents(thread_index, events, spin_nevents_);
            continue;
        }

        // Last check before blocking wait
        if (CheckEventFdReady(thread_notify_fds_[thread_index])) {
            ConsumeEvent(thread_notify_fds_[thread_index]);
            continue;
        }
        if (auto task_opt = queue.PopNonBlocking()) {
            auto task_ptr = std::move(*task_opt);
            if (task_ptr) {
                ExecuteTask(task_ptr.get(), thread_index);
                continue;
            }
        }
        if (IsShuttingDown()) break;
        
        // Block for events
        WaitForEvents(thread_index, events, epoll_fd);
    }
}

bool EpollEventDispatcher::CheckEventFdReady(int eventfd) {
    if (eventfd < 0) return false;
    
    fd_set read_set;
    FD_ZERO(&read_set);
    FD_SET(eventfd, &read_set);
    
    struct timeval timeout{0, 0};
    int result = select(eventfd + 1, &read_set, nullptr, nullptr, &timeout);
    
    return (result > 0 && FD_ISSET(eventfd, &read_set));
}

void EpollEventDispatcher::CleanupDeadOwners() {
    std::vector<int> fds_to_cleanup;
    
    {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        for (auto it = fd_to_owner_.begin(); it != fd_to_owner_.end();) {
            if (it->second.expired()) {
                fds_to_cleanup.push_back(it->first);
                it = fd_to_owner_.erase(it);
            } else {
                ++it;
            }
        }
    }
    
    for (int fd : fds_to_cleanup) {
        UnregisterFd(fd);
    }
}

void EpollEventDispatcher::ExecuteTask(
    impl::TaskContext* task, 
    std::size_t thread_index) {
    if (!task) return;

    (void)thread_index;

    bool success = false;
    try {
        auto& task_counter = task->GetTaskCounter();
        impl::TaskCounter::RunningToken run_token{task_counter};
        task->DoStep();
        success = true;
    } catch (const std::exception& ex) {
        LOG_ERROR() << "Exception in task execution: " << ex.what();
    } catch (...) {
        LOG_ERROR() << "Unknown exception in task execution";
    }
    
    try {
        if (!success || task->IsFinished()) {
            task->FinishDetached();
        }
    } catch (const std::exception& ex) {
        LOG_ERROR() << "Exception in task cleanup: " << ex.what();
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
    bool has_spin_events = false;
    spin_nevents_ = 0;
    auto now = spin_start;
    
    while (now - spin_start < kMinSpinningTime) {
        // Check for events without blocking
        spin_nevents_ = epoll_wait(epoll_fd, events, kMaxEvents, 0);
        
        if (IsShuttingDown()) {
            break;
        }

        if (spin_nevents_ > 0) {
            has_spin_events = true;
            break;
        } else if (spin_nevents_ < 0 && errno != EINTR) {
            LOG_ERROR() << "epoll_wait failed during spin: " << strerror(errno);
        }
        
        // Small pause to reduce CPU usage during spinning
        _mm_pause();
        now = std::chrono::steady_clock::now();
    }

    thread_spinning_[thread_index].store(false, std::memory_order_relaxed);
    return has_spin_events;
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
        int fd = static_cast<int>(event_data & kEventFdMask);
        
        bool is_pipe = false;
        struct stat st;
        if (fstat(fd, &st) == 0) {
            is_pipe = S_ISFIFO(st.st_mode);
        }
        
        if (is_pipe) {
            fd_set read_fds, write_fds;
            FD_ZERO(&read_fds);
            FD_ZERO(&write_fds);
            FD_SET(fd, &read_fds);
            FD_SET(fd, &write_fds);
            
            struct timeval tv_zero{0, 0};
            select(fd + 1, &read_fds, &write_fds, nullptr, &tv_zero);
            
            uint32_t real_events = 0;
            if (FD_ISSET(fd, &read_fds)) real_events |= EPOLLIN;
            if (FD_ISSET(fd, &write_fds)) real_events |= EPOLLOUT;
            
            if (real_events) {
                ProcessFdEvent(fd, real_events | (event.events & (EPOLLERR | EPOLLHUP)), 
                               thread_index);
                continue;
            }
        }
        ProcessFdEvent(static_cast<int>(event_data & kEventFdMask), event.events, thread_index);
    }
}

void EpollEventDispatcher::ProcessFdEvent(int fd, uint32_t events, size_t current_thread) {
    std::function<void(uint32_t)> callback;
    size_t owner_thread = std::numeric_limits<size_t>::max();
    bool forward_to_other_thread = false;
    {
        std::lock_guard<std::mutex> lock(fd_mutex_);
        auto it = fd_callbacks_.find(fd);
        if (it == fd_callbacks_.end()) {
            return;
        }
        
        // Check if the fd is owned by another thread
        if (it->second.owner_thread != current_thread && it->second.owner_thread < thread_count_) {
            owner_thread = it->second.owner_thread;
            forward_to_other_thread = true;
        } else {
            callback = it->second.callback;
        }
    }

    if (forward_to_other_thread) {
        PostEvent(owner_thread);
        return;
    }
    
    if (!callback) {
        return;
    }
    // Check if the owner is still valid
    bool expired = false;
    {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        auto it = fd_to_owner_.find(fd);
        if (it != fd_to_owner_.end() && it->second.expired()) {
            expired = true;
        }
    }
    
    if (expired) {
        UnregisterFd(fd);
        return;
    }
    
    try {
        callback(events);
    } catch (const std::exception& ex) {
        LOG_ERROR() << "Exception in fd callback: " << ex.what();
    } catch (...) {
        LOG_ERROR() << "Unknown exception in fd callback";
    }
}

void EpollEventDispatcher::HandleEpollError() {
    if (errno == EINTR) {
        // Interrupted, just continue
        return;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Not an error - just no events available
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
    
    if (fd < 0) {
        LOG_WARNING() << "Attempt to register invalid fd " << fd;
        return std::numeric_limits<std::size_t>::max();
    }
    
    if (!callback) {
        LOG_WARNING() << "Attempt to register fd " << fd << " with null callback";
        return std::numeric_limits<std::size_t>::max();
    }
    
    // First, select a thread to own this fd
    auto thread_index_opt = SelectThreadToWakeup();
    if (!thread_index_opt) {
        LOG_ERROR() << "No available thread to handle fd " << fd;
        return std::numeric_limits<std::size_t>::max();
    }
    
    size_t thread_index = *thread_index_opt;
    size_t result_index = std::numeric_limits<std::size_t>::max();
    
    {
        std::unique_lock<std::mutex> fd_lock(fd_mutex_);
        
        // Check if fd is already registered
        auto it = fd_callbacks_.find(fd);
        bool has_existing = (it != fd_callbacks_.end());

        if (has_existing && it->second.owner_thread != thread_index) {
            thread_index = it->second.owner_thread;
        }

        epoll_event ev{};
        ev.events = events | EPOLLET;
        ev.data.u64 = static_cast<uint64_t>(fd);

        if (has_existing) {
            // Update existing callback
            it->second.callback = std::move(callback);
            it->second.requested_events = events;
        } else {
            // Register new fd
            fd_callbacks_.emplace(fd, FdCallbackInfo{std::move(callback), events, thread_index});
        }

        // Add\update in epoll
        int epoll_fd = thread_epoll_fds_[thread_index];
        int op = has_existing ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
        int result = epoll_ctl(epoll_fd, op, fd, &ev);
        
        if (result == -1) {
            if (errno == EEXIST && op == EPOLL_CTL_ADD) {
                // FD already exists, modify instead
                result = epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
            }
            
            if (result == -1) {
                LOG_WARNING() << "Failed to " << (op == EPOLL_CTL_ADD ? "add" : "modify") 
                            << " fd " << fd << " to thread epoll: " << strerror(errno);
                
                // Cleanup
                if (!has_existing) {
                    fd_callbacks_.erase(fd);
                }
                
                return std::numeric_limits<std::size_t>::max();
            }
        }
        
        result_index = thread_index;
        fd_lock.unlock();
        
        // Register owner if provided
        if (owner.lock()) {
            std::lock_guard<std::mutex> registry_lock(registry_mutex_);
            fd_to_owner_[fd] = std::move(owner);
        }
    }
    // Notify the thread to process events
    PostEvent(thread_index);
    return result_index;
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
    
    static std::atomic<std::size_t> next_thread{0};
    return next_thread.fetch_add(1, std::memory_order_relaxed) % thread_count_;
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