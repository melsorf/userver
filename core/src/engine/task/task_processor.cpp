#include "task_processor.hpp"

#ifdef __linux__
#include <sys/epoll.h>
#include <sys/eventfd.h>
#endif  // __linux__

#include <sys/types.h>
#include <csignal>

#include <fmt/format.h>

#include <concurrent/impl/latch.hpp>
#include <userver/compiler/thread_local.hpp>
#include <userver/logging/log.hpp>
#include <userver/utils/assert.hpp>
#include <userver/utils/impl/static_registration.hpp>
#include <userver/utils/numeric_cast.hpp>
#include <userver/utils/rand.hpp>
#include <userver/utils/thread_name.hpp>
#include <userver/utils/threads.hpp>
#include <userver/utils/traceful_exception.hpp>
#include <utils/statistics/thread_statistics.hpp>

#include <engine/task/counted_coroutine_ptr.hpp>
#include <engine/task/task_context.hpp>
#include <engine/task/task_counter.hpp>
#include <engine/task/task_processor_pools.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {
namespace {

template <class Value>
struct OverloadActionAndValue final {
    TaskProcessorSettings::OverloadAction action;
    Value value;
};

template <class OverloadBitAndValue>
constexpr OverloadActionAndValue<OverloadBitAndValue> GetOverloadActionAndValue(
    const std::atomic<OverloadBitAndValue>& x
) {
    const auto value = x.load();
    if (value < OverloadBitAndValue{0}) {
        return {TaskProcessorSettings::OverloadAction::kIgnore, -value};
    } else {
        return {TaskProcessorSettings::OverloadAction::kCancel, value};
    }
}

void SetTaskQueueWaitTimepoint(impl::TaskContext* context) {
    static constexpr std::size_t kTaskTimestampInterval = 4;
    thread_local std::size_t task_count = 0;
    if (task_count++ == kTaskTimestampInterval) {
        task_count = 0;
        context->SetQueueWaitTimepoint(std::chrono::steady_clock::now());
    } else {
        /* Don't call clock_gettime() too often.
         * This leads to killing some innocent tasks on overload, up to
         * +(kTaskTimestampInterval-1), we may sacrifice them.
         */
        context->SetQueueWaitTimepoint(std::chrono::steady_clock::time_point());
    }
}

// Hooks are modified only before task processors created and only in main
// thread, so it doesn't need any synchronization.
std::vector<std::function<void()>>& ThreadStartedHooks() {
    static std::vector<std::function<void()>> thread_started_hooks;
    return thread_started_hooks;
}

void EmitMagicNanosleep() {
    // If we're ptrace'd (e.g. by strace), the magic syscall tells a tracer
    // that all startup stuff of the current thread is done.
    // Before this timepoint we could do blocking syscalls.
    // From now on, every blocking syscall is a bug.
    const struct timespec ts = {0, 42};
    nanosleep(&ts, nullptr);
}

void TaskProcessorThreadStartedHook() {
    utils::impl::AssertStaticRegistrationFinished();
    utils::WithDefaultRandom([](auto&) {});
    for (const auto& func : ThreadStartedHooks()) {
        func();
    }
    EmitMagicNanosleep();
}

auto MakeTaskQueue(TaskProcessorConfig config) {
    using ResultType = std::variant<TaskQueue, WorkStealingTaskQueue>;
    switch (config.task_processor_queue) {
        case TaskQueueType::kGlobalTaskQueue:
            return ResultType{std::in_place_index<0>, std::move(config)};
        case TaskQueueType::kWorkStealingTaskQueue:
            return ResultType{std::in_place_index<1>, std::move(config)};
    }
    UINVARIANT(false, "Unexpected value of TaskQueueType enum");
}

bool PlatformSupportsEpollet() {
#ifdef __linux__
    return true;
#else   // __linux__
    return false;
#endif  // __linux__
}

#ifdef __linux__
int CreateEpollFd() {
    int fd = epoll_create1(0);
    if (fd == -1) {
        throw utils::TracefulException("Failed to create epoll instance");
    }
    return fd;
}

int CreateEventFd() {
    int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd == -1) {
        throw utils::TracefulException("Failed to create eventfd");
    }
    return fd;
}
#endif  // __linux__

}  // namespace


namespace current_task {
    TaskProcessor* GetTaskProcessorUnchecked() noexcept {
        auto* context = GetCurrentTaskContextUnchecked();
        if (!context) return nullptr;
        return &context->GetTaskProcessor();
    }
}

TaskProcessor::TaskProcessor(TaskProcessorConfig config, std::shared_ptr<impl::TaskProcessorPools> pools)
    : task_queue_(MakeTaskQueue(config)),
    task_counter_(config.worker_threads),
    config_(std::move(config)),
    pools_(std::move(pools))
{
    utils::impl::FinishStaticRegistration();

    try {
        LOG_INFO() << "creating task_processor " << Name() << " "
                   << "worker_threads=" << config_.worker_threads << " thread_name=" << config_.thread_name;

#ifdef __linux__
        use_ev_thread_pool_ = !PlatformSupportsEpollet();
        if (!UseEvThreadPool()) {
            per_thread_epoll_fds_.resize(config_.worker_threads);
            per_thread_event_fds_.resize(config_.worker_threads, -1);
            for (auto& epoll_fd : per_thread_epoll_fds_) {
                epoll_fd = CreateEpollFd();
            }
            for (auto& event_fd : per_thread_event_fds_) {
                event_fd = CreateEventFd();
            }
            thread_spinning_ = std::make_unique<std::atomic<bool>[]>(config_.worker_threads);
            for (size_t i = 0; i < config_.worker_threads; ++i) {
                thread_spinning_[i].store(false, std::memory_order_relaxed);
            }
            thread_sleep_start_time_ = std::make_unique<std::atomic<uint64_t>[]>(config_.worker_threads);
            for (size_t i = 0; i < config_.worker_threads; ++i) {
                thread_sleep_start_time_[i].store(0, std::memory_order_relaxed);
            }
        }
#endif  // __linux__

        concurrent::impl::Latch workers_left{static_cast<std::ptrdiff_t>(config_.worker_threads)};
        workers_.reserve(config_.worker_threads);
        for (std::size_t i = 0; i < config_.worker_threads; ++i) {
            workers_.emplace_back([this, i, &workers_left] {
                PrepareWorkerThread(i);
                workers_left.count_down();

#ifdef __linux__
                if (UseEvThreadPool()) {
                    ProcessTasks();
                } else {
                    RunEventLoop(i);
                }
#else   // __linux__
                ProcessTasks();
#endif  // __linux__
                FinalizeWorkerThread();
            });
        }

        cpu_stats_storage_ = std::make_unique<utils::statistics::ThreadPoolCpuStatsStorage>(workers_);
        workers_left.wait();
    } catch (...) {
        Cleanup();
        throw;
    }
}

TaskProcessor::~TaskProcessor() { Cleanup(); }

void TaskProcessor::Cleanup() noexcept {
    InitiateShutdown();

    // Some tasks may be bound but not scheduled yet
    task_counter_.WaitForExhaustionBlocking();

    std::visit([](auto&& arg) { return arg.StopProcessing(); }, task_queue_);

    for (auto& w : workers_) {
        w.join();
    }
#ifdef __linux__
    if (!UseEvThreadPool()) {
        for (auto ep : per_thread_epoll_fds_) {
            if (ep >= 0) ::close(ep);
        }
        for (auto event_fd : per_thread_event_fds_) {
            if (event_fd >= 0) ::close(event_fd);
        }
    }
#endif
    UASSERT(!task_counter_.MayHaveTasksAlive());
}

void TaskProcessor::InitiateShutdown() {
    is_shutting_down_ = true;
#ifdef __linux__
    WakeupEventLoop();
#endif  // __linux__
    detached_contexts_->RequestCancellation(TaskCancellationReason::kShutdown);
}

void TaskProcessor::Schedule(impl::TaskContext* context) {
    UASSERT(context);
    const auto [action, max_queue_length] = GetOverloadActionAndValue(action_bit_and_max_task_queue_wait_length_);
    if (max_queue_length && !context->IsCritical()) {
        UASSERT(max_queue_length > 0);
        if (const auto overload_size = GetOverloadByLength(max_queue_length)) {
            LOG_LIMITED_WARNING() << "failed to enqueue task: task_queue_size_approximate=" << overload_size << " >= "
                                  << "length_limit=" << max_queue_length << " task_processor=" << Name()
                                  << ". Make sure that there's enough system resources to process so "
                                     "many tasks, adjust the "
                                     "`default-service.default-task-processor.wait_queue_overload."
                                     "length_limit` parameter in USERVER_TASK_PROCESSOR_QOS dynamic "
                                     "config to increase the limit.";
            HandleOverload(*context, action);
        }
    }
    if (is_shutting_down_) {
        context->RequestCancel(TaskCancellationReason::kShutdown);
#ifdef __linux__
        if (!UseEvThreadPool()) WakeupEventLoop();
#endif  // __linux__
    }
    SetTaskQueueWaitTimepoint(context);

    std::visit([&context](auto&& arg) { return arg.Push(context); }, task_queue_);

#ifdef __linux__
    WakeupEventLoop();
#endif  // __linux__
}

void TaskProcessor::Adopt(impl::TaskContext& context) { detached_contexts_->Add(context); }

ev::ThreadPool& TaskProcessor::EventThreadPool() { return pools_->EventThreadPool(); }

impl::CountedCoroutinePtr TaskProcessor::GetCoroutine() { return {pools_->GetCoroPool().GetCoroutine(), *this}; }

std::size_t TaskProcessor::GetTaskQueueSize() const {
    return std::visit([](auto&& arg) { return arg.GetSizeApproximate(); }, task_queue_);
}

void TaskProcessor::SetSettings(const TaskProcessorSettings& settings) {
    sensor_task_queue_wait_time_ = settings.sensor_wait_queue_time_limit;

    // We store the overload action and limit in a single atomic, to avoid races
    // on {kIgnore, 10} transitions to {kCancel, 10000}, when the limit is taken
    // from and old value and action from a new one. As a result, with a race
    // we may cancel a task that fits into 10000 limit.
    //
    // see GetOverloadActionAndValue()
    UASSERT(settings.wait_queue_time_limit >= std::chrono::microseconds{0});
    static_assert(
        std::is_unsigned_v<decltype(settings.wait_queue_length_limit)>,
        "Could hold negative values, add a runtime check that the "
        "value is positive"
    );
    switch (settings.overload_action) {
        case TaskProcessorSettings::OverloadAction::kCancel:
            action_bit_and_max_task_queue_wait_time_ = settings.wait_queue_time_limit;
            action_bit_and_max_task_queue_wait_length_ =
                utils::numeric_cast<std::int64_t>(settings.wait_queue_length_limit);
            break;
        case TaskProcessorSettings::OverloadAction::kIgnore:
            action_bit_and_max_task_queue_wait_time_ = -settings.wait_queue_time_limit;
            action_bit_and_max_task_queue_wait_length_ =
                -utils::numeric_cast<std::int64_t>(settings.wait_queue_length_limit);
            break;
    }

    auto threshold = settings.profiler_execution_slice_threshold;
    if (threshold.count() > 0) {
        auto old_threshold = task_profiler_threshold_.exchange(threshold);
        if (old_threshold.count() == 0) {
            LOG_WARNING() << fmt::format(
                "Task profiling is now enabled for task processor '{}' "
                "(threshold={}us), you may "
                "change settings or disable it in "
                "USERVER_TASK_PROCESSOR_PROFILER_DEBUG config",
                config_.thread_name,
                threshold.count()
            );
        }
    } else {
        auto old_threshold = task_profiler_threshold_.exchange(std::chrono::microseconds(0));
        if (old_threshold.count() > 0) {
            LOG_WARNING() << fmt::format(
                "Task profiling is now disabled for task processor '{}', you may "
                "enable it in USERVER_TASK_PROCESSOR_PROFILER_DEBUG config",
                config_.thread_name
            );
        }
    }
    profiler_force_stacktrace_.store(settings.profiler_force_stacktrace);
}

std::chrono::microseconds TaskProcessor::GetProfilerThreshold() const { return task_profiler_threshold_.load(); }

bool TaskProcessor::ShouldProfilerForceStacktrace() const { return profiler_force_stacktrace_.load(); }

std::size_t TaskProcessor::GetTaskTraceMaxCswForNewTask() const {
    thread_local std::size_t count = 0;
    if (count++ == config_.task_trace_every) {
        count = 0;
        return config_.task_trace_max_csw;
    } else {
        return 0;
    }
}

const std::string& TaskProcessor::GetTaskTraceLoggerName() const { return config_.task_trace_logger_name; }

void TaskProcessor::SetTaskTraceLogger(logging::LoggerPtr logger) {
    task_trace_logger_ = std::move(logger);
    [[maybe_unused]] const auto was_task_trace_logger_set =
        task_trace_logger_set_.exchange(true, std::memory_order_release);
    UASSERT(!was_task_trace_logger_set);
}

logging::LoggerPtr TaskProcessor::GetTaskTraceLogger() const {
    // logger macros should be ready to deal with null logger
    if (!task_trace_logger_set_.load(std::memory_order_acquire)) return {};
    return task_trace_logger_;
}

std::vector<std::uint8_t> TaskProcessor::CollectCurrentLoadPct() const {
    UASSERT(cpu_stats_storage_);

    return cpu_stats_storage_->CollectCurrentLoadPct();
}

void RegisterThreadStartedHook(std::function<void()> func) {
    utils::impl::AssertStaticRegistrationAllowed("Calling engine::RegisterThreadStartedHook()");
    ThreadStartedHooks().push_back(std::move(func));
}

void TaskProcessor::PrepareWorkerThread(std::size_t index) {
    switch (config_.os_scheduling) {
        case OsScheduling::kNormal:
            break;
        case OsScheduling::kLowPriority:
            utils::SetCurrentThreadLowPriorityScheduling();
            break;
        case OsScheduling::kIdle:
            utils::SetCurrentThreadIdleScheduling();
            break;
    }

    std::visit([index](auto& obj) { obj.PrepareWorker(index); }, task_queue_);

    pools_->GetCoroPool().PrepareLocalCache();

    utils::SetCurrentThreadName(fmt::format("{}_{}", config_.thread_name, index));

    impl::SetLocalTaskCounterData(task_counter_, index);

    pools_->GetCoroPool().RegisterThread();

    TaskProcessorThreadStartedHook();

#ifdef __linux__
    // Add event_fd to the epoll
    if (!UseEvThreadPool() && index < per_thread_epoll_fds_.size()) {
        const int epoll_fd = per_thread_epoll_fds_[index];
        if (epoll_fd >= 0 && index < per_thread_event_fds_.size() && per_thread_event_fds_[index] >= 0) {
            struct epoll_event ev;
            ev.events = EPOLLIN;
            ev.data.fd = per_thread_event_fds_[index];
            if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, per_thread_event_fds_[index], &ev) == -1) {
                throw utils::TracefulException("Failed to add event_fd to epoll");
            }

            int flags = fcntl(per_thread_event_fds_[index], F_GETFL, 0);
            if (flags == -1) {
                throw utils::TracefulException("Failed to get event_fd flags");
            }
            if (fcntl(per_thread_event_fds_[index], F_SETFL, flags | O_NONBLOCK) == -1) {
                throw utils::TracefulException("Failed to set event_fd to non-blocking mode");
            }
        }
    }
#endif
}

void TaskProcessor::FinalizeWorkerThread() noexcept { pools_->GetCoroPool().ClearLocalCache(); }

void TaskProcessor::ProcessTasks() noexcept {
    while (true) {
        auto context = std::visit([](auto&& arg) { return arg.PopBlocking(); }, task_queue_);
        if (!context) break;

        CheckWaitTime(*context);

        bool has_failed = false;
        try {
            impl::TaskCounter::RunningToken token{GetTaskCounter()};
            context->DoStep();
        } catch (const std::exception& ex) {
            LOG_ERROR() << "uncaught exception from DoStep: " << ex;
            has_failed = true;
        }

        pools_->GetCoroPool().AccountStackUsage();

        if (has_failed || context->IsFinished()) {
            context->FinishDetached();
        }
    }
}

void TaskProcessor::CheckWaitTime(impl::TaskContext& context) {
    const auto [action, max_wait_time] = GetOverloadActionAndValue(action_bit_and_max_task_queue_wait_time_);
    const auto sensor_wait_time = sensor_task_queue_wait_time_.load();

    if (max_wait_time.count() == 0 && sensor_wait_time.count() == 0) {
        SetTaskQueueWaitTimeOverloaded(false);
        return;
    }

    const auto wait_timepoint = context.GetQueueWaitTimepoint();
    if (wait_timepoint != std::chrono::steady_clock::time_point()) {
        const auto wait_time = std::chrono::steady_clock::now() - wait_timepoint;
        const auto wait_time_us = std::chrono::duration_cast<std::chrono::microseconds>(wait_time);
        LOG_TRACE() << "queue wait time = " << wait_time_us.count() << "us";

        SetTaskQueueWaitTimeOverloaded(max_wait_time.count() && wait_time >= max_wait_time);

        if (sensor_wait_time.count() && wait_time >= sensor_wait_time) {
            GetTaskCounter().AccountTaskOverloadSensor();
        } else {
            GetTaskCounter().AccountTaskNoOverloadSensor();
        }
    } else {
        // no info, let's pretend this task has the same queue wait time as the
        // previous one
    }

    // Don't cancel critical tasks, but use their timestamp to cancel other tasks
    if (overloaded_cache_->overloaded_by_wait_time.load()) {
        HandleOverload(context, action);
    }
}

void TaskProcessor::SetTaskQueueWaitTimeOverloaded(bool new_value) noexcept {
    auto& atomic = overloaded_cache_->overloaded_by_wait_time;
    // The check helps to reduce contention.
    if (atomic.load(std::memory_order_relaxed) != new_value) {
        atomic.store(new_value, std::memory_order_relaxed);
    }
}

void TaskProcessor::HandleOverload(impl::TaskContext& context, TaskProcessorSettings::OverloadAction action) {
    GetTaskCounter().AccountTaskOverload();

    if (action == TaskProcessorSettings::OverloadAction::kCancel) {
        if (!context.IsCritical()) {
            LOG_LIMITED_WARNING() << "Task with task_id=" << logging::HexShort(context.GetTaskId())
                                  << " was waiting in queue for too long, cancelling. Make sure that "
                                     "there's no blocking syscalls in the task, use utils::CpuRelax. "
                                     "Adjust the `default-service.default-task-processor."
                                     "wait_queue_overload.sensor_time_limit_us` parameter in "
                                     "USERVER_TASK_PROCESSOR_QOS dynamic config to increase the limit.";

            context.RequestCancel(TaskCancellationReason::kOverload);
            GetTaskCounter().AccountTaskCancelOverload();
        } else {
            LOG_TRACE() << "Task with task_id=" << logging::HexShort(context.GetTaskId())
                        << " was waiting in queue for too long, but it is marked "
                           "as critical, not cancelling.";
        }
    }
}

TaskProcessor::OverloadByLength TaskProcessor::GetOverloadByLength(const std::size_t max_queue_length) noexcept {
    const auto old_overload_by_length = overloaded_cache_->overload_by_length.load();
    // With this choice of factor, the probability of skipping over 200 tasks
    // before checking is negligible.
    constexpr int kFactor = 16;
    // In Overloaded state, checks are performed every time to stop cancelling
    // tasks as soon as possible.
    //
    // If we applied any kind of factor for Overloaded state, then it's possible
    // that even once the task queue size drops far enough below the limit, we
    // won't accept a new task with a high probability. This is because
    // the current implementation never updates the queue size cache after Pop.
    if (old_overload_by_length == 0 && utils::RandRange(kFactor) != 0) {
        return old_overload_by_length;
    }

    // ComputeOverloadByLength requires computing task queue length, which is
    // too expensive to do on every Push. So we cache the Overloaded state and
    // only recompute it once in a while.
    return ComputeOverloadByLength(old_overload_by_length, max_queue_length);
}

TaskProcessor::OverloadByLength TaskProcessor::ComputeOverloadByLength(
    const OverloadByLength old_overload_by_length,
    const std::size_t max_queue_length
) noexcept {
    static constexpr std::size_t kExitOverloadStatusFactorNumerator = 19;
    static constexpr std::size_t kExitOverloadStatusFactorDenominator = 20;

    const auto queue_size = GetTaskQueueSize();

    // Avoid rapid entering-exiting "overloaded by length" state with associated
    // contention.
    const auto size_limit = old_overload_by_length ? kExitOverloadStatusFactorNumerator * max_queue_length /
                                                         kExitOverloadStatusFactorDenominator
                                                   : max_queue_length;

    const OverloadByLength new_overload_by_length = queue_size >= size_limit ? queue_size : 0;

    if (new_overload_by_length != old_overload_by_length) {
        overloaded_cache_->overload_by_length.store(new_overload_by_length, std::memory_order_relaxed);
    }
    return new_overload_by_length;
}

#ifdef __linux__
std::size_t TaskProcessor::RegisterFd(int fd, uint32_t events, std::function<void(uint32_t)> callback) {
    if (fd < 0) {
        return std::numeric_limits<std::size_t>::max();
    }
    if (UseEvThreadPool()) return 0;
    if (per_thread_epoll_fds_.empty()) {
        LOG_WARNING() << "RegisterFd called but per_thread_epoll_fds_ is empty";
        return std::numeric_limits<std::size_t>::max();
    }
    std::size_t index;
    {
        std::lock_guard<std::mutex> lock(fd_map_mtx_);
        auto existing_it = fd_to_thread_index_.find(fd);
        if (existing_it != fd_to_thread_index_.end()) {
            index = existing_it->second;
        } else {
            index = task_counter_.GetLocalTaskThreadId() % per_thread_epoll_fds_.size();
            fd_to_thread_index_[fd] = index;
        }
    }

    struct epoll_event ev;
    ev.events = events | EPOLLET;
    ev.data.fd = fd;

    {
        std::lock_guard<std::mutex> lock(epoll_mtx_);
        int epoll_fd = per_thread_epoll_fds_[index];
        int op = EPOLL_CTL_ADD;

        bool has_existing = fd_callbacks_.find(fd) != fd_callbacks_.end();
        if (has_existing) {
            op = EPOLL_CTL_MOD;
        }
        int result = epoll_ctl(epoll_fd, op, fd, &ev);
        if (result == -1) {
            if (errno == EEXIST && op == EPOLL_CTL_ADD) {
                result = epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
            }
            
            if (result == -1) {
                LOG_WARNING() << "Failed to " << (op == EPOLL_CTL_ADD ? "add" : "modify") 
                    << " fd " << fd << " to per-thread epoll: " << strerror(errno);
                
                // Remove the fd from the map if it was not added
                if (!has_existing) {
                    std::lock_guard<std::mutex> idx_lock(fd_map_mtx_);
                    fd_to_thread_index_.erase(fd);
                }
                return std::numeric_limits<std::size_t>::max();
            }
        }

        fd_callbacks_[fd] = std::move(callback);
    }
    return index;
}

void TaskProcessor::UnregisterFd(int fd) {
    if (UseEvThreadPool()) return;

    std::size_t index;
    bool found = false;

    {
        std::lock_guard<std::mutex> lock(fd_map_mtx_);
        auto it = fd_to_thread_index_.find(fd);
        if (it != fd_to_thread_index_.end()) {
            index = it->second;
            fd_to_thread_index_.erase(it);
            found = true;
        }
    }

    if (!found) {
        LOG_DEBUG() << "Attempt to unregister fd " << fd << " that is not in the map";
        return;
    }

    if (index >= per_thread_epoll_fds_.size()) {
        LOG_ERROR() << "Invalid thread index " << index << " for fd " << fd 
            << ", max index is " << (per_thread_epoll_fds_.size() - 1);
        return;
    } 

    bool need_wakeup = false;

    {
        std::lock_guard<std::mutex> lock(epoll_mtx_);
        int epoll_fd = per_thread_epoll_fds_[index];

        auto callback_it = fd_callbacks_.find(fd);
        if (callback_it != fd_callbacks_.end()) {
            fd_callbacks_.erase(callback_it);
            need_wakeup = true;
        }

        if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
            if (errno != ENOENT && errno != EBADF) {
                LOG_ERROR() << "Failed to remove fd " << fd  << " from per-thread epoll: " << strerror(errno);
            }
        }
    }

    if (need_wakeup) {
        WakeupEventLoopThread(index);
    }
}

void TaskProcessor::WakeupEventLoopThread(std::size_t thread_index) const {
    if (UseEvThreadPool() || thread_index >= per_thread_event_fds_.size()) return;
    
    const int event_fd = per_thread_event_fds_[thread_index];
    if (event_fd < 0) return;
    
    uint64_t value = 1;
    ssize_t ret;
    do {
        ret = write(event_fd, &value, sizeof(value));
    } while (ret == -1 && errno == EINTR);
    
    if (ret != sizeof(value)) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            LOG_ERROR() << "Failed to write to event_fd " << event_fd 
                << " (thread " << thread_index << "): " << strerror(errno);
        }
    }
}

void TaskProcessor::WakeupEventLoop() {
    if (UseEvThreadPool()) return;
    
    size_t thread_count = config_.worker_threads;
    if (thread_count == 0) return;

    // In shutdown case, wake up all threads to ensure clean shutdown
    if (is_shutting_down_) {
        for (size_t i = 0; i < thread_count; ++i) {
            WakeupEventLoopThread(i);
        }
        return;
    }

    // 1. Check for spinning threads first
    // 2. If no spinning threads, check for sleeping threads
    // 3. If no sleeping threads, check for least-recently-woken

    // First pass: Find a spinning thread
    for (size_t i = 0; i < thread_count; ++i) {
        if (thread_spinning_[i].load(std::memory_order_acquire)) {
            WakeupEventLoopThread(i);
            return;
        }
    }

    // Second pass: Find a sleeping thread with valid timestamp
    size_t best_thread_idx = SIZE_MAX;
    uint64_t longest_sleep_time = 0;
    
    for (size_t i = 0; i < thread_count; ++i) {
        auto sleep_timestamp = thread_sleep_start_time_[i].load(std::memory_order_acquire);
        
        // Skip threads with uninitialized timestamps
        if (sleep_timestamp == 0) {
            continue;
        }
        
        // Select the thread that has been sleeping the longest
        if (best_thread_idx == SIZE_MAX || sleep_timestamp < longest_sleep_time) {
            best_thread_idx = i;
            longest_sleep_time = sleep_timestamp;
        }
    }

    // If we found a suitable sleeping thread, wake it up
    if (best_thread_idx != SIZE_MAX) {
        WakeupEventLoopThread(best_thread_idx);
        return;
    }

    // No spinning or sleeping threads found
    // Use a random approach
    static std::atomic<size_t> next_thread_index{0};
    size_t thread_index = next_thread_index.fetch_add(1, std::memory_order_relaxed) % thread_count;
    
    WakeupEventLoopThread(thread_index);
}

void TaskProcessor::RunEventLoop(const std::size_t thread_index) {
    const int epoll_fd = per_thread_epoll_fds_[thread_index];
    const int event_fd = per_thread_event_fds_[thread_index];
    
    if (epoll_fd < 0 || event_fd < 0) {
        LOG_ERROR() << "Invalid epoll or event fd for thread " << thread_index;
        // fallback: just do tasks
        ProcessTasks();
        return;
    }

    if (std::holds_alternative<WorkStealingTaskQueue>(task_queue_)) {
        LOG_ERROR() << "RunEventLoop called with WorkStealingTaskQueue, falling back to ProcessTasks";
        ProcessTasks();
        return;
    }

    auto& queue = std::get<TaskQueue>(task_queue_);
    constexpr std::size_t kMaxEvents{256};
    struct epoll_event events[kMaxEvents];

    while (!is_shutting_down_) {
        while (auto context_ptr = queue.PopNonBlocking()) {
            if (!context_ptr.has_value()) {
                // "Stop" token
                is_shutting_down_ = true;
                break;
            }
            if (!context_ptr.value()) continue;
            
            auto context = context_ptr.value();
            bool has_failed = false;
            CheckWaitTime(*context);

            try {
                impl::TaskCounter::RunningToken run_token{GetTaskCounter()};
                context->DoStep();
            } catch (...) {
                LOG_ERROR() << "unhandled exception from DoStep()";
                has_failed = true;
            }
            pools_->GetCoroPool().AccountStackUsage();
            if (has_failed || context->IsFinished()) {
                context->FinishDetached();
            }
        }

        if (is_shutting_down_) break;
        
        // Check again for tasks before going to epoll_wait
        {
            auto context_ptr = queue.PopNonBlocking();
            if (context_ptr.has_value()) {
                if (!context_ptr.value()) {
                    // "Stop" token
                    is_shutting_down_ = true;
                    break;
                }
                
                // Put it back and continue processing from the top
                if (context_ptr.value()) {
                    queue.Push(std::move(context_ptr.value()));
                }
                continue;
            }
        }

        if (is_shutting_down_) break;

        // Perform spinning before epoll_wait
        if (SpinBeforeEpollWait(thread_index)) {
            continue; // Skip epoll_wait if task was found during spin
        }

        thread_sleep_start_time_[thread_index].store(std::chrono::steady_clock::now().time_since_epoch().count(), std::memory_order_release);
        // Double-check if any tasks appeared before we sleep
        if (queue.GetSizeApproximate() > 0 || is_shutting_down_) {
            thread_sleep_start_time_[thread_index].store(0, std::memory_order_release);
            continue;  // Skip epoll_wait and go back to processing tasks
        }
        std::atomic_thread_fence(std::memory_order_seq_cst);
        int ready = epoll_wait(epoll_fd, events, kMaxEvents, -1);
        thread_sleep_start_time_[thread_index].store(0, std::memory_order_release);
        if (is_shutting_down_) break;
        if (ready < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR() << "epoll_wait failed: " << strerror(errno);
            break;
        }

        for (int i = 0; i < ready && !is_shutting_down_; ++i) {
            const auto fd = events[i].data.fd;
            if (fd == event_fd) {
                // Drain the event_fd
                uint64_t buffer;
                ssize_t ret;
                do {
                    ret = read(event_fd, &buffer, sizeof(buffer));
                } while (ret > 0);
                
                if (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                    LOG_ERROR() << "Failed to read from event_fd: " << strerror(errno);
                }
                continue; // Continue to next event
            }
            // Handle regular file descriptor events
            std::unique_lock<std::mutex> lock(epoll_mtx_);
            auto it = fd_callbacks_.find(fd);
            if (it != fd_callbacks_.end()) {
                auto callback = it->second;
                auto event_mask = events[i].events;
                lock.unlock();

                // Include all relevant event types
                uint32_t filtered_events = event_mask & (EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP);
                if (filtered_events) {
                    try {
                        callback(filtered_events);
                    } catch (const std::exception& ex) {
                        LOG_ERROR() << "Exception in fd callback: " << ex;
                    } catch (...) {
                        LOG_ERROR() << "Unknown exception in fd callback";
                    }
                }

                // Don't try to rearm if EPOLLHUP or EPOLLERR were signaled
                // as the file descriptor may no longer be valid
                if (!(event_mask & (EPOLLHUP | EPOLLERR))) {
                    lock.lock();
                    // Check if callback is still registered (could've been removed during execution)
                    it = fd_callbacks_.find(fd);
                    if (it != fd_callbacks_.end()) {
                        struct epoll_event ev;
                        ev.events = (EPOLLIN | EPOLLOUT | EPOLLET);
                        ev.data.fd = fd;
                        if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev) == -1) {
                            if (errno != EBADF && errno != ENOENT) {
                                LOG_ERROR() << "Failed to rearm fd " << fd << ": " << strerror(errno);
                            }
                        }
                    }
                }
            } else {
                lock.unlock();
                // File descriptor registered but no callback found
                LOG_DEBUG() << "Event received for fd " << fd << " with no registered callback";
            }
        }
    }
}

bool TaskProcessor::SpinBeforeEpollWait(std::size_t thread_index) {
    const auto spin_count = config_.spinning_iterations / 5;
    auto& queue = std::get<TaskQueue>(task_queue_);
    bool task_found_during_spin = false;

    thread_spinning_[thread_index].store(true, std::memory_order_relaxed);
    utils::FastScopeGuard spinning_guard([&] () noexcept {
        thread_spinning_[thread_index].store(false, std::memory_order_relaxed);
    });
    for (int i = 0; i < spin_count; ++i) {
        if (queue.GetSizeApproximate() > 0) {
            task_found_during_spin = true;
            break; // Task found, exit spinning
        }
        std::this_thread::yield(); // Relax the CPU
    }
    return task_found_during_spin || is_shutting_down_;
}
#endif  // __linux__

}  // namespace engine

USERVER_NAMESPACE_END