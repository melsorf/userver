#include "task_processor.hpp"

#include <sys/types.h>
#include <csignal>

#include <fmt/format.h>

#include <concurrent/impl/latch.hpp>
#include <userver/logging/log.hpp>
#include <userver/utils/assert.hpp>
#include <userver/utils/impl/static_registration.hpp>
#include <userver/utils/numeric_cast.hpp>
#include <userver/utils/rand.hpp>
#include <userver/utils/thread_name.hpp>
#include <userver/utils/threads.hpp>
#include <utils/statistics/thread_statistics.hpp>

#include <engine/task/counted_coroutine_ptr.hpp>
#include <engine/task/task_context.hpp>
#include <engine/task/task_processor_pools.hpp>

#ifdef __linux__
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>
#endif

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

auto MakeTaskQueue(TaskProcessorConfig config) {
    using ResultType = std::variant<TaskQueue, WorkStealingTaskQueue>;
    // Force TaskQueue if epoll integration is enabled, as work-stealing is not yet supported
#ifdef __linux__
    if (config.use_epoll_io_poller) {
        if (config.task_processor_queue == TaskQueueType::kWorkStealingTaskQueue) {
             LOG_WARNING() << "WorkStealingTaskQueue is not supported with use_epoll_io_poller=true, forcing GlobalTaskQueue.";
        }
        return ResultType{std::in_place_index<0>, std::move(config)};
    }
#endif
    switch (config.task_processor_queue) {
        case TaskQueueType::kGlobalTaskQueue:
            return ResultType{std::in_place_index<0>, std::move(config)};
        case TaskQueueType::kWorkStealingTaskQueue:
            return ResultType{std::in_place_index<1>, std::move(config)};
    }
    UINVARIANT(false, "Unexpected value of TaskQueueType enum");
}

#ifdef __linux__
// Constants for epoll
constexpr int kMaxEpollEvents = 16; // Max events to process per epoll_wait call
constexpr uint64_t kEventFdSignalValue = 1;

// Helper to get current thread index
std::optional<std::size_t> GetCurrentWorkerIndex(const std::string& expected_prefix) {
    std::string current_name = utils::GetCurrentThreadName();
    if (current_name.rfind(expected_prefix, 0) == 0) {
        try {
            return std::stoull(current_name.substr(expected_prefix.length()));
        } catch (...) {
        }
    }
    return std::nullopt;
}
#endif

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

}  // namespace

TaskProcessor::TaskProcessor(TaskProcessorConfig config, std::shared_ptr<impl::TaskProcessorPools> pools)
    : task_queue_(MakeTaskQueue(config)),
      task_counter_(config.worker_threads),
      config_(std::move(config)),
      pools_(std::move(pools))
#ifdef __linux__
      , epoll_states_(config_.use_epoll_io_poller ? config_.worker_threads : 0),
      worker_states_(config_.use_epoll_io_poller ? config_.worker_threads : 0)
#endif
       {
    utils::impl::FinishStaticRegistration();
    try {
        LOG_INFO() << "creating task_processor " << Name() << " "
                   << "worker_threads=" << config_.worker_threads << " thread_name=" << config_.thread_name
#ifdef __linux__
                   << " use_epoll_io_poller=" << config_.use_epoll_io_poller
#endif
                   ;
        concurrent::impl::Latch workers_left{static_cast<std::ptrdiff_t>(config_.worker_threads)};
        workers_.reserve(config_.worker_threads);
        for (std::size_t i = 0; i < config_.worker_threads; ++i) {
            workers_.emplace_back([this, i, &workers_left] {
                PrepareWorkerThread(i);
                workers_left.count_down();
                ProcessTasks(i);
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

#ifdef __linux__
    // Wake up any epoll-waiting threads
    if (config_.use_epoll_io_poller) {
        for (auto& state : epoll_states_) {
            if (state.task_event_fd != -1) {
                uint64_t val = kEventFdSignalValue;
                [[maybe_unused]] auto res = ::write(state.task_event_fd, &val, sizeof(val));
            }
        }
    }
#endif

    for (auto& w : workers_) {
        w.join();
    }

    UASSERT(!task_counter_.MayHaveTasksAlive());
}

void TaskProcessor::InitiateShutdown() {
    is_shutting_down_ = true;
    detached_contexts_->RequestCancellation(TaskCancellationReason::kShutdown);
}

#ifdef __linux__
bool TaskProcessor::TrySignalIdleWorker() {
    static std::atomic<std::size_t> next_worker_idx{0};
    std::size_t attempts = 0;
    const std::size_t num_workers = config_.worker_threads;

    while (attempts < num_workers) {
        std::size_t current_idx = next_worker_idx.fetch_add(1) % num_workers;
        WorkerState expected_idle = WorkerState::kIdle;
        WorkerState expected_spinning = WorkerState::kSpinning;
        WorkerState expected_sleeping = WorkerState::kSleeping;

        // Spinning > sleeping
        if (worker_states_[current_idx].compare_exchange_strong(expected_spinning, WorkerState::kRunning) ||
            worker_states_[current_idx].compare_exchange_strong(expected_sleeping, WorkerState::kRunning))
        {
            if (epoll_states_[current_idx].task_event_fd != -1) {
                uint64_t val = kEventFdSignalValue;
                [[maybe_unused]] auto res = ::write(epoll_states_[current_idx].task_event_fd, &val, sizeof(val));
                LOG_TRACE() << "Signalled worker " << current_idx << " via eventfd";
                return true;
            }
            // If fd is bad, try next worker
        }
        attempts++;
    }

    LOG_TRACE() << "No idle or sleeping worker found to signal";
    return false;
}
#endif

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
    if (is_shutting_down_) context->RequestCancel(TaskCancellationReason::kShutdown);

    SetTaskQueueWaitTimepoint(context);

    std::visit([&context](auto&& arg) { return arg.Push(context); }, task_queue_);

#ifdef __linux__
    // If using epoll, try to wake up a specific idle/sleeping worker
    if (config_.use_epoll_io_poller) {
        TrySignalIdleWorker();
        // If no idle worker was signaled, the task will be picked up eventually
        // by a worker finishing its current task or waking from epoll_wait.
    }
#endif
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

void TaskProcessor::PrepareWorkerThread(std::size_t index) noexcept {
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

#ifdef __linux__
    // Initialize epoll state if enabled
    if (config_.use_epoll_io_poller) {
        UASSERT(index < epoll_states_.size());
        UASSERT(index < worker_states_.size());

        worker_states_[index].store(WorkerState::kIdle); // Initial state

        auto& state = epoll_states_[index];
        state.epoll_fd = epoll_create1(EPOLL_CLOEXEC);
        if (state.epoll_fd == -1) {
            LOG_ERROR() << "Failed to create epoll fd for worker " << index << ": " << strerror(errno);
            // How to handle this? Maybe fall back?
            return;
        }

        state.task_event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
        if (state.task_event_fd == -1) {
            LOG_ERROR() << "Failed to create eventfd for worker " << index << ": " << strerror(errno);
             ::close(state.epoll_fd);
             state.epoll_fd = -1;
             return;
        }

        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = state.task_event_fd; // Associate event with task_event_fd
        if (epoll_ctl(state.epoll_fd, EPOLL_CTL_ADD, state.task_event_fd, &ev) == -1) {
            LOG_ERROR() << "Failed to add eventfd to epoll for worker " << index << ": " << strerror(errno);
            ::close(state.epoll_fd);
            ::close(state.task_event_fd);
            state.epoll_fd = -1;
            state.task_event_fd = -1;
            return;
        }
         LOG_DEBUG() << "Worker " << index << " initialized epoll_fd=" << state.epoll_fd << ", task_event_fd=" << state.task_event_fd;
    }
#endif

    TaskProcessorThreadStartedHook();
}

void TaskProcessor::FinalizeWorkerThread() noexcept { pools_->GetCoroPool().ClearLocalCache(); }

void TaskProcessor::ProcessTasks(std::size_t worker_index) noexcept {
#ifdef __linux__
    if (config_.use_epoll_io_poller && epoll_states_[worker_index].epoll_fd != -1) {
        ProcessTasksEpoll(worker_index);
    } else {
        ProcessTasks();
    }
#else
    ProcessTasks();
#endif
}

#ifdef __linux__
// Epoll-based worker loop
void TaskProcessor::ProcessTasksEpoll(std::size_t worker_index) noexcept {
    LOG_DEBUG() << "Worker " << worker_index << " starting epoll processing loop.";
    auto& state = epoll_states_[worker_index];
    auto& worker_state = worker_states_[worker_index];
    struct epoll_event events[kMaxEpollEvents];
    TaskQueue& task_queue = std::get<TaskQueue>(task_queue_);

    while (true) {
        boost::intrusive_ptr<impl::TaskContext> context{nullptr};

        // 1. Spinning
        worker_state.store(WorkerState::kSpinning);
        for (std::size_t i = 0; i < config_.epoll_spinning_iterations; ++i) {
            context = task_queue.PopNonBlocking();
            if (context) break;
             // cpu relaxation like _mm_pause() here?
        }
        worker_state.store(WorkerState::kIdle); // Back to idle before potential sleep

        // 2. Process Task if found during spin
        if (context) {
             if (!context) break; // Check for stop signal
             worker_state.store(WorkerState::kRunning);
             // Process task - same logic as legacy
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
             worker_state.store(WorkerState::kIdle); // Task finished, back to idle
             continue; // Loop again to check for more tasks immediately
        }

        // 3. Epoll Wait Phase
        LOG_TRACE() << "Worker " << worker_index << " entering epoll_wait.";
        worker_state.store(WorkerState::kSleeping); 
        int n_events = epoll_wait(state.epoll_fd, events, kMaxEpollEvents, -1); 
        worker_state.store(WorkerState::kRunning);
        LOG_TRACE() << "Worker " << worker_index << " woke up from epoll_wait with " << n_events << " events.";


        if (n_events == -1) {
            if (errno == EINTR) continue; // Interrupted by signal, just retry
            LOG_ERROR() << "epoll_wait failed for worker " << worker_index << ": " << strerror(errno);
            break;
        }

        bool task_found_in_epoll = false;
        for (int i = 0; i < n_events; ++i) {
            int fd = events[i].data.fd;
            uint32_t revents = events[i].events;

            if (fd == state.task_event_fd) {
                // Task notification
                uint64_t value;
                // Read from eventfd to reset it
                [[maybe_unused]] auto res = ::read(fd, &value, sizeof(value));
                // Even if read fails (EAGAIN), the event was signaled. Try popping.
                LOG_TRACE() << "Worker " << worker_index << " received task notification via eventfd.";

                // Try to pop tasks until queue is empty
                while(true) {
                     context = task_queue.PopNonBlocking();
                     if (!context) break; // No more tasks for now
                     task_found_in_epoll = true; // Mark that we got a task this cycle

                     // Check for stop signal
                     if (!context) {
                         LOG_DEBUG() << "Worker " << worker_index << " received stop signal.";
                         worker_state.store(WorkerState::kIdle);
                         return;
                     }

                     // Process the task
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
            } else {
                // I/O event
                LOG_TRACE() << "Worker " << worker_index << " received IO event for fd=" << fd;
                impl::TaskContext* io_context = nullptr;
                {
                    // Find the task associated with this fd
                    std::lock_guard lock(state.fd_map_mutex);
                    auto it = state.fd_map.find(fd);
                    if (it != state.fd_map.end()) {
                        io_context = it->second;
                    } else {
                         LOG_WARNING() << "Worker " << worker_index << " received epoll event for unknown fd=" << fd;
                    }
                }

                if (io_context) {
                    // Wake up the task.
                    io_context->Wakeup(impl::TaskContext::WakeupSource::kIoWait);
                }
            }
        }
        worker_state.store(WorkerState::kIdle);

    } 
     LOG_DEBUG() << "Worker " << worker_index << " exiting epoll processing loop.";
}
#endif // __linux__

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
// Add fd to the current worker's epoll set
bool TaskProcessor::StartEpollPolling(int fd, impl::TaskContext* context, int epoll_events) {
    if (!config_.use_epoll_io_poller) return false;

    auto worker_index_opt = GetCurrentWorkerIndex(config_.thread_name + "_");
    if (!worker_index_opt) {
        LOG_ERROR() << "Could not determine worker index in StartEpollPolling";
        return false;
    }
    std::size_t worker_index = *worker_index_opt;

    UASSERT(worker_index < epoll_states_.size());
    auto& state = epoll_states_[worker_index];
    if (state.epoll_fd == -1) return false; // Epoll not initialized for this worker

    struct epoll_event ev;
    ev.events = epoll_events | EPOLLET | EPOLLONESHOT;
    ev.data.fd = fd;

    LOG_TRACE() << "Worker " << worker_index << " adding fd=" << fd << " to epoll_fd=" << state.epoll_fd;

    {
        std::lock_guard lock(state.fd_map_mutex);
        state.fd_map[fd] = context;
    }

    if (epoll_ctl(state.epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        // If it already exists (EEXIST), maybe modify? For ONESHOT, ADD might be okay.
        // If ADD fails, remove from map.
        if (errno == EEXIST) {
             LOG_TRACE() << "fd=" << fd << " already exists in epoll set, attempting MOD";
             if (epoll_ctl(state.epoll_fd, EPOLL_CTL_MOD, fd, &ev) == -1) {
                 LOG_ERROR() << "Failed to MOD fd=" << fd << " in epoll for worker " << worker_index << ": " << strerror(errno);
                 std::lock_guard lock(state.fd_map_mutex);
                 state.fd_map.erase(fd);
                 return false;
             }
        } else {
            LOG_ERROR() << "Failed to ADD fd=" << fd << " to epoll for worker " << worker_index << ": " << strerror(errno);
            std::lock_guard lock(state.fd_map_mutex);
            state.fd_map.erase(fd);
            return false;
        }
    }
    return true;
}

// Remove fd from the current worker's epoll set
bool TaskProcessor::StopEpollPolling(int fd) {
    if (!config_.use_epoll_io_poller) return false;

   // Find current worker index
   auto worker_index_opt = GetCurrentWorkerIndex(config_.thread_name + "_");
    if (!worker_index_opt) {
        LOG_ERROR() << "Could not determine worker index in StopEpollPolling";
        return false;
    }
   std::size_t worker_index = *worker_index_opt;


   UASSERT(worker_index < epoll_states_.size());
   auto& state = epoll_states_[worker_index];
   if (state.epoll_fd == -1) return false; // Epoll not initialized

   LOG_TRACE() << "Worker " << worker_index << " removing fd=" << fd << " from epoll_fd=" << state.epoll_fd;

   // Remove from epoll first. Ignore errors (e.g., fd already removed).
   if (epoll_ctl(state.epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
       // ENOENT (No such file or directory) is expected if already removed or closed.
       if (errno != ENOENT) {
            LOG_WARNING() << "Failed to DEL fd=" << fd << " from epoll for worker " << worker_index << ": " << strerror(errno);
            // Continue to remove from map anyway
       }
   }

   // Remove from map
   {
       std::lock_guard lock(state.fd_map_mutex);
       state.fd_map.erase(fd);
   }
   return true;
}
#endif // __linux__

}  // namespace engine

USERVER_NAMESPACE_END
