#include "task_processor.hpp"

#include <sys/types.h>
#include <csignal>
#include <chrono>

#include <fmt/format.h>

#ifdef __linux__
#include <sys/epoll.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <linux/futex.h>
#include <immintrin.h>
#endif // __linux__
#include <cerrno>

#include <concurrent/impl/latch.hpp>
#include <userver/engine/io/exception.hpp>
#include <userver/engine/io/eventfd.hpp>
#include <userver/logging/log.hpp>
#include <userver/utils/assert.hpp>
#include <userver/utils/cpu_relax.hpp>
#include <userver/utils/impl/static_registration.hpp>
#include <userver/utils/numeric_cast.hpp>
#include <userver/utils/rand.hpp>
#include <userver/utils/thread_name.hpp>
#include <userver/utils/threads.hpp>
#include <utils/statistics/thread_statistics.hpp>

#include <engine/task/counted_coroutine_ptr.hpp>
#include <engine/task/task_context.hpp>
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
#ifdef __linux__
            if (config.use_per_thread_epoll) {
                // Epoll is not supported for work stealing
                LOG_WARNING() << "use_per_thread_epoll=true is ignored because task_processor_queue is WorkStealingTaskQueue for TaskProcessor " << config.name;
                // Force disable epoll if mistakenly enabled for work-stealing
                config.use_per_thread_epoll = false;
            }
#endif
            return ResultType{std::in_place_index<1>, std::move(config)};
    }
    UINVARIANT(false, "Unexpected value of TaskQueueType enum");
}

#ifdef __linux__
constexpr int kMaxEpollEvents = 64;
#endif // __linux__

}  // namespace

TaskProcessor::TaskProcessor(TaskProcessorConfig config, std::shared_ptr<impl::TaskProcessorPools> pools)
    : task_queue_(MakeTaskQueue(config)),
      task_counter_(config.worker_threads),
      config_(std::move(config)),
      pools_(std::move(pools))
{
#ifdef __linux__
    // Only allocate epoll data if the feature is enabled and we are using TaskQueue
    per_thread_epoll_data_.resize(
        (config_.use_per_thread_epoll && std::holds_alternative<TaskQueue>(task_queue_))
        ? config_.worker_threads : 0);

    // Ensure epoll is only attempted with TaskQueue
    if (config_.use_per_thread_epoll && !std::holds_alternative<TaskQueue>(task_queue_)) {
        throw std::logic_error(fmt::format(
            "TaskProcessor '{}': use_per_thread_epoll=true is only supported with TaskQueueType::kGlobalTaskQueue",
            Name()
        ));
    }
#endif
    utils::impl::FinishStaticRegistration();
    try {
        LOG_INFO() << "creating task_processor " << Name() << " "
                   << "worker_threads=" << config_.worker_threads << " thread_name=" << config_.thread_name
#ifdef __linux__
                   << " use_per_thread_epoll=" << config_.use_per_thread_epoll
#endif
                   ;
        concurrent::impl::Latch workers_left{static_cast<std::ptrdiff_t>(config_.worker_threads)};
        workers_.reserve(config_.worker_threads);
        for (std::size_t i = 0; i < config_.worker_threads; ++i) {
            workers_.emplace_back([this, i, &workers_left] {
                PrepareWorkerThread(i);
                workers_left.count_down();
#ifdef __linux__
                if (config_.use_per_thread_epoll) {
                    ProcessTasksEpoll();
                } else {
                    ProcessTasks();
                }
#else
                ProcessTasks();
#endif
                FinalizeWorkerThread(i);
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
    // Close epoll fds only if they were created
    if (config_.use_per_thread_epoll && std::holds_alternative<TaskQueue>(task_queue_)) {
        for (auto& epoll_data : per_thread_epoll_data_) {
            if (epoll_data.epoll_fd != -1) {
                close(epoll_data.epoll_fd);
                epoll_data.epoll_fd = -1;
            }
        }
    }
#endif

    UASSERT(!task_counter_.MayHaveTasksAlive());
}

void TaskProcessor::InitiateShutdown() {
    is_shutting_down_ = true;
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
    if (is_shutting_down_) context->RequestCancel(TaskCancellationReason::kShutdown);

    SetTaskQueueWaitTimepoint(context);

    std::visit([&context](auto&& arg) { return arg.Push(context); }, task_queue_);
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
    if (config_.use_per_thread_epoll) {
        UASSERT(index < per_thread_epoll_data_.size());
        per_thread_epoll_data_[index].epoll_fd = epoll_create1(EPOLL_CLOEXEC);
        if (per_thread_epoll_data_[index].epoll_fd == -1) {
            LOG_ERROR() << "Failed to create epoll instance for worker " << index << " of task processor " << Name() << ": " << strerror(errno)
                        << ". Falling back to legacy processing for this thread.";
            // Keep epoll_fd as -1, ProcessTasksEpoll will handle this
        } else {
            // Add task queue eventfd to epoll
            auto* task_queue_ptr = std::get_if<TaskQueue>(&task_queue_);
            UASSERT(task_queue_ptr); // Should always be TaskQueue if use_per_thread_epoll is true
            int task_queue_event_fd = task_queue_ptr->GetEventFd();

            epoll_event ev{};
            ev.events = EPOLLIN | EPOLLET;
            ev.data.ptr = reinterpret_cast<void*>(static_cast<uintptr_t>(EpollDataType::kTaskQueue));
            if (epoll_ctl(per_thread_epoll_data_[index].epoll_fd, EPOLL_CTL_ADD, task_queue_event_fd, &ev) == -1) {
                LOG_ERROR() << "Failed to add task queue eventfd to epoll for worker " << index << " of task processor " << Name() << ": " << strerror(errno)
                            << ". Falling back to legacy processing for this thread.";
                close(per_thread_epoll_data_[index].epoll_fd);
                per_thread_epoll_data_[index].epoll_fd = -1;
            } else {
                LOG_DEBUG() << "Successfully created epoll fd " << per_thread_epoll_data_[index].epoll_fd
                            << " and added task queue eventfd " << task_queue_event_fd << " for worker " << index
                            << " of task processor " << Name();
            }
        }
        // Store thread_id mapping
        {
            std::lock_guard lock(thread_id_map_mutex_);
            thread_id_to_index_[std::this_thread::get_id()] = index;
        }
    }

    {
        std::lock_guard lock(thread_id_map_mutex_);
        thread_id_to_index_[std::this_thread::get_id()] = index;
    }
#endif

    TaskProcessorThreadStartedHook();
}

void TaskProcessor::FinalizeWorkerThread() noexcept {
#ifdef __linux__
    if (config_.use_per_thread_epoll && std::holds_alternative<TaskQueue>(task_queue_)) {
        // Remove thread_id mapping
        {
            std::lock_guard lock(thread_id_map_mutex_);
            thread_id_to_index_.erase(std::this_thread::get_id());
        }
        // Epoll fd is closed in Cleanup after thread joins
    }
#endif
    pools_->GetCoroPool().ClearLocalCache();
}

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

#ifdef __linux__

void TaskProcessor::ProcessSingleTask(impl::TaskContext* context_ptr, size_t worker_index) noexcept {
    UASSERT(context_ptr && !TaskQueue::IsStopToken(context_ptr));
    boost::intrusive_ptr<impl::TaskContext> context{context_ptr, /* add_ref=*/false}; // Manage lifetime
    CheckWaitTime(*context);
    bool has_failed = false;
    try {
        impl::TaskCounter::RunningToken run_token{GetTaskCounter()};
        context->DoStep();
    } catch (const std::exception& ex) {
        LOG_ERROR() << "uncaught exception from DoStep in worker " << worker_index << ": " << ex;
        has_failed = true;
    }

    pools_->GetCoroPool().AccountStackUsage();

    if (has_failed || context->IsFinished()) {
        context->FinishDetached();
    }
}

void TaskProcessor::ProcessTasksEpoll() noexcept {
    size_t worker_index = -1;
    {
        std::lock_guard lock(thread_id_map_mutex_);
        auto it = thread_id_to_index_.find(std::this_thread::get_id());
        if (it == thread_id_to_index_.end()) {
            LOG_ERROR() << "Could not find worker index for thread " << std::this_thread::get_id()
                        << " in task processor " << Name() << ". Falling back to ProcessTasks";
            ProcessTasks();
            return;
        }
        worker_index = it->second;
    }

    // Check if epoll setup failed for this specific thread
    UASSERT(worker_index < per_thread_epoll_data_.size());
    const int epoll_fd = per_thread_epoll_data_[worker_index].epoll_fd;

    if (epoll_fd == -1) {
        LOG_WARNING() << "Epoll is not available for worker " << worker_index << " of task processor " << Name()
                    << ", falling back to ProcessTasks";
        ProcessTasks();
        return;
    }

    auto* task_queue_ptr = std::get_if<TaskQueue>(&task_queue_);
    UASSERT(task_queue_ptr); // Should always be TaskQueue in this function
    const int task_queue_event_fd = task_queue_ptr->GetEventFd();
    
    thread_local moodycamel::ConsumerToken token = task_queue_ptr->GetConsumerToken();

    std::vector<epoll_event> events(kMaxEpollEvents);
    LOG_DEBUG() << "Worker " << worker_index << " starting epoll loop for task processor " << Name();

    while (true) {
        impl::TaskContext* context_ptr = nullptr;

        // 1. Try immediate pop (non-blocking)
        if (task_queue_ptr->DoTryPop(token, context_ptr)) {
            if (TaskQueue::IsStopToken(context_ptr)) {
                LOG_DEBUG() << "Worker " << worker_index << " received stop token (immediate pop)";
                break; // Exit loop
            }
            LOG_TRACE() << "Worker " << worker_index << " popped task (immediate)";
            ProcessSingleTask(context_ptr, worker_index);
            continue; // Go back to immediate pop
        }

        // 2. Spin loop pop
        bool task_found_spinning = false;
        const size_t spin_iterations = config_.spinning_iterations;
        for (size_t i = 0; i < spin_iterations; ++i) {
            if (task_queue_ptr->DoTryPop(token, context_ptr)) {
                if (TaskQueue::IsStopToken(context_ptr)) {
                    LOG_DEBUG() << "Worker " << worker_index << " received stop token (spin pop)";
                    goto stop_processing_label; // Break outer loop
                }
                task_found_spinning = true;
                break; // Exit spin loop
            }
        }

        if (task_found_spinning) {
            LOG_TRACE() << "Worker " << worker_index << " popped task (spin)";
            ProcessSingleTask(context_ptr, worker_index);
            continue; // Go back to immediate pop
        }

        // 3. Blocking wait phase using epoll_wait
        LOG_TRACE() << "Worker " << worker_index << " entering epoll_wait";
        int n_events = epoll_wait(epoll_fd, events.data(), kMaxEpollEvents, -1); // Infinite timeout
        LOG_TRACE() << "Worker " << worker_index << " woke up from epoll_wait with " << n_events << " events";

        if (n_events < 0) {
            if (errno == EINTR) {
                LOG_TRACE() << "Worker " << worker_index << " epoll_wait interrupted, retrying";
                continue; // Interrupted by signal, just retry
            }
            LOG_ERROR() << "epoll_wait failed for worker " << worker_index << " of task processor " << Name() << ": " << strerror(errno);
            continue; // TODO: Consider breaking or specific error handling?
        }

        // Process all received events
        for (int i = 0; i < n_events; ++i) {
            uintptr_t data_type_val = reinterpret_cast<uintptr_t>(events[i].data.ptr);

            if (data_type_val == static_cast<uintptr_t>(EpollDataType::kTaskQueue)) {
                // Task queue eventfd marker
                LOG_TRACE() << "Worker " << worker_index << " received task queue event";
                // Drain eventfd
                uint64_t counter;
                try {
                    while (task_queue_ptr->GetEventFdObject().TryRead(counter)) {
                        LOG_TRACE() << "Worker " << worker_index << " drained " << counter << " from eventfd " << task_queue_event_fd;
                   }
                } catch (const engine::io::IoException& e) {
                    LOG_ERROR() << "Error draining eventfd " << task_queue_event_fd << " for worker " << worker_index << ": " << e;
                } catch (const std::exception& e) {
                     LOG_ERROR() << "Unexpected error draining eventfd " << task_queue_event_fd << " for worker " << worker_index << ": " << e.what();
                }
                // Don't pop here, let the loop restart to check the queue
            } else {
                // External I/O event (or other callback types)
                LOG_TRACE() << "Worker " << worker_index << " received external IO/callback event";
                auto* callback_data = static_cast<EpollCallbackDataBase*>(events[i].data.ptr);
                if (callback_data) {
                    try {
                        callback_data->Invoke(events[i].events);
                    } catch (const std::exception& e) {
                        LOG_ERROR() << "Unhandled exception in epoll callback for worker " << worker_index << ": " << e;
                        // TODO: Consider deregistering the fd or other error handling
                    }
                } else {
                     LOG_ERROR() << "Received epoll event with non-null data.ptr but failed to cast to EpollCallbackDataBase*";
                }
            }
        } // end processing events

        // After processing all epoll events, check the task queue non-blockingly again
        // This handles the case where a task arrived while processing epoll events.
        if (task_queue_ptr->DoTryPop(token, context_ptr)) {
            if (TaskQueue::IsStopToken(context_ptr)) {
                LOG_DEBUG() << "Worker " << worker_index << " received stop token (post-epoll pop)";
                break;
            }
            LOG_TRACE() << "Worker " << worker_index << " popped task (post-epoll)";
            ProcessSingleTask(context_ptr, worker_index);
        }
    }

stop_processing_label: // Label for breaking outer loop
    LOG_DEBUG() << "Worker " << worker_index << " stopping epoll loop for task processor " << Name();
}

int TaskProcessor::GetCurrentThreadEpollFd() const {
    if (!config_.use_per_thread_epoll || !std::holds_alternative<TaskQueue>(task_queue_)) return -1;

    size_t index = -1;
    {
        std::lock_guard lock(thread_id_map_mutex_);
        auto it = thread_id_to_index_.find(std::this_thread::get_id());
        if (it == thread_id_to_index_.end()) {
            // This can happen if called from a non-worker thread or before PrepareWorkerThread completes/succeeds
            LOG_TRACE() << "GetCurrentThreadEpollFd called from unknown thread " << std::this_thread::get_id();
            return -1;
        }
        index = it->second;
    }
    UASSERT(index < per_thread_epoll_data_.size());
    // Return -1 if epoll setup failed for this specific thread
    return per_thread_epoll_data_[index].epoll_fd;
}

bool TaskProcessor::RegisterEpollCallback(int fd, EpollCallbackDataBase& data, uint32_t epoll_events) {
    const int epoll_fd = GetCurrentThreadEpollFd();
    if (epoll_fd == -1) {
        LOG_WARNING() << "Attempt to register epoll callback for fd " << fd << " on thread " << std::this_thread::get_id()
                      << " which does not have a valid epoll fd (TaskProcessor " << Name() << ")";
        return false; // Not an epoll worker thread or epoll setup failed
    }

    epoll_event ev{};
    ev.events = epoll_events | EPOLLET;
    ev.data.ptr = &data;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        LOG_WARNING() << "Failed to add fd " << fd << " to epoll fd " << epoll_fd << ": " << strerror(errno);
        return false;
    }
    LOG_TRACE() << "Registered fd " << fd << " with epoll fd " << epoll_fd;
    return true;
}

bool TaskProcessor::ModifyEpollCallback(int fd, EpollCallbackDataBase& data, uint32_t epoll_events) {
    const int epoll_fd = GetCurrentThreadEpollFd();
    if (epoll_fd == -1) {
        LOG_WARNING() << "Attempt to modify epoll callback for fd " << fd << " on thread " << std::this_thread::get_id()
                    << " which does not have a valid epoll fd (TaskProcessor " << Name() << ")";
        return false;
    }

    epoll_event ev{};
    ev.events = epoll_events | EPOLLET;
    ev.data.ptr = &data;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev) == -1) {
        LOG_WARNING() << "Failed to modify fd " << fd << " in epoll fd " << epoll_fd << ": " << strerror(errno);
        return false;
    }
     LOG_TRACE() << "Modified fd " << fd << " in epoll fd " << epoll_fd;
    return true;
}

bool TaskProcessor::DeregisterEpollCallback(int fd) {
    const int epoll_fd = GetCurrentThreadEpollFd();
    if (epoll_fd == -1) {
        LOG_TRACE() << "Attempt to deregister epoll callback for fd " << fd << " on thread " << std::this_thread::get_id()
                    << " which does not have a valid epoll fd (TaskProcessor " << Name() << ")";
        return false;
    }

    epoll_event ev{};
    if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
        if (errno != ENOENT) {
           LOG_WARNING() << "Failed to remove fd " << fd << " from epoll fd " << epoll_fd << ": " << strerror(errno);
           return false;
        }
        LOG_TRACE() << "Attempted to deregister fd " << fd << " from epoll fd " << epoll_fd << ", but it was not found (ENOENT)";
    } else {
        LOG_TRACE() << "Deregistered fd " << fd << " from epoll fd " << epoll_fd;
    }
    return true;
}

#endif // __linux__

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

}  // namespace engine

USERVER_NAMESPACE_END
