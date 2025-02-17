#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <thread>
#include <variant>
#include <vector>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <engine/task/task_counter.hpp>
#include <engine/task/task_processor_config.hpp>
#include <engine/task/task_queue.hpp>
#include <engine/task/work_stealing_queue/task_queue.hpp>
#include <userver/concurrent/impl/interference_shield.hpp>
#include <userver/engine/impl/detached_tasks_sync_block.hpp>
#include <userver/logging/logger.hpp>
#include <utils/statistics/thread_statistics.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

namespace impl {
class TaskContext;
class TaskProcessorPools;
class CountedCoroutinePtr;
}  // namespace impl

namespace ev {
class ThreadPool;
}  // namespace ev

class TaskProcessor final {
public:
    TaskProcessor(TaskProcessorConfig, std::shared_ptr<impl::TaskProcessorPools>);
    ~TaskProcessor();

    void InitiateShutdown();

    void Schedule(impl::TaskContext*);

    void Adopt(impl::TaskContext& context);

    impl::CountedCoroutinePtr GetCoroutine();

    ev::ThreadPool& EventThreadPool();

    std::shared_ptr<impl::TaskProcessorPools> GetTaskProcessorPools() { return pools_; }

    const std::string& Name() const { return config_.name; }

    impl::TaskCounter& GetTaskCounter() noexcept { return task_counter_; }

    const impl::TaskCounter& GetTaskCounter() const { return task_counter_; }

    std::size_t GetTaskQueueSize() const;

    std::size_t GetWorkerCount() const { return workers_.size(); }

    void SetSettings(const TaskProcessorSettings& settings);

    std::chrono::microseconds GetProfilerThreshold() const;

    bool ShouldProfilerForceStacktrace() const;

    std::size_t GetTaskTraceMaxCswForNewTask() const;

    const std::string& GetTaskTraceLoggerName() const;

    void SetTaskTraceLogger(logging::LoggerPtr logger);

    logging::LoggerPtr GetTaskTraceLogger() const;

    std::vector<std::uint8_t> CollectCurrentLoadPct() const;

#ifdef __linux__
    bool UseEvThreadPool() const { return use_ev_thread_pool_; }

    void RegisterFileDescriptor(int fd, uint32_t events, std::function<void(uint32_t)> callback) {
        RegisterFd(fd, events, std::move(callback));
    }

    void UnregisterFileDescriptor(int fd) { UnregisterFd(fd); }
#endif // __linux__

private:
    // Contains queue size cache when overloaded by length, 0 otherwise.
    using OverloadByLength = std::size_t;

    struct OverloadedCache final {
        std::atomic<bool> overloaded_by_wait_time{false};
        std::atomic<OverloadByLength> overload_by_length{0};
    };

    void Cleanup() noexcept;

    void PrepareWorkerThread(std::size_t index);

    void FinalizeWorkerThread() noexcept;

    void ProcessTasks() noexcept;

    void CheckWaitTime(impl::TaskContext& context);

    void SetTaskQueueWaitTimeOverloaded(bool new_value) noexcept;

    void HandleOverload(impl::TaskContext& context, TaskProcessorSettings::OverloadAction);

    OverloadByLength GetOverloadByLength(std::size_t max_queue_length) noexcept;

    OverloadByLength ComputeOverloadByLength(OverloadByLength old_overload_by_length, std::size_t max_queue_length)
        noexcept;

#ifdef __linux__
    void RegisterFd(int fd, uint32_t events, std::function<void(uint32_t)> callback);

    void UnregisterFd(int fd);

    void RunEventLoop(std::size_t index);
#endif  // __linux__

    concurrent::impl::InterferenceShield<impl::DetachedTasksSyncBlock> detached_contexts_{
        impl::DetachedTasksSyncBlock::StopMode::kCancel
    };
    concurrent::impl::InterferenceShield<OverloadedCache> overloaded_cache_;
    std::variant<TaskQueue, WorkStealingTaskQueue> task_queue_;
    impl::TaskCounter task_counter_;

    const TaskProcessorConfig config_;
    const std::shared_ptr<impl::TaskProcessorPools> pools_;
    std::vector<std::thread> workers_;
    logging::LoggerPtr task_trace_logger_{nullptr};

    std::atomic<std::chrono::microseconds> task_profiler_threshold_{{}};
    std::atomic<std::chrono::microseconds> sensor_task_queue_wait_time_{{}};

    std::atomic<std::chrono::microseconds> action_bit_and_max_task_queue_wait_time_{{}};
    std::atomic<std::int64_t> action_bit_and_max_task_queue_wait_length_{0};

    std::atomic<bool> profiler_force_stacktrace_{false};
    std::atomic<bool> is_shutting_down_{false};
    std::atomic<bool> task_trace_logger_set_{false};

    std::unique_ptr<utils::statistics::ThreadPoolCpuStatsStorage> cpu_stats_storage_{nullptr};

#ifdef __linux__
    std::atomic<int> event_fd_{-1};
    std::mutex epoll_mtx_;
    bool use_ev_thread_pool_{false};
    std::unordered_map<int, std::function<void(uint32_t)>> fd_callbacks_;
    std::vector<int> per_thread_epoll_fds_;
#endif  // __linux__
};

/// Register a function that runs on all threads on task processor creation.
/// Used for pre-initializing thread_local variables with heavy constructors
/// (constructor that does blocking system calls, file access, ...):
///
/// @note It is a low-level function. You might not want to use it.
void RegisterThreadStartedHook(std::function<void()>);

}  // namespace engine

USERVER_NAMESPACE_END
