#pragma once

#include <optional>

#include <moodycamel/blockingconcurrentqueue.h>
#include <moodycamel/lightweightsemaphore.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <engine/task/task_processor_config.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

namespace impl {
class TaskContext;
}  // namespace impl

class TaskQueue final {
public:
    explicit TaskQueue(const TaskProcessorConfig& config);

    void Push(boost::intrusive_ptr<impl::TaskContext>&& context);

    // Returns nullptr as a stop signal
    boost::intrusive_ptr<impl::TaskContext> PopBlocking();

    void StopProcessing();

    std::size_t GetSizeApproximate() const noexcept;

    void PrepareWorker(std::size_t index);

    std::optional<boost::intrusive_ptr<impl::TaskContext>> PopNonBlocking();

private:
    void DoPush(impl::TaskContext* context);

    impl::TaskContext* DoPopBlocking(moodycamel::ConsumerToken& token);

    moodycamel::ConcurrentQueue<impl::TaskContext*> queue_;
    moodycamel::LightweightSemaphore queue_semaphore_;

#ifdef __linux__
    int notify_fd_{-1};  // File descriptor for task notifications via eventfd
public:
    int GetNotifyFd() const;  // For EpollEventDispatcher to use
#endif
};

}  // namespace engine

USERVER_NAMESPACE_END
