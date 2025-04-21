#pragma once

#include <moodycamel/blockingconcurrentqueue.h>
#include <moodycamel/lightweightsemaphore.h>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <engine/task/task_processor_config.hpp>
#include <userver/engine/io/eventfd.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

namespace impl {
class TaskContext;
}  // namespace impl

class TaskQueue final {
public:
    explicit TaskQueue(const TaskProcessorConfig& config);

    void Push(boost::intrusive_ptr<impl::TaskContext>&& context);

    // Non-blocking attempt to pop a task. Returns nullptr if queue is empty.
    // Returns the stop token (nullptr) if it was popped.
    boost::intrusive_ptr<impl::TaskContext> TryPop();

    // Returns nullptr as a stop signal
    boost::intrusive_ptr<impl::TaskContext> PopBlocking();

    void StopProcessing();

    std::size_t GetSizeApproximate() const noexcept;

    void PrepareWorker(std::size_t index);

    // Get the file descriptor of the eventfd used for signaling task availability
    int GetEventFd() const noexcept { return eventfd_.GetFd(); }

    engine::io::EventFd& GetEventFdObject() { return eventfd_; }

    moodycamel::ConsumerToken GetConsumerToken() { return moodycamel::ConsumerToken(queue_); }

    // Non-blocking pop logic, returns true if an item (or stop token) was dequeued
    bool DoTryPop(moodycamel::ConsumerToken& token, impl::TaskContext*& context);

    // Check if the popped context is the stop token
    static bool IsStopToken(impl::TaskContext* context) { return context == nullptr; }

private:
    void DoPush(impl::TaskContext* context);

    impl::TaskContext* DoPopBlocking(moodycamel::ConsumerToken& token);

    moodycamel::ConcurrentQueue<impl::TaskContext*> queue_;
    moodycamel::LightweightSemaphore queue_semaphore_;

    engine::io::EventFd eventfd_;
    std::atomic<bool> is_running_{true};
};

}  // namespace engine

USERVER_NAMESPACE_END
