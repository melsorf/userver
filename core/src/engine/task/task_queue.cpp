#include <engine/task/task_queue.hpp>

#include <engine/task/task_context.hpp>
#include <userver/engine/io/exception.hpp>
#include <userver/utils/assert.hpp>
#include <cstdint>
#include <userver/logging/log.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

namespace {
constexpr std::size_t kSemaphoreInitialCount = 0;
}

TaskQueue::TaskQueue(const TaskProcessorConfig& config)
    : queue_semaphore_(kSemaphoreInitialCount, config.spinning_iterations), eventfd_{}
{}

void TaskQueue::Push(boost::intrusive_ptr<impl::TaskContext>&& context) {
    UASSERT(context);
    DoPush(context.get());
    context.detach();
}

boost::intrusive_ptr<impl::TaskContext> TaskQueue::TryPop() {
    thread_local moodycamel::ConsumerToken token(queue_);
    impl::TaskContext* context_ptr = nullptr;
    if (DoTryPop(token, context_ptr)) {
        return boost::intrusive_ptr<impl::TaskContext>{context_ptr, /* add_ref=*/false};
    }
    return nullptr;
}

boost::intrusive_ptr<impl::TaskContext> TaskQueue::PopBlocking() {
    // Current thread handles only a single TaskProcessor, so it's safe to store
    // a token for the task processor in a thread-local variable.
    thread_local moodycamel::ConsumerToken token(queue_);

    boost::intrusive_ptr<impl::TaskContext> context{
        DoPopBlocking(token),
        /* add_ref= */ false};

    if (!context) {
        // return "stop" token back
        DoPush(nullptr);
    }

    return context;
}

void TaskQueue::StopProcessing() { DoPush(nullptr); }

std::size_t TaskQueue::GetSizeApproximate() const noexcept { return queue_.size_approx(); }

int TaskQueue::GetEventFd() const noexcept {
    return eventfd_.GetFd();
}

bool TaskQueue::DoTryPop(moodycamel::ConsumerToken& token, impl::TaskContext*& context) {
    return queue_.try_dequeue(token, context);
}

bool TaskQueue::IsStopToken(impl::TaskContext* context) {
    return context == nullptr;
}

void TaskQueue::PrepareWorker(std::size_t) {}

void TaskQueue::DoPush(impl::TaskContext* context) {
    // This piece of code is copy-pasted from
    // moodycamel::BlockingConcurrentQueue::enqueue
    queue_.enqueue(context);
    queue_semaphore_.signal();
    try {
        eventfd_.Signal(); // Signal eventfd for epoll waiters
    } catch (const std::exception& e) {
        LOG_ERROR() << "Failed to signal eventfd in TaskQueue::DoPush: " << e.what();
    }
}

impl::TaskContext* TaskQueue::DoPopBlocking(moodycamel::ConsumerToken& token) {
    impl::TaskContext* context_ptr = nullptr;
    while (!DoTryPop(token, context_ptr)) {
        queue_semaphore_.wait();
        // Drain eventfd after semaphore wait to avoid redundant wakeups
        uint64_t counter = 0;
        [[maybe_unused]] bool read_success = eventfd_.TryRead(counter);
        // We don't strictly need the result, just draining it is enough
    }
    return context_ptr;
}

}  // namespace engine

USERVER_NAMESPACE_END
