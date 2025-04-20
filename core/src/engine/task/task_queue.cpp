#include <engine/task/task_queue.hpp>

#include <engine/task/task_context.hpp>
#include <userver/engine/io/exception.hpp>
#include <userver/utils/assert.hpp>
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
    impl::TaskContext* context{};

    // This piece of code is copy-pasted from
    // moodycamel::BlockingConcurrentQueue::wait_dequeue
    queue_semaphore_.wait();
    while (!queue_.try_dequeue(token, context)) {
        // Can happen when another consumer steals our item in exchange for another
        // item in a Moodycamel sub-queue that we have already passed.
    }

    // If we popped an item, we need to consume the corresponding eventfd signal
    // This is slightly racy but helps keep the eventfd count roughly correct.
    // The epoll loop handles the definitive draining
    if (context) {
        uint64_t counter;
        [[maybe_unused]] auto res = engine::io::util::TryReadFromEventFd(eventfd_, counter);
    }

    return context;
}

}  // namespace engine

USERVER_NAMESPACE_END
