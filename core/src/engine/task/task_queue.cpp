#include <engine/task/task_queue.hpp>

#include <engine/task/task_context.hpp>
#include <userver/logging/log.hpp>

#ifdef __linux__
#include <sys/eventfd.h>
#include <unistd.h>
#endif

USERVER_NAMESPACE_BEGIN

namespace engine {

namespace {
constexpr std::size_t kSemaphoreInitialCount = 0;
}

TaskQueue::TaskQueue(const TaskProcessorConfig& config)
    : queue_semaphore_(kSemaphoreInitialCount, config.spinning_iterations), notify_fd_(-1) {
#ifdef __linux__
    if (config.use_epoll_mode) {
        // Create eventfd for task notifications
        notify_fd_ = eventfd(0, EFD_NONBLOCK);
        if (notify_fd_ == -1) {
            LOG_ERROR() << "Failed to create eventfd: " << strerror(errno);
        }
    }
#endif
}

TaskQueue::~TaskQueue() {
#ifdef __linux__
    if (notify_fd_ != -1) {
        close(notify_fd_);
        notify_fd_ = -1;
    }
#endif
}

void TaskQueue::Push(boost::intrusive_ptr<impl::TaskContext>&& context) {
    UASSERT(context);
    DoPush(context.get());
    context.detach();
}

boost::intrusive_ptr<impl::TaskContext> TaskQueue::PopBlocking() {
    // Current thread handles only a single TaskProcessor, so it's safe to store
    // a token for the task processor in a thread-local variable.
    thread_local moodycamel::ConsumerToken token(queue_);

    impl::TaskContext* raw_context = nullptr;

#ifdef __linux__
    // In epoll mode, DoPopBlocking is only called when a notification was received,
    // so we can try to dequeue directly without waiting
    if (notify_fd_ != -1) {
        if (queue_.try_dequeue(token, raw_context)) {
            boost::intrusive_ptr<impl::TaskContext> context{
                raw_context, /* add_ref= */ false};
            
            if (!context) {
                // return "stop" token back
                DoPush(nullptr);
            }
            
            return context;
        }
        
        // If we're here, it means the notification was received but someone else
        // took the task before us. In EPOLLET mode we need to drain the eventfd
        uint64_t value;
        if (read(notify_fd_, &value, sizeof(value)) == -1 && errno != EAGAIN) {
            LOG_ERROR() << "Failed to read from eventfd: " << strerror(errno);
        }
        
        // Keep trying until we get a task
        while (!queue_.try_dequeue(token, raw_context)) {
            std::this_thread::yield();
        }
    } else {
#endif
        // Semaphore-based blocking approach
        raw_context = DoPopBlocking(token);
#ifdef __linux__
    }
#endif
    boost::intrusive_ptr<impl::TaskContext> context{
        raw_context, /* add_ref= */ false};

    if (!context) {
        // return "stop" token back
        DoPush(nullptr);
    }

    return context;
}

void TaskQueue::StopProcessing() { DoPush(nullptr); }

std::size_t TaskQueue::GetSizeApproximate() const noexcept { return queue_.size_approx(); }

void TaskQueue::PrepareWorker(std::size_t) {}

void TaskQueue::DoPush(impl::TaskContext* context) {
    queue_.enqueue(context);
    
#ifdef __linux__
    if (notify_fd_ != -1) {
        // Notify through eventfd
        const uint64_t increment = 1;
        if (write(notify_fd_, &increment, sizeof(increment)) == -1) {
            if (errno != EAGAIN) {
                LOG_ERROR() << "Failed to write to eventfd: " << strerror(errno);
            }
        }
    } else {
        // Fall back to semaphore if epoll mode is not available
        queue_semaphore_.signal();
    }
#else
    queue_semaphore_.signal();
#endif
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

    return context;
}

std::optional<boost::intrusive_ptr<impl::TaskContext>> TaskQueue::PopNonBlocking() {
    thread_local moodycamel::ConsumerToken token(queue_);
    impl::TaskContext* raw_context = nullptr;

    if (!queue_.try_dequeue(token, raw_context)) {
        // No tasks available
        return std::nullopt;
    }
    if (!raw_context) {
        // "Stop" token
        DoPush(nullptr);
        return boost::intrusive_ptr<impl::TaskContext>(nullptr);
    }

    return boost::intrusive_ptr<impl::TaskContext>(raw_context, /*add_ref=*/false); // Actual task
}

#ifdef __linux__
int TaskQueue::GetNotifyFd() const {
    return notify_fd_;
}
#endif

}  // namespace engine

USERVER_NAMESPACE_END
