#include "task_processor_epoll.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <engine/task/task_processor.hpp>
#include <engine/task/task_context.hpp>
#include <userver/engine/io/fd_poller.hpp>
#include <userver/logging/log.hpp>
#include <userver/utils/assert.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

TaskProcessorEpoll::TaskProcessorEpoll(TaskProcessor& processor)
    : processor_(processor) {}

TaskProcessorEpoll::~TaskProcessorEpoll() {
  if (wakeup_event_fd_ >= 0) {
    close(wakeup_event_fd_);
  }
}

void TaskProcessorEpoll::Initialize() {
  if (initialized_.exchange(true)) {
    return;  // Already initialized
  }

  epoll_ = std::make_unique<io::EpollPoller>();
  
  // Create eventfd for waking up threads when new tasks arrive
  wakeup_event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wakeup_event_fd_ < 0) {
    throw std::system_error(errno, std::system_category(), "eventfd failed");
  }

  // Add wakeup eventfd to the epoll set
  if (!epoll_->Add(wakeup_event_fd_, io::EpollPoller::Kind::kRead)) {
    close(wakeup_event_fd_);
    wakeup_event_fd_ = -1;
    throw std::runtime_error("Failed to add wakeup eventfd to epoll set");
  }
  
  LOG_INFO() << "TaskProcessorEpoll initialized for " << processor_.Name();
}

bool TaskProcessorEpoll::AddFd(int fd, io::EpollPoller::Kind kind, 
                              void* user_data, 
                              const io::EpollFlags& flags) {
  UASSERT(initialized_);
  UASSERT(epoll_);
  return epoll_->Add(fd, kind, user_data, flags);
}

bool TaskProcessorEpoll::ModifyFd(int fd, io::EpollPoller::Kind kind,
                                 void* user_data,
                                 const io::EpollFlags& flags) {
  UASSERT(initialized_);
  UASSERT(epoll_);
  return epoll_->Modify(fd, kind, user_data, flags);
}

bool TaskProcessorEpoll::RemoveFd(int fd) {
  UASSERT(initialized_);
  UASSERT(epoll_);
  return epoll_->Remove(fd);
}

boost::intrusive_ptr<impl::TaskContext> TaskProcessorEpoll::WaitForTasksAndEvents(bool may_block) {
  UASSERT(initialized_);
  UASSERT(epoll_);

  // Try to get a task first without blocking
  auto task_context = processor_.TryGetTask();
  if (task_context) {
    return task_context;
  }
  
  if (!may_block) {
    // Check for events without blocking
    auto events = epoll_->Wait(0);
    if (!events.empty()) {
      ProcessEvents();
      // Try getting a task again after processing events
      return processor_.TryGetTask();
    }
    return nullptr;
  }
  
  // Spinning phase before going to sleep
  spinning_threads_.fetch_add(1, std::memory_order_relaxed);
  
  for (unsigned i = 0; i < kSpinningIterations; ++i) {
    task_context = processor_.TryGetTask();
    if (task_context) {
      spinning_threads_.fetch_sub(1, std::memory_order_relaxed);
      return task_context;
    }
    
    // Check for events non-blocking during spinning
    if (i % 8 == 0) {  // Check less frequently during spinning
      auto events = epoll_->Wait(0);
      if (!events.empty()) {
        ProcessEvents();
        task_context = processor_.TryGetTask();
        if (task_context) {
          spinning_threads_.fetch_sub(1, std::memory_order_relaxed);
          return task_context;
        }
      }
    }
    
    // Lightweight CPU pause to avoid hogging the CPU
    __builtin_ia32_pause();
  }
  
  spinning_threads_.fetch_sub(1, std::memory_order_relaxed);
  
  // Sleep phase - wait for events or tasks with blocking
  auto events = epoll_->Wait(-1);  // Wait indefinitely
  if (!events.empty()) {
    ProcessEvents();
  }
  
  // Try one more time to get a task
  return processor_.TryGetTask();
}

void TaskProcessorEpoll::SignalNewTasks() {
  if (!initialized_ || wakeup_event_fd_ < 0) {
    return;
  }
  
  // If there are spinning threads, no need to wake up a blocked thread
  if (spinning_threads_.load(std::memory_order_relaxed) > 0) {
    return;
  }
  
  const uint64_t value = 1;
  [[maybe_unused]] auto ret = write(wakeup_event_fd_, &value, sizeof(value));
}

void TaskProcessorEpoll::ProcessEvents() {
  if (!epoll_) return;
  
  auto events = epoll_->Wait(0);  // Non-blocking poll
  
  for (const auto& event : events) {
    if (event.fd == wakeup_event_fd_) {
      uint64_t value;
      read(wakeup_event_fd_, &value, sizeof(value));
      continue;
    }

    // Handle event using the user data pointer
    auto* fd_poller = static_cast<io::FdPoller*>(event.user_data);
    if (fd_poller) {
      fd_poller->WakeupWaiters();
    }
  }
}

}  // namespace engine

USERVER_NAMESPACE_END