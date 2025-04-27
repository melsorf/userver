#pragma once

#include <atomic>
#include <memory>
#include <optional>

#include <engine/io/epoll_poller.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine {

class TaskProcessor;
namespace impl {
class TaskContext;
}

/// @brief Integration between TaskProcessor and epoll
class TaskProcessorEpoll final {
public:
  explicit TaskProcessorEpoll(TaskProcessor& processor);
  ~TaskProcessorEpoll();

  /// Initialize epoll support for the TaskProcessor
  void Initialize();
  
  /// Add file descriptor to watch
  bool AddFd(int fd, io::EpollPoller::Kind kind, void* user_data = nullptr,
             const io::EpollFlags& flags = io::EpollFlags::kDefault());
  
  /// Modify watched file descriptor
  bool ModifyFd(int fd, io::EpollPoller::Kind kind, void* user_data = nullptr,
                const io::EpollFlags& flags = io::EpollFlags::kDefault());
  
  /// Remove file descriptor from watching
  bool RemoveFd(int fd);
  
  /// Wait for tasks and events
  /// @param may_block Whether the function is allowed to block
  /// @return Task context if available, nullptr otherwise
  boost::intrusive_ptr<impl::TaskContext> WaitForTasksAndEvents(bool may_block);
  
  /// Signal that there are new tasks
  void SignalNewTasks();

  io::EpollPoller* GetEpollPoller() const { return epoll_.get(); }

private:
  static constexpr unsigned kSpinningIterations = 4000;

  // Process events from epoll
  void ProcessEvents();

  TaskProcessor& processor_;
  std::unique_ptr<io::EpollPoller> epoll_;
  int wakeup_event_fd_{-1};
  std::atomic<bool> initialized_{false};
  std::atomic<unsigned> spinning_threads_{0};
};

}  // namespace engine

USERVER_NAMESPACE_END