#pragma once

/// @file userver/engine/io/epoll_poller.hpp
/// @brief Edge-triggered epoll wrapper

#include <memory>
#include <optional>
#include <vector>
#include <sys/epoll.h>

#include <userver/engine/deadline.hpp>
#include <userver/utils/fast_pimpl.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine::io {

// Forward declaration
class FdPoller;

struct EpollFlags {
  bool edge_triggered{true};
  bool oneshot{false};
  
  static constexpr EpollFlags kDefault() { return {}; }
  static constexpr EpollFlags kOneshot() { return {true, true}; }
};

class EpollPoller final {
public:
  // Using the same Kind enum as FdPoller
  enum class Kind {
    kRead = 1,
    kWrite = 2,
    kReadWrite = 3,
  };

  struct Event {
    int fd{-1};
    Kind kind{Kind::kRead};
    void* user_data{nullptr};
  };

  EpollPoller();
  ~EpollPoller();

  EpollPoller(const EpollPoller&) = delete;
  EpollPoller(EpollPoller&&) = delete;
  EpollPoller& operator=(const EpollPoller&) = delete;
  EpollPoller& operator=(EpollPoller&&) = delete;

  /// Add a file descriptor to the epoll set
  bool Add(int fd, Kind kind, void* user_data = nullptr, 
           const EpollFlags& flags = EpollFlags::kDefault());

  /// Modify a file descriptor in the epoll set
  bool Modify(int fd, Kind kind, void* user_data = nullptr,
              const EpollFlags& flags = EpollFlags::kDefault());

  /// Remove a file descriptor from the epoll set
  bool Remove(int fd);

  /// Wait for events with timeout in milliseconds (-1 for infinite)
  std::vector<Event> Wait(int timeout_ms = -1);

  /// Get the epoll file descriptor
  int GetEpollFd() const noexcept;

  /// Signal that the epoll wait should be interrupted
  void Interrupt();

private:
  struct Impl;
  utils::FastPimpl<Impl, 128, 16> impl_;
};

}  // namespace engine::io

USERVER_NAMESPACE_END