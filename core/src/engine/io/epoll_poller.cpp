#include <engine/io/epoll_poller.hpp>

#ifdef __linux__
#include <sys/epoll.h>
#include <sys/eventfd.h>
#endif
#include <unistd.h>

#include <stdexcept>
#include <system_error>
#include <unordered_map>

#include <userver/logging/log.hpp>
#include <userver/utils/assert.hpp>

#include <userver/engine/io/fd_poller.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine::io {

namespace {

int GetEpollEvents(EpollPoller::Kind kind, const EpollFlags& flags) {
  int events = 0;
  
  if (static_cast<int>(kind) & static_cast<int>(EpollPoller::Kind::kRead))
    events |= EPOLLIN;
  if (static_cast<int>(kind) & static_cast<int>(EpollPoller::Kind::kWrite))
    events |= EPOLLOUT;
  
  if (flags.edge_triggered)
    events |= EPOLLET;
  if (flags.oneshot)
    events |= EPOLLONESHOT;
    
  return events;
}

EpollPoller::Kind GetKindFromEpollEvents(uint32_t events) {
  bool read = events & EPOLLIN;
  bool write = events & EPOLLOUT;
  
  if (read && write) return EpollPoller::Kind::kReadWrite;
  if (read) return EpollPoller::Kind::kRead;
  if (write) return EpollPoller::Kind::kWrite;
  
  // Default to read if no specific events
  return EpollPoller::Kind::kRead;
}

} // namespace

struct EpollPoller::Impl {
  int epoll_fd_{-1};
  int interrupt_event_fd_{-1};
  
  // Map of file descriptors to their user data
  std::unordered_map<int, void*> fd_to_user_data_;
};

EpollPoller::EpollPoller() : impl_({}) {
  // Create epoll instance
  impl_->epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
  if (impl_->epoll_fd_ < 0) {
    throw std::system_error(errno, std::system_category(), "epoll_create1 failed");
  }

  // Create an eventfd for interrupting epoll_wait
  impl_->interrupt_event_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (impl_->interrupt_event_fd_ < 0) {
    close(impl_->epoll_fd_);
    throw std::system_error(errno, std::system_category(), "eventfd failed");
  }

  // Add the interrupt eventfd to epoll set
  if (!Add(impl_->interrupt_event_fd_, Kind::kRead)) {
    close(impl_->interrupt_event_fd_);
    close(impl_->epoll_fd_);
    throw std::runtime_error("Failed to add interrupt eventfd to epoll set");
  }
}

EpollPoller::~EpollPoller() {
  if (impl_->interrupt_event_fd_ >= 0) {
    close(impl_->interrupt_event_fd_);
  }
  if (impl_->epoll_fd_ >= 0) {
    close(impl_->epoll_fd_);
  }
}

bool EpollPoller::Add(int fd, Kind kind, void* user_data, const EpollFlags& flags) {
  UASSERT(fd >= 0);
  
  struct epoll_event ev{};
  ev.events = GetEpollEvents(kind, flags);
  ev.data.fd = fd;

  int ret = epoll_ctl(impl_->epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
  if (ret == 0) {
    impl_->fd_to_user_data_[fd] = user_data;
    return true;
  }
  
  return false;
}

bool EpollPoller::Modify(int fd, Kind kind, void* user_data, const EpollFlags& flags) {
  UASSERT(fd >= 0);
  
  struct epoll_event ev{};
  ev.events = GetEpollEvents(kind, flags);
  ev.data.fd = fd;

  int ret = epoll_ctl(impl_->epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
  if (ret == 0) {
    impl_->fd_to_user_data_[fd] = user_data;
    return true;
  }
  
  return false;
}

bool EpollPoller::Remove(int fd) {
  UASSERT(fd >= 0);
  
  struct epoll_event ev{};
  int ret = epoll_ctl(impl_->epoll_fd_, EPOLL_CTL_DEL, fd, &ev);
  if (ret == 0) {
    impl_->fd_to_user_data_.erase(fd);
    return true;
  }
  
  return false;
}

std::vector<EpollPoller::Event> EpollPoller::Wait(int timeout_ms) {
  constexpr int kMaxEvents = 64;
  struct epoll_event events[kMaxEvents];

  int nfds = epoll_wait(impl_->epoll_fd_, events, kMaxEvents, timeout_ms);
  if (nfds < 0) {
    if (errno == EINTR)
      return {};
    throw std::system_error(errno, std::system_category(), "epoll_wait failed");
  }

  std::vector<Event> result;
  result.reserve(nfds);

  for (int i = 0; i < nfds; ++i) {
    int fd = events[i].data.fd;
    
    // Handle interrupt event
    if (fd == impl_->interrupt_event_fd_) {
      uint64_t value;
      read(impl_->interrupt_event_fd_, &value, sizeof(value));
      continue;
    }

    auto kind = GetKindFromEpollEvents(events[i].events);
    void* user_data = nullptr;
    
    auto it = impl_->fd_to_user_data_.find(fd);
    if (it != impl_->fd_to_user_data_.end()) {
      user_data = it->second;
    }
    
    result.push_back({fd, kind, user_data});
  }

  return result;
}

int EpollPoller::GetEpollFd() const noexcept {
  return impl_->epoll_fd_;
}

void EpollPoller::Interrupt() {
  const uint64_t value = 1;
  [[maybe_unused]] auto ret = write(impl_->interrupt_event_fd_, &value, sizeof(value));
  // Ignore write errors - it's just for interruption
}

}  // namespace engine::io

USERVER_NAMESPACE_END