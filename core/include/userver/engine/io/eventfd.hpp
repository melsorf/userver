#pragma once

#include <cstdint>

#include <userver/utils/flags.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine::io {

/// @brief Flags for creating an eventfd
enum class EventFdFlags {
    kNone = 0,
    kNonblocking = 1 << 0, // Set O_NONBLOCK flag on the new fd
    kSemaphored = 1 << 1,  // Provide semaphore-like semantics for reads
    kCloexec = 1 << 2,     // Set FD_CLOEXEC flag on the new fd
};
using EventFdCreateFlags = utils::Flags<EventFdFlags>;

/// @brief Provides a file descriptor that can be used for event notification.
/// Writing to the eventfd increments an internal counter, reading consumes it.
/// Can be used with epoll/poll.
class EventFd final {
public:
    /// @brief Creates an eventfd.
    /// @param initial_value The initial value of the eventfd counter.
    /// @param flags Flags for eventfd creation (e.g., non-blocking, cloexec)
    explicit EventFd(unsigned int initial_value = 0,
                     EventFdCreateFlags flags = EventFdFlags::kNonblocking | EventFdFlags::kCloexec);

    ~EventFd();

    EventFd(const EventFd&) = delete;
    EventFd& operator=(const EventFd&) = delete;
    EventFd(EventFd&& other) noexcept;
    EventFd& operator=(EventFd&& other) noexcept;

    /// @brief Returns the underlying file descriptor.
    int GetFd() const noexcept;

    /// @brief Signals the eventfd by adding 1 to its counter.
    void Signal();

    /// @brief Signals the eventfd by adding the specified value to its counter.
    void Signal(uint64_t value);

    /// @brief Reads the current value from the eventfd counter.
    /// If the eventfd was created with kSemaphored, this reads 1 and decrements the counter by 1.
    /// Otherwise, it reads the current counter value and resets it to 0.
    uint64_t Read();

    /// @brief Tries to read the current value from the eventfd counter without blocking.
    bool TryRead(uint64_t& value);

private:
    void Close() noexcept;

    int fd_{-1};
};

} // namespace engine::io

USERVER_NAMESPACE_END