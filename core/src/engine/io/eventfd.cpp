#include <userver/engine/io/eventfd.hpp>

#include <unistd.h>
#include <sys/eventfd.h>
#include <cerrno>
#include <stdexcept>
#include <string>

#include <userver/engine/io/exception.hpp>
#include <userver/utils/assert.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine::io {

namespace {
int FlagsToNative(EventFdCreateFlags flags) {
    int native_flags = 0;
    if (flags & EventFdFlags::kNonblocking) native_flags |= EFD_NONBLOCK;
    if (flags & EventFdFlags::kSemaphored) native_flags |= EFD_SEMAPHORE;
    if (flags & EventFdFlags::kCloexec) native_flags |= EFD_CLOEXEC;
    return native_flags;
}
} // namespace

EventFd::EventFd(unsigned int initial_value, EventFdCreateFlags flags) {
    fd_ = ::eventfd(initial_value, FlagsToNative(flags));
    if (fd_ == -1) {
        throw std::runtime_error("Failed to create eventfd: " + std::string(strerror(errno)));
    }
}

EventFd::~EventFd() {
    Close();
}

EventFd::EventFd(EventFd&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
}

EventFd& EventFd::operator=(EventFd&& other) noexcept {
    if (this != &other) {
        Close();
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}


int EventFd::GetFd() const noexcept {
    return fd_;
}

void EventFd::Signal() {
    Signal(1);
}

void EventFd::Signal(uint64_t value) {
    UASSERT(fd_ != -1);
    UASSERT(value > 0); // eventfd requires a non-zero value for write

    ssize_t res = ::write(fd_, &value, sizeof(value));
    if (res != sizeof(value)) {
         throw std::runtime_error("Failed to write to eventfd " + std::to_string(fd_) + ": " + std::string(strerror(errno)));
    }
}

uint64_t EventFd::Read() {
    UASSERT(fd_ != -1);
    uint64_t value = 0;
    ssize_t res = ::read(fd_, &value, sizeof(value));

    if (res == sizeof(value)) {
        return value;
    }

    if (res == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
             throw IoWouldBlock(fd_, "eventfd read would block");
        }
        throw IoException("Failed to read from eventfd " + std::to_string(fd_) + ": " + std::string(strerror(errno)));
    }

    throw IoException("Unexpected result from reading eventfd " + std::to_string(fd_) + ": read " + std::to_string(res) + " bytes");
}

bool EventFd::TryRead(uint64_t& value) {
    UASSERT(fd_ != -1);
    ssize_t res = ::read(fd_, &value, sizeof(value));

    if (res == sizeof(value)) {
        return true;
    }

    if (res == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return false;
        }
         throw IoException("Failed to read from eventfd " + std::to_string(fd_) + ": " + std::string(strerror(errno)));
    }

    throw IoException("Unexpected result from reading eventfd " + std::to_string(fd_) + ": read " + std::to_string(res) + " bytes");
}


void EventFd::Close() noexcept {
    if (fd_ != -1) {
        int res = ::close(fd_);
        UASSERT_MSG(res == 0, "Failed to close eventfd");
        fd_ = -1;
    }
}

} // namespace engine::io

USERVER_NAMESPACE_END