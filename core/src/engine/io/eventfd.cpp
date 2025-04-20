#include <userver/engine/io/eventfd.hpp>

#include <unistd.h>
#ifdef __linux__
#include <sys/eventfd.h>
#else
#include <sys/types.h>
#include <fcntl.h>
#endif

#include <cerrno>

#include <userver/engine/io/exception.hpp>
#include <userver/logging/log.hpp>
#include <userver/utils/assert.hpp>
#include <utils/check_syscall.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine::io {

namespace {

#ifdef __linux__
int FlagsToNative(EventFdCreateFlags flags) {
    int native_flags = 0;
    if (flags & EventFdFlags::kNonblocking) native_flags |= EFD_NONBLOCK;
    if (flags & EventFdFlags::kCloexec) native_flags |= EFD_CLOEXEC;
    if (flags & EventFdFlags::kSemaphore) native_flags |= EFD_SEMAPHORE;
    return native_flags;
}
#endif

} // namespace

struct EventFd::Impl {
    int fd = -1;

#ifndef __linux__
    // Fallback implementation using a pipe for non-Linux systems
    int write_fd = -1;
    // Store flags for fallback behavior simulation
    EventFdCreateFlags create_flags;
#endif

    Impl(unsigned int initial_value, EventFdCreateFlags flags)
#ifndef __linux__
        : create_flags(flags)
#endif
    {
#ifdef __linux__
        int native_flags = FlagsToNative(flags);
        fd = ::eventfd(initial_value, native_flags);
        if (fd == -1) {
            const auto error_code = errno;
            throw IoException("Cannot create eventfd") << IoErrorCode(error_code);
        }
        LOG_DEBUG() << "Created eventfd: " << fd << " with flags=" << flags.Value();
#else
        // Fallback using pipe
        if (initial_value != 0) {
             LOG_WARNING() << "eventfd fallback using pipe does not support non-zero initial value, ignoring initial_value=" << initial_value;
        }
        if (flags & EventFdFlags::kSemaphore) {
             LOG_WARNING() << "eventfd fallback using pipe does not support semaphore semantics, ignoring kSemaphore flag";
        }

        int pipe_flags = 0;
        if (flags & EventFdFlags::kCloexec) pipe_flags |= O_CLOEXEC;
        if (flags & EventFdFlags::kNonblocking) pipe_flags |= O_NONBLOCK;

        int pipe_fds[2] = {-1, -1};
        if (::pipe2(pipe_fds, pipe_flags) == -1) {
            const auto error_code = errno;
            throw IoException("Cannot create pipe for eventfd fallback") << IoErrorCode(error_code);
        }
        fd = pipe_fds[0];       // Read end for polling
        write_fd = pipe_fds[1]; // Write end for signaling
        LOG_DEBUG() << "Created pipe for eventfd fallback: read_fd=" << fd << ", write_fd=" << write_fd << " with flags=" << flags.Value();
#endif
    }

    ~Impl() {
        if (fd != -1) {
            ::close(fd);
            LOG_DEBUG() << "Closed eventfd/pipe read fd: " << fd;
        }
#ifndef __linux__
        if (write_fd != -1) {
            ::close(write_fd);
            LOG_DEBUG() << "Closed pipe write fd: " << write_fd;
        }
#endif
    }

    void Signal(std::uint64_t value) {
        UASSERT(value > 0);
        UASSERT(IsValid());

#ifdef __linux__
        ssize_t ret = ::write(fd, &value, sizeof(value));
        utils::CheckSyscall(ret, "writing to eventfd {}", fd);
        UASSERT(ret == sizeof(value));
#else
        // Write a single byte for pipe fallback
        char buf = 1;
        for (uint64_t i = 0; i < value; ++i) {
             ssize_t ret = ::write(write_fd, &buf, sizeof(buf));
             utils::CheckSyscall(ret, "writing to eventfd pipe {}", write_fd);
             UASSERT(ret == sizeof(buf));
        }
#endif
    }

    std::uint64_t Read() {
        UASSERT(IsValid());
        std::uint64_t value = 0;
#ifdef __linux__
        ssize_t ret = ::read(fd, &value, sizeof(value));
        utils::CheckSyscall(ret, "reading from eventfd {}", fd);
        UASSERT(ret == sizeof(value));
#else
        // If kSemaphore is not set (default), drain the pipe
        char buf;
        bool first_read = true;
        do {
            ssize_t ret = ::read(fd, &buf, sizeof(buf));
            if (ret == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // If it was the first read attempt and non-blocking, throw EAGAIN
                    if (first_read && (create_flags & EventFdFlags::kNonblocking)) {
                         throw IoException("Reading from eventfd pipe failed") << IoErrorCode(errno);
                    }
                    // Otherwise, we read something or blocked, so break
                    break;
                }
                utils::CheckSyscall(ret, "reading from eventfd pipe {}", fd);
            }
            if (ret == 0) { // EOF, should not happen for pipe unless write end closed
                 throw IoException("Reading from eventfd pipe returned 0 (EOF)");
            }
            UASSERT(ret == sizeof(buf));
            value++; // Increment count for each byte read
            first_read = false;
        } while (!(create_flags & EventFdFlags::kSemaphore)); // Drain if not semaphore

        if (value == 0 && !(create_flags & EventFdFlags::kNonblocking)) {
            throw IoException("Reading from eventfd pipe failed") << IoErrorCode(EAGAIN);
        }
#endif
        return value;
    }

     bool TryRead(std::uint64_t& value) {
        UASSERT(IsValid());
        value = 0;
#ifdef __linux__
        ssize_t ret = ::read(fd, &value, sizeof(value));
        if (ret == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return false;
            }
            utils::CheckSyscall(ret, "trying to read from eventfd {}", fd);
        }
        UASSERT(ret == sizeof(value));
        return true;
#else
        // Read one byte at a time for pipe fallback
        char buf;
        do {
            ssize_t ret = ::read(fd, &buf, sizeof(buf));
            if (ret == -1) {
                 if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return value > 0;
                 }
                 utils::CheckSyscall(ret, "trying to read from eventfd pipe {}", fd);
            }
            if (ret == 0) { // EOF
                 return value > 0;
            }
            UASSERT(ret == sizeof(buf));
            value++;
        } while (!(create_flags & EventFdFlags::kSemaphore)); // Drain if not semaphore
        return true; // At least one byte read
#endif
    }


    bool IsValid() const noexcept {
        return fd != -1;
    }
};

EventFd::EventFd(unsigned int initial_value, EventFdCreateFlags flags)
    : impl_(initial_value, flags) {}

EventFd::~EventFd() = default;

int EventFd::GetFd() const noexcept {
    return impl_->fd;
}

bool EventFd::IsValid() const noexcept {
    return impl_->IsValid();
}

void EventFd::Signal() {
    impl_->Signal(1);
}

void EventFd::Signal(std::uint64_t value) {
    impl_->Signal(value);
}

std::uint64_t EventFd::Read() {
    return impl_->Read();
}

bool EventFd::TryRead(std::uint64_t& value) {
    return impl_->TryRead(value);
}


} // namespace engine::io

USERVER_NAMESPACE_END