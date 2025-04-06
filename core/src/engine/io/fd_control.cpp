#include "fd_control.hpp"

#include <fcntl.h>
#include <sys/resource.h>
#include <unistd.h>

#include <memory>
#include <stdexcept>

#include <userver/engine/task/cancel.hpp>
#include <userver/logging/log.hpp>
#include <userver/utils/assert.hpp>

#include <engine/task/task_context.hpp>
#include <engine/task/task_processor.hpp>
#include <utils/check_syscall.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine::io::impl {
namespace {

int SetNonblock(int fd) {
    int oldflags =
        utils::CheckSyscallCustomException<IoSystemError>(::fcntl(fd, F_GETFL), "getting file status flags, fd={}", fd);
    if (!(oldflags & O_NONBLOCK)) {
        utils::CheckSyscallCustomException<IoSystemError>(
            ::fcntl(fd, F_SETFL, oldflags | O_NONBLOCK), "setting file status flags, fd=", fd
        );
    }
    return fd;
}

int SetCloexec(int fd) {
    int oldflags =
        utils::CheckSyscallCustomException<IoSystemError>(::fcntl(fd, F_GETFD), "getting file status flags, fd={}", fd);
    if (!(oldflags & FD_CLOEXEC)) {
        utils::CheckSyscallCustomException<IoSystemError>(
            ::fcntl(fd, F_SETFD, oldflags | FD_CLOEXEC), "setting file status flags, fd={}", fd
        );
    }
    return fd;
}

int ReduceSigpipe(int fd) {
#ifdef F_SETNOSIGPIPE
    // may fail for all we care, SIGPIPE is ignored anyway
    ::fcntl(fd, F_SETNOSIGPIPE, 1);
#endif
    return fd;
}

}  // namespace

void FdControlDeleter::operator()(FdControl* ptr) const noexcept { std::default_delete<FdControl>{}(ptr); }

#ifndef NDEBUG
Direction::SingleUserGuard::SingleUserGuard(Direction& dir) : dir_(dir) { dir_.poller_.SwitchStateToInUse(); }

Direction::SingleUserGuard::~SingleUserGuard() { dir_.poller_.SwitchStateToReadyToUse(); }
#endif  // #ifndef NDEBUG

// Write operations on socket usually do not block, so it makes sense to reuse
// the same ThreadControl for the sake of better balancing of ev threads.
FdControl::FdControl(const ev::ThreadControl& control) : read_(control), write_(control) {}

bool Direction::Wait(Deadline deadline) {
#ifdef __linux__
    if (is_ready_.load(std::memory_order_acquire)) {
        return true;
    }

    if (is_epoll_mode_ && fd_ >= 0) {
        bool is_pipe = false;
        struct stat st;
        if (fstat(fd_, &st) == 0) {
            is_pipe = S_ISFIFO(st.st_mode);
        }

        if (is_pipe) {
            fd_set fds;
            FD_ZERO(&fds);
            FD_SET(fd_, &fds);
            
            struct timeval tv_zero{0, 0};
            int select_res = 0;
            
            if (kind_ == Kind::kRead) {
                select_res = select(fd_ + 1, &fds, nullptr, nullptr, &tv_zero);
            } else {
                select_res = select(fd_ + 1, nullptr, &fds, nullptr, &tv_zero);
            }
            
            if (select_res > 0 && FD_ISSET(fd_, &fds)) {
                is_ready_.store(true, std::memory_order_release);
                return true;
            }

            auto remaining = deadline.TimeLeft();
            constexpr auto kShortTimeout = std::chrono::milliseconds(10);
            
            while (remaining > std::chrono::milliseconds(0)) {
                auto current_timeout = std::min(remaining, std::chrono::duration_cast<std::chrono::steady_clock::duration>(kShortTimeout));
                bool wait_result = poller_.Wait(Deadline::FromDuration(current_timeout)).has_value();
                
                if (wait_result) {
                    is_ready_.store(true, std::memory_order_release);
                    return true;
                }
                
                if (kind_ == Kind::kRead) {
                    select_res = select(fd_ + 1, &fds, nullptr, nullptr, &tv_zero);
                } else {
                    select_res = select(fd_ + 1, nullptr, &fds, nullptr, &tv_zero);
                }
                
                if (select_res > 0 && FD_ISSET(fd_, &fds)) {
                    is_ready_.store(true, std::memory_order_release);
                    return true;
                }
                
                remaining = deadline.TimeLeft();
            }
            
            return false;
        }
    }
#endif
    
    bool result = poller_.Wait(deadline).has_value();

#ifdef __linux__
    if (result) {
        is_ready_.store(true, std::memory_order_release);
    }
#endif

    return result;
}

FdControl::~FdControl() {
    try {
        Close();
    } catch (const std::exception& e) {
        LOG_ERROR() << "Exception while destructing: " << e;
    }
}

FdControlHolder FdControl::Adopt(int fd) {
    FdControlHolder fd_control{new FdControl(current_task::GetEventThread())};
    // TODO: add conditional CLOEXEC set
    SetCloexec(fd);
    SetNonblock(fd);
    ReduceSigpipe(fd);
    fd_control->read_.Reset(fd, Direction::Kind::kRead);
    fd_control->write_.Reset(fd, Direction::Kind::kWrite);

    bool is_pipe = false;
    struct stat st;
    if (fstat(fd, &st) == 0) {
        is_pipe = S_ISFIFO(st.st_mode);
    }

    // Configure for epoll mode if available
    bool use_epoll = false;
    try {
        auto& task_processor = current_task::GetTaskProcessor();
        use_epoll = task_processor.IsEpollModeEnabled();
    } catch (...) {
        // May be called from non-coroutine context
    }
    
    fd_control->read_.SetEpollMode(use_epoll);
    fd_control->write_.SetEpollMode(use_epoll);

    if (is_pipe && use_epoll) {
        fd_set read_fds, write_fds;
        FD_ZERO(&read_fds);
        FD_ZERO(&write_fds);
        FD_SET(fd, &read_fds);
        FD_SET(fd, &write_fds);
        
        struct timeval tv_zero{0, 0};
        select(fd + 1, &read_fds, &write_fds, nullptr, &tv_zero);
        
        if (FD_ISSET(fd, &read_fds)) {
            fd_control->read_.NotifyReady();
        }
        
        if (FD_ISSET(fd, &write_fds)) {
            fd_control->write_.NotifyReady();
        }
    } else {
        fd_control->write_.NotifyReady();
    }
    
    return fd_control;
}

void FdControl::Close() {
    if (!IsValid()) return;
    const auto fd = Fd();
#ifdef __linux__
     // Notify waiters before closing
    read_.NotifyReady();
    write_.NotifyReady();
#endif
    Invalidate();
    if (fd < 0) return;
    if (::close(fd) == -1) {
        const auto error_code = errno;
        std::error_code ec(error_code, std::system_category());
        UASSERT_MSG(!error_code, "Failed to close fd=" + std::to_string(fd));
        LOG_ERROR() << "Cannot close fd " << fd << ": " << ec.message();
    }
}

void FdControl::Invalidate() {
    read_.Invalidate();
    write_.Invalidate();
}

}  // namespace engine::io::impl

USERVER_NAMESPACE_END
