#ifdef __linux__

#include <userver/engine/io/sys_linux/inotify.hpp>

#include <fcntl.h>
#include <unistd.h>
#include <climits>
#include <sys/epoll.h>

#include <userver/engine/task/task_base.hpp>
#include <engine/task/task_processor.hpp>
#include <userver/logging/log.hpp>
#include <userver/utils/assert.hpp>
#include <utils/check_syscall.hpp>

USERVER_NAMESPACE_BEGIN

namespace engine::io::sys_linux {

std::string ToString(EventType type) {
    switch (type) {
        case EventType::kNone:
            return "none";
        case EventType::kAccess:
            return "access";
        case EventType::kOpen:
            return "open";
        case EventType::kAttribChanged:
            return "attrib-changed";
        case EventType::kCloseWrite:
            return "close-write";
        case EventType::kCloseNoWrite:
            return "close-nowrite";
        case EventType::kCreate:
            return "create";
        case EventType::kDelete:
            return "delete";
        case EventType::kDeleteSelf:
            return "delete-self";
        case EventType::kModify:
            return "modify";
        case EventType::kMovedFrom:
            return "moved-from";
        case EventType::kMovedTo:
            return "moved-to";
        case EventType::kMoveSelf:
            return "move-self";
        case EventType::kOnlyDir:
            return "only-dir";
        case EventType::kIsDir:
            return "is-dir";
    }

    return fmt::format("unknown {}", static_cast<int>(type));
}

std::string ToString(EventTypeMask mask) { return ToString(static_cast<EventType>(mask.GetValue())); }

logging::LogHelper& operator<<(logging::LogHelper& lh, const Event& event) noexcept {
    lh << fmt::format("({}, {})", event.path, ToString(event.mask));
    return lh;
}

Inotify::Inotify() : fd_(engine::current_task::GetEventThread()),
    use_ev_thread_pool_(engine::current_task::GetTaskProcessor().UseEvThreadPool()) 
{
    fd_.Reset(inotify_init(), FdPoller::Kind::kRead);
    UASSERT(fd_.GetFd() != -1);

    if (!use_ev_thread_pool_) {
        // Set non-blocking mode
        int flags = fcntl(fd_.GetFd(), F_GETFL, 0);
        utils::CheckSyscall(flags, "fcntl(F_GETFL)");
        utils::CheckSyscall(fcntl(fd_.GetFd(), F_SETFL, flags | O_NONBLOCK), "fcntl(F_SETFL)");
        
        engine::current_task::GetTaskProcessor().RegisterFileDescriptor(fd_.GetFd(), EPOLLIN | EPOLLET,
            [this](uint32_t events) {
                if (events & EPOLLIN) {
                    Dispatch();
                }
            }
        );
    }
}

Inotify::~Inotify() {
    if (!engine::current_task::GetTaskProcessor().UseEvThreadPool()) {
        engine::current_task::GetTaskProcessor().UnregisterFileDescriptor(fd_.GetFd());
    }
    
    auto fd = fd_.GetFd();
    if (fd != -1) {
        close(fd);
    }
}

void Inotify::AddWatch(const std::string& path, EventTypeMask flags) {
    auto wd = utils::CheckSyscall(
        inotify_add_watch(fd_.GetFd(), path.c_str(), static_cast<int>(flags.GetValue())), "inotify_add_watch"
    );

    path_to_wd_[path] = wd;
    wd_to_path_[wd] = path;
}

void Inotify::RmWatch(const std::string& path) {
    auto wd = path_to_wd_.at(path);
    path_to_wd_.erase(path);
    wd_to_path_.erase(wd);

    utils::CheckSyscall(inotify_rm_watch(fd_.GetFd(), wd), "inotify_rm_watch");
}

std::optional<Event> Inotify::Poll(engine::Deadline deadline) {
    if (!pending_events_.empty()) {
        auto front = pending_events_.front();
        pending_events_.pop();
        return front;
    }

    auto kind = fd_.Wait(deadline);
    if (!kind) return {};

    Dispatch();

    if (!pending_events_.empty()) {
        auto front = pending_events_.front();
        pending_events_.pop();
        return front;
    }
    return {};
}

void Inotify::Dispatch() {
    char buff[sizeof(inotify_event) + NAME_MAX + 1];

    auto process_buffer = [this, &buff](ssize_t len) {
        for (ssize_t pos = 0; pos < len;) {
            // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
            auto* event = reinterpret_cast<inotify_event*>(buff + pos);
            pos += sizeof(inotify_event) + event->len;
            std::string path =
                event->len ? (wd_to_path_[event->wd] + '/' + std::string{event->name}) : wd_to_path_[event->wd];
            pending_events_.push(Event{std::move(path), EventTypeMask(static_cast<EventType>(event->mask))});
        }
    };

    // For ev thread pool mode, do a single read; for epoll, read until no more events
    const bool repeat = !use_ev_thread_pool_;

    while (true) {
        auto len = read(fd_.GetFd(), &buff, sizeof(buff));
        if (len < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // No more data available; this is normal condition
                // for edge-triggered mode when all data has been consumed
                break;
            }
            utils::CheckSyscall(len, "read");
        } else if (len == 0) {
            // EOF case
            break;
        }
        process_buffer(len);
        if (!repeat) break;
    }
}

}  // namespace engine::io::sys_linux

USERVER_NAMESPACE_END

#endif  // __linux__
