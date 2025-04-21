#include <userver/engine/io/fd_poller.hpp>

#include <engine/ev/thread_control.hpp>
#include <engine/ev/watcher.hpp>
#include <engine/impl/future_utils.hpp>
#include <engine/impl/wait_list_light.hpp>
#include <engine/task/task_context.hpp>
#include <engine/task/task_processor.hpp>
#include <userver/utils/assert.hpp>
#include <userver/utils/atomic.hpp>
#include <userver/logging/log.hpp>

template <>
struct fmt::formatter<USERVER_NAMESPACE::engine::io::FdPoller::State> {
    static constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template <typename State, typename FormatContext>
    auto format(State state, FormatContext& ctx) const {
        std::string_view str = "broken";
        switch (state) {
            case State::kInvalid:
                str = "invalid";
                break;
            case State::kReadyToUse:
                str = "ready to use";
                break;
            case State::kInUse:
                str = "in use";
                break;
        }

        return fmt::format_to(ctx.out(), "{}", str);
    }
};

USERVER_NAMESPACE_BEGIN

namespace engine::io {

namespace {

#ifdef __linux__
uint32_t ToEpollEvents(FdPoller::Kind kind) {
    uint32_t events = 0;
    if (static_cast<int>(kind) & static_cast<int>(FdPoller::Kind::kRead)) {
        events |= EPOLLIN;
    }
    if (static_cast<int>(kind) & static_cast<int>(FdPoller::Kind::kWrite)) {
        events |= EPOLLOUT;
    }
    return events;
}
    
std::optional<FdPoller::Kind> FromEpollEvents(uint32_t events) {
    int kind_int = 0;
    if (events & (EPOLLIN | EPOLLERR | EPOLLHUP)) {
        kind_int |= static_cast<int>(FdPoller::Kind::kRead);
    }
    if (events & (EPOLLOUT | EPOLLERR | EPOLLHUP)) {
        kind_int |= static_cast<int>(FdPoller::Kind::kWrite);
    }

    if (kind_int == 0) return std::nullopt;
    return static_cast<FdPoller::Kind>(kind_int);
}
#endif // __linux__
    
int ToEvEvents(FdPoller::Kind kind) {
    int events = 0;
    if (static_cast<int>(kind) & static_cast<int>(FdPoller::Kind::kRead)) {
        events |= EV_READ;
    }
    if (static_cast<int>(kind) & static_cast<int>(FdPoller::Kind::kWrite)) {
        events |= EV_WRITE;
    }
    return events;
}

FdPoller::Kind GetUserMode(int ev_events) {
    if ((ev_events & EV_READ) && (ev_events & EV_WRITE)) {
        return FdPoller::Kind::kReadWrite;
    }

    if (ev_events & EV_READ) {
        return FdPoller::Kind::kRead;
    }

    if (ev_events & EV_WRITE) {
        return FdPoller::Kind::kWrite;
    }

    UINVARIANT(false, "Failed to recognize events that happened on the socket.");
}

}  // namespace

struct FdPoller::Impl final : public engine::impl::ContextAccessor
#ifdef __linux__
                            , public engine::EpollCallbackDataBase
#endif
{
    Impl(ev::ThreadControl& control);

    ~Impl();

    engine::impl::TaskContext::WakeupSource DoWait(Deadline deadline);

    bool IsValid() const noexcept;

    void Invalidate();
    void Reset(int fd, Kind kind);

    void StopWatcher() noexcept;

#ifdef __linux__
    // EpollCallbackDataBase implementation
    void Invoke(uint32_t epoll_events) override;
#endif

    static void IoWatcherCb(struct ev_loop*, ev_io*, int) noexcept;
    void WakeupWaiters();

    void ResetReady() noexcept { waiters_->GetAndResetSignal(); }

    // ContextAccessor implementation
    bool IsReady() const noexcept override { return waiters_->IsSignaled(); }

    engine::impl::EarlyWakeup TryAppendWaiter(engine::impl::TaskContext& waiter) override {
        if (waiters_->GetSignalOrAppend(&waiter)) {
            return engine::impl::EarlyWakeup{true};
        }
        // Start watcher only if epoll is not being used or failed
#ifdef __linux__
        if (!using_epoll_) {
            watcher_.StartAsync();
        }
#else
        watcher_.StartAsync();
#endif
        return engine::impl::EarlyWakeup{false};
    }

    void RemoveWaiter(engine::impl::TaskContext& waiter) noexcept override {
        waiters_->Remove(waiter);
        // we need to stop watcher manually to avoid racy wakeups later
#ifdef __linux__
        if (!using_epoll_) {
            watcher_.StopAsync();
        }
#else
        watcher_.StopAsync();
#endif
    }

    void AfterWait() noexcept override {
#ifdef __linux__
        if (!using_epoll_) {
             watcher_.Stop();
        }
#else
        watcher_.Stop();
#endif
    }

    void RethrowErrorResult() const override {}

    void SwitchStateToInUse();
    void SwitchStateToReadyToUse();

    int fd_ = -1;
    Kind kind_ = Kind::kRead;
    std::atomic<FdPoller::State> state_{FdPoller::State::kInvalid};
    ev::ThreadControl& control_;
    engine::TaskProcessor* task_processor_ = nullptr;
    engine::impl::FastPimplWaitListLight waiters_;
    ev::Watcher<ev_io> watcher_;
    std::atomic<FdPoller::Kind> events_that_happened_{};

#ifdef __linux__
    bool using_epoll_ = false; // Track if epoll is currently used for this fd
#endif
};

void FdPoller::Impl::WakeupWaiters() { waiters_->SetSignalAndWakeupOne(); }

FdPoller::Impl::Impl(ev::ThreadControl& control) : control_(control), watcher_(control_, this) {
    watcher_.Init(&IoWatcherCb);
}

FdPoller::Impl::~Impl() { Invalidate(); }

#ifdef __linux__
void FdPoller::Impl::Invoke(uint32_t epoll_events) {
    LOG_TRACE() << "FdPoller::Impl::Invoke called for fd=" << fd_ << " epoll_events=" << epoll_events;
    auto kind_opt = FromEpollEvents(epoll_events);
    if (kind_opt) {
        events_that_happened_.store(*kind_opt, std::memory_order_relaxed);
        WakeupWaiters();
    } else {
        LOG_WARNING() << "FdPoller::Impl::Invoke received unexpected epoll events " << epoll_events << " for fd=" << fd_;
    }
}
#endif

engine::impl::TaskContext::WakeupSource FdPoller::Impl::DoWait(Deadline deadline) {
    UASSERT(IsValid());

    State current_state = state_.load(std::memory_order_acquire);
    if (current_state != State::kReadyToUse) {
        LOG_ERROR() << "FdPoller::Impl::DoWait: Invalid state=" << current_state 
                    << ", fd=" << fd_ << ". Expected state=kReadyToUse";
        UASSERT_MSG(false, fmt::format("Socket misuse: FdPoller.DoWait called in invalid state: '{}', "
                                     "expected '{}'", current_state, State::kReadyToUse));
        return engine::impl::TaskContext::WakeupSource::kNone;
    }

    auto& current = current_task::GetCurrentTaskContext();
    task_processor_ = &current.GetTaskProcessor();

    engine::impl::FutureWaitStrategy wait_strategy{*this, current};
    engine::impl::TaskContext::WakeupSource wakeup_source = engine::impl::TaskContext::WakeupSource::kNone;

#ifdef __linux__
    int epoll_fd = task_processor_->GetCurrentThreadEpollFd();
    if (epoll_fd != -1) {
        // Try using per-thread epoll
        LOG_TRACE() << "FdPoller using epoll for fd=" << fd_ << " on epoll_fd=" << epoll_fd;
        uint32_t epoll_events = ToEpollEvents(kind_);
        if (task_processor_->RegisterEpollCallback(fd_, *this, epoll_events)) {
            using_epoll_ = true;
            SwitchStateToInUse(); // Mark as in use for epoll wait
            try {
                wakeup_source = current.Sleep(wait_strategy, deadline);
            } catch (...) {
                // Ensure deregistration even if Sleep throws
                if (using_epoll_) {
                    task_processor_->DeregisterEpollCallback(fd_);
                    using_epoll_ = false;
                }
                SwitchStateToReadyToUse();
                throw;
            }
            // Deregister after waking up or timeout/cancel
            if (using_epoll_) {
                task_processor_->DeregisterEpollCallback(fd_);
                using_epoll_ = false;
            }
            SwitchStateToReadyToUse();
            return wakeup_source; // Return after epoll attempt
        } else {
            LOG_WARNING() << "Failed to register epoll callback for fd=" << fd_ << ", falling back to ev_watcher";
            // Fallback to ev_watcher if registration fails
        }
    } else {
         LOG_TRACE() << "FdPoller falling back to ev_watcher for fd=" << fd_ << " (no epoll fd)";
         // Fallback if epoll is disabled or unavailable on this thread
    }
#endif // __linux__
    // Fallback or non-Linux: Use ev::Watcher
    LOG_TRACE() << "FdPoller using ev_watcher for fd=" << fd_;
    watcher_.Set(fd_, ToEvEvents(kind_));
    SwitchStateToInUse(); // Mark as in use for ev wait
    try {
        wakeup_source = current.Sleep(wait_strategy, deadline);
    } catch (...) {
        SwitchStateToReadyToUse();
        throw;
    }
    SwitchStateToReadyToUse();
    return wakeup_source;
}

void FdPoller::Impl::Invalidate() {
    if (state_.load(std::memory_order_relaxed) == State::kInvalid) return;

    // Ensure we are not in the middle of a wait
    UASSERT_MSG(state_.load(std::memory_order_relaxed) != State::kInUse,
               "Cannot invalidate FdPoller while it is in use (waiting)");

    StopWatcher(); // Stops both epoll (if active) and ev_watcher

    fd_ = -1;
    // Use exchange with acquire-release to synchronize with potential concurrent Resets
    state_.exchange(State::kInvalid, std::memory_order_acq_rel);
    ResetReady();
    WakeupWaiters();
}

void FdPoller::Impl::StopWatcher() noexcept {
#ifdef __linux__
    if (using_epoll_ && task_processor_) {
        task_processor_->DeregisterEpollCallback(fd_);
        using_epoll_ = false;
    }
#endif
    // Always try to stop ev_watcher, it's safe even if not started or fd is invalid
    watcher_.Stop();
}

void FdPoller::Impl::IoWatcherCb(struct ev_loop*, ev_io* watcher, int) noexcept {
    const auto ev_events = watcher->events;

    UASSERT(watcher->active);
    UASSERT((ev_events & ~(EV_READ | EV_WRITE)) == 0);

    auto* self = static_cast<FdPoller::Impl*>(watcher->data);

    // Cleanup watcher_ first, then awake the coroutine.
    // Otherwise, the coroutine may close watcher_'s fd before watcher_ is stopped.
    const auto guard = self->watcher_.StopWithinEvCallback();

    self->events_that_happened_.store(GetUserMode(ev_events), std::memory_order_relaxed);
    self->WakeupWaiters();
}

bool FdPoller::Impl::IsValid() const noexcept {
    return state_.load(std::memory_order_relaxed) != State::kInvalid;
}

FdPoller::FdPoller(ev::ThreadControl& control) : pimpl_(control) {
    static_assert(std::atomic<State>::is_always_lock_free);
}

FdPoller::~FdPoller() = default;

FdPoller::operator bool() const noexcept { return IsValid(); }

bool FdPoller::IsValid() const noexcept { return pimpl_->IsValid(); }

int FdPoller::GetFd() const noexcept {
    UASSERT(IsValid());
    return pimpl_->fd_;
}

std::optional<FdPoller::Kind> FdPoller::Wait(Deadline deadline) {
    ResetReady();
    if (pimpl_->DoWait(deadline) == engine::impl::TaskContext::WakeupSource::kWaitList) {
        return pimpl_->events_that_happened_.load(std::memory_order_relaxed);
    } else {
        return std::nullopt;
    }
}

std::optional<FdPoller::Kind> FdPoller::GetReady() noexcept {
    if (pimpl_->waiters_->GetAndResetSignal()) {
        return pimpl_->events_that_happened_.load(std::memory_order_relaxed);
    } else {
        return std::nullopt;
    }
}

engine::impl::ContextAccessor* FdPoller::TryGetContextAccessor() noexcept { return &*pimpl_; }

void FdPoller::Reset(int fd, Kind kind) { pimpl_->Reset(fd, kind); }

void FdPoller::Invalidate() { pimpl_->Invalidate(); }

void FdPoller::WakeupWaiters() { pimpl_->WakeupWaiters(); }

void FdPoller::Impl::SwitchStateToInUse() {
    State expected_state = State::kReadyToUse;
    if (!state_.compare_exchange_strong(expected_state, State::kInUse, 
                                      std::memory_order_acq_rel)) {
        LOG_ERROR() << "FdPoller::Impl::SwitchStateToInUse: Invalid state transition. "
                    << "Expected state=kReadyToUse, actual state=" << expected_state;
        UASSERT_MSG(false, fmt::format("Socket misuse: expected socket state is '{}', "
                                      "actual state is '{}'", 
                                      State::kReadyToUse, expected_state));
    }
}

void FdPoller::Impl::SwitchStateToReadyToUse() {
    State expected_state = State::kInUse;
    if (!state_.compare_exchange_strong(expected_state, State::kReadyToUse, 
                                      std::memory_order_acq_rel)) {
        LOG_ERROR() << "FdPoller::Impl::SwitchStateToReadyToUse: Invalid state transition. "
                    << "Expected state=kInUse, actual state=" << expected_state;
        UASSERT_MSG(false, fmt::format("Socket misuse: expected socket state is '{}', "
                                     "actual state is '{}'", 
                                     State::kInUse, expected_state));
    }
}

void FdPoller::Impl::Reset(int fd, Kind kind) {
    UASSERT(fd >= 0);
    State old_state = state_.exchange(State::kInvalid, std::memory_order_acq_rel);

    UASSERT_MSG(old_state != State::kInUse, "Cannot reset FdPoller while it is in use (waiting)");

    if (old_state != State::kInvalid) {
        StopWatcher();
    }

    fd_ = fd;
    kind_ = kind;
    // Use store with release semantics to make fd_ and kind_ visible before state change
    state_.store(State::kReadyToUse, std::memory_order_release);
    ResetReady();
}

void FdPoller::ResetReady() noexcept { pimpl_->ResetReady(); }

void FdPoller::SwitchStateToInUse() { pimpl_->SwitchStateToInUse(); }
void FdPoller::SwitchStateToReadyToUse() { pimpl_->SwitchStateToReadyToUse(); }

}  // namespace engine::io

USERVER_NAMESPACE_END
