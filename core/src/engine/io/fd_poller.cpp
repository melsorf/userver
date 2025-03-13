#include <userver/engine/io/fd_poller.hpp>

#include <engine/ev/watcher.hpp>
#include <engine/impl/future_utils.hpp>
#include <engine/impl/wait_list_light.hpp>
#include <engine/task/task_context.hpp>
#ifdef __linux__
#include <sys/epoll.h>
#include <engine/task/task_processor.hpp>
#include <userver/engine/task/task.hpp>
#endif

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

struct CallbackState {
    std::atomic<FdPoller::Kind> events_that_happened_{};
    std::function<void()> wakeup_function;
    
    explicit CallbackState(std::function<void()> wakeup) 
        : wakeup_function(std::move(wakeup)) {}
};

int GetEvMode(FdPoller::Kind kind) {
    switch (kind) {
        case FdPoller::Kind::kRead:
            return EV_READ;
        case FdPoller::Kind::kWrite:
            return EV_WRITE;
        case FdPoller::Kind::kReadWrite:
            return EV_READ | EV_WRITE;

        default:
            UINVARIANT(false, "Invalid kind: " + std::to_string(static_cast<int>(kind)));
    }
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

#ifdef __linux__
uint32_t KindToEpollEvents(FdPoller::Kind kind) {
    switch (kind) {
        case FdPoller::Kind::kRead:
            return EPOLLIN;
        case FdPoller::Kind::kWrite:
            return EPOLLOUT;
        case FdPoller::Kind::kReadWrite:
            return EPOLLIN | EPOLLOUT;
        default:
            UINVARIANT(false, "Invalid kind: " + std::to_string(static_cast<int>(kind)));
    }
}
#endif

}  // namespace

struct FdPoller::Impl final : public engine::impl::ContextAccessor {
    Impl(ev::ThreadControl control);

    ~Impl();

    engine::impl::TaskContext::WakeupSource DoWait(Deadline deadline);

    bool IsValid() const noexcept;

    void Invalidate();
    void Reset(int fd, Kind kind, bool register_epollet = true);

    void StopWatcher() noexcept;

    static void IoWatcherCb(struct ev_loop*, ev_io*, int) noexcept;
    void WakeupWaiters();

    void ResetReady() noexcept { waiters_->GetAndResetSignal(); }

    // ContextAccessor implementation
    bool IsReady() const noexcept override { return waiters_->IsSignaled(); }

    engine::impl::EarlyWakeup TryAppendWaiter(engine::impl::TaskContext& waiter) override {
        if (waiters_->GetSignalOrAppend(&waiter)) {
            return engine::impl::EarlyWakeup{true};
        }
#ifdef __linux__
        if (!use_epoll_) {
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
        if (!use_epoll_) {
            watcher_.StopAsync();
        }
#else
        watcher_.StopAsync();
#endif
    }

    void AfterWait() noexcept override {
#ifdef __linux__
        if (!use_epoll_) {
            watcher_.Stop();
        }
#else
        watcher_.Stop();
 #endif
    } 

    void RethrowErrorResult() const override {}

    std::atomic<FdPoller::State> state_{FdPoller::State::kInvalid};
    engine::impl::FastPimplWaitListLight waiters_;
    ev::Watcher<ev_io> watcher_;
    std::atomic<FdPoller::Kind> events_that_happened_{};
#ifdef __linux__
    bool use_epoll_{false};
    int fd_{-1};
    std::optional<std::size_t> registered_fd_index_;
    engine::TaskProcessor* task_processor_{nullptr};
#endif
};

void FdPoller::Impl::WakeupWaiters() { waiters_->SetSignalAndWakeupOne(); }

FdPoller::Impl::Impl(ev::ThreadControl control) : watcher_(control, this) { watcher_.Init(&IoWatcherCb); }

FdPoller::Impl::~Impl() {
#ifdef __linux__
    if (use_epoll_ && fd_ >= 0) {
        try {
            if (task_processor_ && registered_fd_index_) {
                task_processor_->UnregisterFileDescriptor(fd_);
            }
        } catch (...) {
            // Destructors shouldn't throw
        }
    }
#endif
}

engine::impl::TaskContext::WakeupSource FdPoller::Impl::DoWait(Deadline deadline) {
    UASSERT(IsValid());

    auto& current = current_task::GetCurrentTaskContext();

    engine::impl::FutureWaitStrategy wait_strategy{*this, current};
    auto ret = current.Sleep(wait_strategy, deadline);

    /*
     * Manually call Stop() here to be sure that after DoWait() no waiter_'s
     * callback (IoWatcherCb) is running.
     */
#ifdef __linux__
    if (!use_epoll_) {
        watcher_.Stop();
    }
#else
    watcher_.Stop();
#endif
    return ret;
}

void FdPoller::Impl::Invalidate() {
#ifdef __linux__
    if (use_epoll_ && task_processor_ && registered_fd_index_ && fd_ >= 0) {
        try {
            task_processor_->UnregisterFileDescriptor(fd_);
        } catch (const std::exception& ex) {
           // ignore
        }
        registered_fd_index_.reset();
        use_epoll_ = false;
        task_processor_ = nullptr;
        fd_ = -1;
    } else {
        StopWatcher();
    }
#else
    StopWatcher();
#endif
    auto old_state = State::kReadyToUse;
    const auto res = state_.compare_exchange_strong(old_state, State::kInvalid);

    UINVARIANT(
        res,
        fmt::format("Socket misuse: expected socket state is '{}', actual state is '{}'", State::kReadyToUse, old_state)
    );
}

void FdPoller::Impl::StopWatcher() noexcept {
    UASSERT(IsValid());
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

bool FdPoller::Impl::IsValid() const noexcept { return state_ != State::kInvalid; }

FdPoller::FdPoller(const ev::ThreadControl& control) : pimpl_(control) {
    static_assert(std::atomic<State>::is_always_lock_free);
}

FdPoller::~FdPoller() = default;

FdPoller::operator bool() const noexcept { return IsValid(); }

bool FdPoller::IsValid() const noexcept { return pimpl_->IsValid(); }

int FdPoller::GetFd() const noexcept { 
#ifdef __linux__
    if (pimpl_->use_epoll_)
        return pimpl_->fd_;
#endif
    return pimpl_->watcher_.GetFd(); 
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

void FdPoller::Reset(int fd, Kind kind, bool register_epollet) { pimpl_->Reset(fd, kind, register_epollet); }

void FdPoller::Invalidate() { pimpl_->Invalidate(); }

void FdPoller::WakeupWaiters() { pimpl_->WakeupWaiters(); }

void FdPoller::SwitchStateToInUse() {
    auto old_state = State::kReadyToUse;
    const auto res = pimpl_->state_.compare_exchange_strong(old_state, State::kInUse);

    UASSERT_MSG(
        res,
        fmt::format("Socket misuse: expected socket state is '{}', actual state is '{}'", State::kReadyToUse, old_state)
    );
}

void FdPoller::SwitchStateToReadyToUse() {
    auto old_state = State::kInUse;
    const auto res = pimpl_->state_.compare_exchange_strong(old_state, State::kReadyToUse);
    UASSERT_MSG(
        res, fmt::format("Socket misuse: expected socket state is '{}', actual state is '{}'", State::kInUse, old_state)
    );
}

void FdPoller::Impl::Reset(int fd, Kind kind, bool register_epollet /*= true*/) {
    UINVARIANT(fd >= 0, "FdPoller::Reset: fd is -1");
    UASSERT(!IsValid());

#ifdef __linux__
    if (register_epollet) {
        auto* current_processor = engine::current_task::GetTaskProcessorUnchecked();
        if (current_processor) {
            uint32_t epoll_events = KindToEpollEvents(kind);
            auto callback = [this](uint32_t events) {
                uint32_t effective = 0;
                if (events & EPOLLIN)
                    effective |= EV_READ;
                if (events & EPOLLOUT)
                    effective |= EV_WRITE;
                const auto userver_kind = GetUserMode(effective);
                events_that_happened_.store(userver_kind, std::memory_order_relaxed);
                WakeupWaiters();
            };
            auto reg_index = current_processor->RegisterFileDescriptor(fd, epoll_events, std::move(callback));
            if (reg_index.has_value()) {
                fd_ = fd;
                registered_fd_index_ = reg_index;
                use_epoll_ = true;
                task_processor_ = current_processor;
                state_ = State::kReadyToUse;
                return;
            }
        }
    }
#endif
    // Fallback to watcher_
    watcher_.Set(fd, GetEvMode(kind));
#ifdef __linux__
    use_epoll_ = false;
    fd_ = -1;
#endif
    state_ = State::kReadyToUse;
}

void FdPoller::ResetReady() noexcept { pimpl_->ResetReady(); }

}  // namespace engine::io

USERVER_NAMESPACE_END