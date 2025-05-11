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
            return EPOLLIN | EPOLLPRI | EPOLLRDHUP | EPOLLET;
        case FdPoller::Kind::kWrite:
            return EPOLLOUT | EPOLLET;
        case FdPoller::Kind::kReadWrite:
            return EPOLLIN | EPOLLOUT | EPOLLPRI | EPOLLRDHUP | EPOLLET;
    }
    UINVARIANT(false, "Invalid kind: " + std::to_string(static_cast<int>(kind)));
}
#endif

}  // namespace

struct FdPoller::Impl final : public engine::impl::ContextAccessor
{
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

#ifdef __linux__
    std::weak_ptr<void> GetWeakReference();
#endif

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
    bool use_epoll_requested_{true}; // By default, try to use epoll when available
    int fd_{-1};
    std::optional<std::size_t> registered_fd_index_;
    engine::TaskProcessor* task_processor_{nullptr};
    std::mutex epoll_mutex_; 

    std::shared_ptr<void> self_reference_;
#endif
};

void FdPoller::Impl::WakeupWaiters() { waiters_->SetSignalAndWakeupOne(); }

FdPoller::Impl::Impl(ev::ThreadControl control) : watcher_(control, this) { watcher_.Init(&IoWatcherCb); }

FdPoller::Impl::~Impl() {
#ifdef __linux__
    int fd_to_unregister = -1;
    engine::TaskProcessor* processor = nullptr;
    
    {
        std::lock_guard<std::mutex> lock(epoll_mutex_);
        if (use_epoll_ && fd_ >= 0 && task_processor_ && registered_fd_index_) {
            fd_to_unregister = fd_;
            processor = task_processor_;
            registered_fd_index_.reset();
        }
    }
    
    if (fd_to_unregister >= 0 && processor) {
        try {
            processor->UnregisterFd(fd_to_unregister);
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
    const auto current_state = state_.load(std::memory_order_acquire);
    if (current_state == FdPoller::State::kInvalid) {
        return;
    }
    UASSERT(current_state == FdPoller::State::kReadyToUse);

#ifdef __linux__
    engine::TaskProcessor* tp_to_unregister_from = nullptr;
    int fd_to_unregister_val = -1;

    {
        std::lock_guard<std::mutex> lock(epoll_mutex_);
        if (use_epoll_ && fd_ >= 0 && task_processor_ && registered_fd_index_) {
            tp_to_unregister_from = task_processor_;
            fd_to_unregister_val = fd_;
        }
        use_epoll_ = false;
        task_processor_ = nullptr;
        registered_fd_index_.reset();
        fd_ = -1;
    }

    if (tp_to_unregister_from && fd_to_unregister_val >= 0) {
        try {
            tp_to_unregister_from->UnregisterFd(fd_to_unregister_val);
        } catch (const std::exception& e) {
            LOG_WARNING() << "FdPoller: Exception during UnregisterFd in Invalidate for fd "
                          << fd_to_unregister_val << ": " << e.what();
        }
    }
#endif
    watcher_.Stop();
    state_.store(FdPoller::State::kInvalid, std::memory_order_release);
    waiters_->SetSignalAndWakeupOne();
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

    self->events_that_happened_.store(GetUserMode(ev_events), std::memory_order_release);
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
        if (engine::current_task::IsCancelRequested()) {
            return std::nullopt;
        }
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

#ifdef __linux__
void FdPoller::SetEpollMode(bool use_epoll) {
    pimpl_->use_epoll_requested_ = use_epoll;
}
#endif

#ifdef __linux__
std::weak_ptr<void> FdPoller::Impl::GetWeakReference() {
    if (!self_reference_) {
        self_reference_ = std::shared_ptr<void>(this, [](void*){/*already managed by FastPimpl*/});
    }
    return self_reference_;
}
#endif

void FdPoller::Impl::Reset(int fd_arg, Kind kind, bool register_epollet_hint) {
    Invalidate(); // Clean up previous state, including unregistering from epoll if any

    state_.store(State::kInvalid, std::memory_order_release); 

#ifdef __linux__
    bool registered_with_tp_epoll = false;
    if (register_epollet_hint && use_epoll_requested_ && engine::current_task::IsTaskProcessorThread()) {
        try {
            engine::TaskProcessor& current_tp = engine::current_task::GetTaskProcessor();
            if (current_tp.IsEpollModeEnabled()) {
                uint32_t epoll_events = KindToEpollEvents(kind);

                auto epoll_callback = [this_impl = this](uint32_t received_events) {
                    FdPoller::Kind userver_kind = FdPoller::Kind::kRead; // Default
                    if (received_events & (EPOLLHUP | EPOLLERR)) {
                        userver_kind = FdPoller::Kind::kReadWrite;
                    } else if ((received_events & EPOLLIN) && (received_events & EPOLLOUT)) {
                        userver_kind = FdPoller::Kind::kReadWrite;
                    } else if (received_events & EPOLLIN) {
                        userver_kind = FdPoller::Kind::kRead;
                    } else if (received_events & EPOLLOUT) {
                        userver_kind = FdPoller::Kind::kWrite;
                    } else {
                        return; // No relevant event
                    }
                    this_impl->events_that_happened_.store(userver_kind, std::memory_order_release);
                    this_impl->WakeupWaiters();
                };

                if (!self_reference_) {
                    self_reference_ = std::make_shared<char>(); // Dummy object
                }
                std::weak_ptr<void> owner_ref = self_reference_;

                std::size_t reg_idx = current_tp.RegisterFd(fd_arg, epoll_events, std::move(epoll_callback), owner_ref);

                if (reg_idx != std::numeric_limits<std::size_t>::max()) {
                    std::lock_guard<std::mutex> lock(epoll_mutex_);
                    fd_ = fd_arg;
                    task_processor_ = &current_tp;
                    use_epoll_ = true;
                    registered_fd_index_ = reg_idx;
                    registered_with_tp_epoll = true;
                } else {
                    LOG_DEBUG() << "FdPoller: Failed to register fd " << fd_arg << " with TaskProcessor epoll. Falling back to libev.";
                }
            }
        } catch (const std::exception& e) {
            LOG_WARNING() << "FdPoller: Exception while trying to get TaskProcessor for epoll: " << e.what() << ". Falling back to libev.";
        }
    }

    if (registered_with_tp_epoll) {
        // Successfully registered with TaskProcessor's epoll, libev watcher remains stopped.
        watcher_.Stop(); // Ensure libev watcher is stopped if it was somehow active
    } else {
        // Fallback to libev watcher
        std::lock_guard<std::mutex> lock(epoll_mutex_); // Ensure epoll state is clear for libev mode
        use_epoll_ = false;
        task_processor_ = nullptr;
        registered_fd_index_.reset();
        fd_ = -1;

        watcher_.Set(fd_arg, GetEvMode(kind));
        // watcher_.StartAsync() will be called by TryAppendWaiter if needed
    }
#else // NOT __linux__ - always use libev watcher
    watcher_.Set(fd_arg, GetEvMode(kind));
    // watcher_.StartAsync() will be called by TryAppendWaiter if needed
#endif
    state_.store(State::kReadyToUse, std::memory_order_release);
}

void FdPoller::ResetReady() noexcept { pimpl_->ResetReady(); }

}  // namespace engine::io

USERVER_NAMESPACE_END