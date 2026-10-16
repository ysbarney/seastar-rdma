/*
 * Copyright 2026 Seastar contributors
 */

#include "rdma-device.hh"

#ifdef SEASTAR_HAVE_RDMA

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <system_error>

#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/util/log.hh>

#include "rdma-endpoint.hh"

namespace seastar::net::rdma {

static logger dev_log("rdma-dev");

// Snap a configured --rdma-block-size down to the largest pool class
// that does not exceed it (never below the 8K default class). Receive
// blocks must be pool-resident so their lkey is derivable from the
// address.
static size_t snap_recv_block_size(unsigned requested) {
    size_t r = std::max<size_t>(rdma_buffer_pool::class_sizes[0], requested);
    for (size_t i = rdma_buffer_pool::num_classes; i > 0; --i) {
        if (r >= rdma_buffer_pool::class_sizes[i - 1]) {
            return rdma_buffer_pool::class_sizes[i - 1];
        }
    }
    return rdma_buffer_pool::class_sizes[0];
}

rdma_device::rdma_device(const rdma_stack_options& opts)
    : _polling(opts.polling.get_value())
    , _qp_depth(std::max<unsigned>(min_qp_size, opts.qp_depth.get_value()))
    , _recv_block_size(snap_recv_block_size(opts.block_size.get_value()))
    , _max_sge(0)
    , _cqe_poll_batch(std::max(1u, opts.cqe_poll_batch.get_value()))
    , _qp_pool_size(std::max(1u, opts.qp_pool_size.get_value()))
    , _direct_min_size(std::max<size_t>(1, opts.zcopy_min_size.get_value()))
    , _max_regions(std::max(1u, opts.max_regions.get_value())) {
    // _max_sge is filled in init_shard() from device caps.
 }

rdma_device::~rdma_device() {
    _stopping = true;
    const verbs* v = verbs_provider();
    for (auto& p : _qp_pool) {
        if (p.res.qp) {
            v->destroy_qp(p.res.qp);
        }
        if (p.res.send_cq) {
            v->destroy_cq(p.res.send_cq);
        }
        if (p.res.recv_cq) {
            v->destroy_cq(p.res.recv_cq);
        }
        if (p.res.polling_cq) {
            v->destroy_cq(p.res.polling_cq);
        }
    }
    if (_channel) {
        v->destroy_comp_channel(_channel);
    }
    if (_pd) {
        v->dealloc_pd(_pd);
    }
    if (_ctx) {
        v->close_device(_ctx);
    }
}

future<gid_selection> rdma_device::select_device(const rdma_stack_options& opts) {
    gid_selection sel;
    device_caps caps;
    auto dev = opts.device.get_value();
    auto gid_idx = opts.gid_index.get_value();
    const char* dev_name = dev.empty() ? nullptr : dev.c_str();
    uint8_t port = uint8_t(opts.port.get_value());
    // Honor an explicit GID index by re-scanning after selection.
    int forced_gid = -1;
    if (!gid_idx.empty()) {
        try {
            forced_gid = std::stoi(gid_idx);
        } catch (...) {
            return make_exception_future<gid_selection>(
                std::runtime_error("invalid --network-stack-rdma-gid-index"));
        }
    }
    const verbs* v = verbs_provider();
    if (!v->enumerate_devices(dev_name, port, sel, caps)) {
        return make_exception_future<gid_selection>(
            std::system_error(errno ? errno : ENODEV, std::system_category(),
                              "rdma: no RDMA device available"));
    }
    if (forced_gid >= 0) {
        sel.gid_index = forced_gid; // validated by open path later
    }
    return make_ready_future<gid_selection>(std::move(sel));
}

future<> rdma_device::init_shard(const gid_selection& sel) {
    const verbs* v = verbs_provider();
    _sel = sel;

    _ctx = v->open_device(sel);
    if (!_ctx) {
        throw std::system_error(errno, std::system_category(), "rdma: ibv_open_device failed");
    }
    _pd = v->alloc_pd(_ctx);
    if (!_pd) {
        throw std::system_error(errno, std::system_category(), "rdma: ibv_alloc_pd failed");
    }

    // Device caps (used for max_sge clamping).
    _caps = device_caps{};
    gid_selection tmp;
    // Re-enumerate just to fill caps; cheap and avoids a second verbs
    // entry point for query_device.
    if (v->enumerate_devices(sel.device_name.c_str(), sel.port, tmp, _caps)) {
        _max_sge = std::max(1, _caps.max_sge);
    } else {
        _max_sge = 16;
    }

    _pool.init(_pd, _max_regions);

    if (!_polling) {
        _channel = v->create_comp_channel(_ctx);
        if (!_channel) {
            throw std::system_error(errno, std::system_category(),
                                    "rdma: ibv_create_comp_channel failed");
        }
    }

    // Pre-create the QP pool.
    for (unsigned i = 0; i < _qp_pool_size; ++i) {
        pooled_qp p;
        p.res = acquire_resources_uncached();
        _qp_pool.push_back(std::move(p));
    }

    if (!_polling) {
        start_channel_watch();
    } else {
        // Busy-poll mode: hook into the reactor loop.
        _rx_poller = reactor::poller::simple([this] { return poll_all(); });
    }

    return make_ready_future<>();
}

qp_resources rdma_device::acquire_resources_uncached() {
    const verbs* v = verbs_provider();
    qp_resources r;
    unsigned cqe = 2 * _qp_depth;
    if (_polling) {
        r.polling_cq = v->create_cq(_ctx, cqe, nullptr, 0);
        if (!r.polling_cq) {
            throw std::system_error(errno, std::system_category(), "rdma: create_cq failed");
        }
        qp_init_desc d{r.polling_cq, r.polling_cq, _qp_depth, _qp_depth, unsigned(_max_sge)};
        r.qp = v->create_qp(_pd, d);
    } else {
        r.send_cq = v->create_cq(_ctx, cqe, _channel, 0);
        r.recv_cq = v->create_cq(_ctx, cqe, _channel, 0);
        if (!r.send_cq || !r.recv_cq) {
            throw std::system_error(errno, std::system_category(), "rdma: create_cq failed");
        }
        // Arm both CQs now: a CQ must be armed *before* the first
        // completion arrives, or that completion never raises a channel
        // event (brpc arms in BringUpQpCq the same way).
        v->req_notify_cq(r.send_cq, false);
        v->req_notify_cq(r.recv_cq, false);
        qp_init_desc d{r.send_cq, r.recv_cq, _qp_depth, _qp_depth, unsigned(_max_sge)};
        r.qp = v->create_qp(_pd, d);
    }
    if (!r.qp) {
        throw std::system_error(errno, std::system_category(), "rdma: ibv_create_qp failed");
    }
    return r;
}

qp_resources rdma_device::acquire_qp() {
    for (auto& p : _qp_pool) {
        if (!p.in_use) {
            p.in_use = true;
            return p.res;
        }
    }
    // Pool exhausted: create a fresh set (pool grows).
    qp_resources r = acquire_resources_uncached();
    _qp_pool.push_back(pooled_qp{r, true});
    return r;
}

t verbs* v = verbs_provider();
    // Transition the QP back to RESET and drain its CQs so the next
    // renter starts clean (same dance as brpc DeallocateResources).
    qp_attr_desc d{};
    d.qp_state = IBV_QPS_RESET;
    v->modify_qp(r.qp, d, QP_STATE);

    std::vector<ibv_wc> wc(_cqe_poll_batch);
    if (!_polling) {
        while (v->poll_cq(r.recv_cq, int(_cqe_poll_batch), wc.data()) > 0) {}
        while (v->poll_cq(r.send_cq, int(_cqe_poll_batch), wc.data()) > 0) {}
        // Re-arm for the next renter.
        v->req_notify_cq(r.send_cq, false);
        v->req_notify_cq(r.recv_cq, false);
    } else {
        while (v->poll_cq(r.polling_cq, int(_cqe_poll_batch), wc.data()) > 0) {}
    }

    for (auto& p : _qp_pool) {
        if (p.in_use && p.res.qp == r.qp) {
            p.in_use = false;
            return;
        }
    }
    // Not from the pool (grown dynamically): destroy.
    v->destroy_qp(r.qp);
    if (r.send_cq) v->destroy_cq(r.send_cq);
    if (r.recv_cq) v->destroy_cq(r.recv_cq);
    if (r.polling_cq) v->destroy_cq(r.polling_cq);
}

void rdma_device::poll_endpoint(rdma_endpoint* ep) {
    // Dispatch is implemented by the endpoint itself (it owns its CQs).
    ep->poll_cq_and_dispatch();
}

void rdma_device::register_endpoint(rdma_endpoint* ep) {
    dev_log.debug("register_endpoint: ep={} poll_set={}",
                  static_cast<void*>(ep), _poll_set.size());
    // The registry is used in both modes: poll mode iterates it from
    // the reactor poller, event mode looks up the endpoint owning a
    // CQ when a completion-channel event arrives.
    _poll_set.push_back(ep);
}

void rdma_device::unregister_endpoint(rdma_endpoint* ep) {
    auto it = std::find(_poll_set.begin(), _poll_set.end(), ep);
    if (it != _poll_set.end()) {
        _poll_set.erase(it);
    }
}

bool rdma_device::poll_all() {
    bool did_work = false;
    for (auto* ep : _poll_set) {
        did_work |= ep->poll_cq_and_dispatch();
    }
    return did_work;
}

void rdma_device::start_channel_watch() {
    const verbs* v = verbs_provider();
    int fd = v->get_comp_channel_fd(_channel);
    // O_NONBLOCK is required for pollable_fd-based readiness waits.
    ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
    ::fcntl(fd, F_SETFD, FD_CLOEXEC);
    // file_desc owns (closes) the fd; the channel is destroyed via
    // verbs by the device destructor, so dup to give the pollable_fd
    // its own handle.
    int dup_fd = ::dup(fd);
    if (dup_fd == -1) {
        throw std::system_error(errno, std::system_category(), "dup(comp_channel)");
    }
    _channel_fd = pollable_fd(file_desc::from_fd(dup_fd));
    _watcher_done = channel_watch_loop();
}

future<> rdma_device::stop() {
    if (_watcher_stopped) {
        return make_ready_future<>();
    }
    _watcher_stopped = true;
    _stopping = true;
    if (_polling || !_channel_fd) {
        return make_ready_future<>();
    }
    // Best-effort wake for a watcher parked on readable(): eventfd-
    // backed channels (the test mock) accept a write. Real ibverbs
    // completion channels are read-only and the write fails; the
    // bounded wait below then abandons the watcher instead.
    // The wake bytes are never consumed: the loop checks _stopping
    // right after the wait returns and breaks before draining events.
    // The single-shard watcher has no suspension point inside the
    // drain loop, so it is always parked here (or already done).
    uint64_t one = 1;
    ssize_t r = ::write(_channel_fd->get_fd(), &one, sizeof(one));
    if (r == -1 && errno != EAGAIN) {
        dev_log.debug("rdma: comp channel wake failed: {}", errno);
    }
    // Bounded wait: an unwakeable watcher (real HW) would never
    // finish, so after a grace period (covering a watcher that is
    // merely draining its last events) it is abandoned. The abandoned
    // coroutine never resumes and so cannot touch the destroyed
    // device; its frame is reclaimed only at process exit.
    return with_timeout(std::chrono::steady_clock::now() + std::chrono::milliseconds(500),
                        std::move(_watcher_done))
        .handle_exception([] (std::exception_ptr) {
            dev_log.info("rdma: channel watcher abandoned at shutdown");
            return make_ready_future<>();
        });
}

future<> rdma_device::channel_watch_loop() {
    // Wait for channel readability, then drain cq events. For every
    // event: ack, dispatch the owning endpoint(s), THEN re-arm the CQ.
    // Re-arming before dispatch would fire an endless stream of events
    // for a CQ whose completions nobody drains (e.g. its endpoint just
    // died -- req_notify on a CQ with pending CQEs generates an event
    // immediately).
    while (!_stopping) {
        co_await _channel_fd->readable();
        if (_stopping) {
            break;
        }
        const verbs* v = verbs_provider();
        cq_h cq = nullptr;
        while (v->get_cq_event(_channel, cq) == 0) {
            dev_log.debug("cq event: cq={} poll_set={}", static_cast<void*>(cq), _poll_set.size());
            v->ack_cq_events(cq, 1);
            // Dispatch the endpoint(s) owning this CQ. Snapshot first:
            // dispatch may fail an endpoint, which unregisters it from
            // _poll_set (vector erase) mid-loop.
            std::vector<rdma_endpoint*> owners;
            for (auto* ep : _poll_set) {
                if (ep->owns_cq(cq)) {
                    owners.push_back(ep);
                }
            }
            for (auto* ep : owners) {
                // A dispatch must never take the shard's watcher down:
                // one broken endpoint (e.g. a borrow that ran out of
                // regions) fails itself; the others keep their
                // completions processed.
                try {
                    ep->poll_cq_and_dispatch();
                } catch (...) {
                    ep->fail(ENOMEM);
                }
            }
            if (owners.empty()) {
                // The endpoint died between the event being queued and
                // now: drain the CQ here or the re-arm below fires
                // forever.
                std::vector<ibv_wc> wcs(_cqe_poll_batch);
                while (v->poll_cq(cq, int(_cqe_poll_batch), wcs.data()) > 0) {}
            }
            v->req_notify_cq(cq, false);
        }
        // (get_cq_event returned EAGAIN)
    }
}

} // namespace seastar::net::rdma

#endif // SEASTAR_HAVE_RDMA
