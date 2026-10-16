/*
 * Copyright 2026 Seastar contributors
 */

//
// rdma-device.hh: per-shard RDMA global state.
//
// Port of brpc's rdma_helper globals + QP/CQ resource pool: one device
// context and PD per shard, a pre-created pool of QP/CQ sets which
// connections rent and recycle, the shared completion channel (event
// mode) or the busy-poll registry (poll mode), and the registered
// buffer pool.
//

#pragma once

#ifdef SEASTAR_HAVE_RDMA

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/internal/poll.hh>
#include <seastar/net/rdma-stack.hh>

#include "rdma-buffer-pool.hh"
#include "rdma-verbs.hh"

namespace seastar::net::rdma {

class rdma_endpoint;

// One QP with its completion resources, as rented from the pool.
struct qp_resources {
    qp_h qp = nullptr;
    cq_h send_cq = nullptr;
    cq_h recv_cq = nullptr;     // event mode: separate from send_cq
    cq_h polling_cq = nullptr;  // poll mode: single CQ for both directions
};

class rdma_device {
public:
    explicit rdma_device(const rdma_stack_options& opts);
    ~rdma_device();
    rdma_device(const rdma_device&) = delete;
    rdma_device& operator=(const rdma_device&) = delete;

    // Shard 0 selects the device/port/GID once and the choice is
    // broadcast via smp; every shard then opens its own context (a
    // verbs context must be used from the thread that created it).
    future<> init_shard(const gid_selection& sel);

    // Select device+GID (called on shard 0 only).
    static future<gid_selection> select_device(const rdma_stack_options& opts);

    qp_resources acquire_qp();
    void release_qp(qp_resources&& r);

    // Create a fresh QP/CQ set (used by the pool and on-demand growth).
    qp_resources acquire_resources_uncached();

    rdma_buffer_pool& pool() noexcept { return _pool; }
    // PD for on-the-fly registrations (direct-send path).
    pd_h pd() const noexcept { return _pd; }
    const gid_selection& selection() const noexcept { return _sel; }
    device_caps caps() const noexcept { return _caps; }
    unsigned qp_depth() const noexcept { return _qp_depth; }
    // Receive block size advertised in the handshake: the configured
    // --rdma-block-size snapped down to the largest pool class not
    // exceeding it (8K/64K/2M). Receive rings borrow blocks of this
    // size, so peers may send whole blocks per WR.
    size_t recv_block_size() const noexcept { return _recv_block_size; }
    unsigned cqe_poll_batch() const noexcept { return _cqe_poll_batch; }
    // Minimum payload for the direct-send (zero-copy) path; mirrors
    // --rdma-zcopy-min-size.
    size_t direct_min_size() const noexcept { return _direct_min_size; }
    bool polling_mode() const noexcept { return _polling; }

    // Event mode: the shared completion channel. All pooled CQs are
    // created against it.
    comp_channel_h comp_channel() const noexcept { return _channel; }

    // Completion dispatch. `ep` is called (on its home shard) whenever
    // a CQE may be pending on either of its CQs. Both modes converge
    // here; event mode via the channel watcher, poll mode via the
    // reactor poller.
    void poll_endpoint(rdma_endpoint* ep);
    void register_endpoint(rdma_endpoint* ep);
    void unregister_endpoint(rdma_endpoint* ep);

    // Stop the event-mode channel watcher and wait for it to finish.
    // Must be called on the device's shard while the reactor can still
    // run tasks (e.g. from an exit task). Idempotent.
    future<> stop();
    
    // reactor::poller hook (poll mode only).
    bool poll_all();

private:
    struct pooled_qp {
        qp_resources res;
        bool in_use = false;
    };

    const bool _polling;
    const unsigned _qp_depth;
    const size_t _recv_block_size;
    unsigned _max_sge; // filled by init_shard() from device caps
    const unsigned _cqe_poll_batch;
    const unsigned _qp_pool_size;
    const size_t _direct_min_size;
    const unsigned _max_regions;

    gid_selection _sel{};
    device_caps _caps{};
    context_h _ctx = nullptr;
    pd_h _pd = nullptr;
    comp_channel_h _channel = nullptr;
    rdma_buffer_pool _pool;
    std::vector<pooled_qp> _qp_pool;
    std::vector<rdma_endpoint*> _poll_set;

    // Event mode: fd of _channel wrapped for reactor readiness waits.
    std::optional<pollable_fd> _channel_fd;
    std::optional<internal::poller> _rx_poller; // poll mode
    future<> _watcher_done = make_ready_future<>();
    bool _stopping = false;

    void start_channel_watch();
    future<> channel_watch_loop();
    bool _watcher_stopped = false;
};

} // namespace seastar::net::rdma

#endif // SEASTAR_HAVE_RDMA
