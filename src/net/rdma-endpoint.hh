/*
 * Copyright 2026 Seastar contributors
 */

//
// rdma-endpoint.hh: one RDMA connection.
//
// Port of brpc's RdmaEndpoint (src/brpc/rdma/rdma_endpoint.cpp)
// // restructured for seastar's pull-based streams:
//
//   - handshake runs as a coroutine over the TCP pollable_fd (brpc
//     needs an explicit non-blocking state machine; we can co_await);
//   - instead of pushing messages into an InputMessenger, received
//     payloads land in a staging deque drained by data_source::get();
//   - instead of the socket write loop re-driving CutFromIOBufList
//     after EAGAIN, put() keeps its own backlog and suspends on a
//     window promise until send completions free SQ slots.
//
// Flow control, SGE cutting, SIGNALED/SOLICITED suppression and the
// poll->notify->re-poll CQ dance are copied faithfully from brpc.
//

#pragma once

#ifdef SEASTAR_HAVE_RDMA

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>

#include <infiniband/verbs.h>

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/net/socket_defs.hh>

#include "rdma-device.hh"
#include "rdma-handshake.hh"

namespace seastar::net::rdma {

class rdma_endpoint : public enable_lw_shared_from_this<rdma_endpoint>
                   , public weakly_referencable<rdma_endpoint> {
public:
    // Client-side handshake: TCP is already connected. On decline from
    // the server, returns std::nullopt, hands the fd back via
    // \p tcp_fd and the pre-read bytes via \p prefix — the caller
    // continues over plain TCP on the same connection.
    static future<std::optional<lw_shared_ptr<rdma_endpoint>>> connect(
        rdma_device* dev, pollable_fd& tcp_fd, socket_address remote,
        lowres_clock::duration timeout, std::vector<char>& prefix);

    // Server-side handshake: `client_hello` holds the already-read
    // hello bytes (magic + body). Returns nullptr on client decline.
    static future<lw_shared_ptr<rdma_endpoint>> accept(
        rdma_device* dev, pollable_fd tcp_fd, socket_address remote,
        const char* client_hello, lowres_clock::duration timeout);

    ~rdma_endpoint();

    // ---- data plane (single shard: the endpoint's home) --------------
    // Takes ownership of the buffers' contents synchronously (copies
    // into pool blocks). The returned future resolves when all bytes
    // are *posted* (not necessarily completed); completion is waited on
    // by shutdown_output()'s full drain, not by flush().
    future<> put(std::span<temporary_buffer<char>> bufs);

    // Returns payload bytes; an empty buffer means EOF (peer FIN).
    future<temporary_buffer<char>> get();

    // Waits until all queued bytes are posted to the SQ (NOT until their
    // send completions arrive -- matching TCP flush semantics, where the
    // data has been handed to the kernel). Posted buffers stay alive in
    // the inflight ring until their CQEs; shutdown_output() drains those
    // before the FIN.
    future<> flush();

    // Half-close: send FIN after flushing pending data.
    future<> shutdown_output();

    // Completion driver (called by rdma_device, event or poll mode).
    // Returns true if any completion was processed.
    bool poll_cq_and_dispatch();

    bool owns_cq(cq_h cq) const noexcept {
        return cq == _res.send_cq || cq == _res.recv_cq || cq == _res.polling_cq;
    }

    bool failed() const noexcept { return _failed; }

    // The TCP fd kept open as a liveness detector (see watch_peer_death).
    pollable_fd& tcp_fd() noexcept { return _tcp; }

    // Forcibly fail the connection (resolves all waiters with an
    // error). Used by the socket impls on abort/error paths.
    void fail(int err);

    // The receive block size negotiated with the peer (each WR we post
    // must fit one of the peer's receive slots). Zero before the
    // handshake completes.
    uint32_t remote_recv_block_size() const noexcept {
        return _remote_recv_block_size;
    }

    // Called by the factory (make_lw_shared needs a public ctor).
    rdma_endpoint(rdma_device* dev, pollable_fd tcp_fd);

private:

    // Handshake helpers.
    future<> run_client_handshake(lowres_clock::duration timeout,
                                  std::vector<char>& prefix);
    future<bool> run_server_handshake(const char* client_hello,
                                      lowres_clock::duration timeout);
    void watch_peer_death();
    void init_rings();
    void apply_remote_hello(const hello_params& remote);
    int bring_up_qp(const hello_params& remote);

    // Send path internals.
    void post_more_sends();
    // Full drain: waits until the backlog is empty AND every posted WR
    // has completed (send CQE) AND no FIN is queued. Used by
    // shutdown_output() -- the FIN (and the teardown that follows it)
    // must not overtake data still owned by the hardware.
    future<> drain_all();
    // Copy-path staging: append bytes to the open block, opening a new
    // one when full (pushing the full one to _send_pending). Throws on
    // pool exhaustion.
    void copy_stage(const char* data, size_t len);
    // Push the open staging block (if any, with bytes) to _send_pending.
    void close_copy_block();
    int send_imm(uint32_t host_imm); // pure-ACK/FIN WR
    void send_ack_if_due();
    uint32_t lookup_lkey(const char* block);
    // Direct-send MR management: pin a registration covering
    // [buf, buf+size) (registering on miss), unpin drops one
    // reference. Deregistration at zero.
    mr_h pin_direct_mr(const char* buf, size_t size);
    void unpin_direct_mr(mr_h mr);
    // Drop all pins (fail/destructor paths).
    void release_all_mr_pins();

    // Receive path internals.
    int post_recv_ring(uint32_t num);
    // Consumer-side slot recycle: re-post the ring slot freed by the
    // application taking the payload, and return the RQ credit to the
    // peer (bounded flow control: credits are owed on consumption, not
    // on receive).
    void recv_consume(size_t payload_len);
    void on_send_wc(ibv_wc& wc);
    void on_recv_wc(ibv_wc& wc);
    void wake_get();
    void wake_window();
    void wake_flush();

    rdma_device* _dev;
    pollable_fd _tcp;             // liveness detector after handshake
    qp_resources _res{};
    // Minimum payload for the direct-send path (put()); smaller
    // payloads take the pooled copy path, where the memcpy is cheaper
    // than an MR registration (reg_mr is a ~20us syscall; the
    // threshold defaults to 256K, where the two roughly balance on
    // an MR-cache miss; a cache hit is pure win at any size).
    size_t _direct_min_size = 256 << 10;

    // Negotiated parameters. _rq_size is the posted receive ring depth
    // (may be shallower than the QP's max_recv_wr when large receive
    // blocks are negotiated); _recv_block_size is our advertised block
    // size the ring blocks carry.
    uint16_t _sq_size = 0, _rq_size = 0;
    size_t _recv_block_size = 0;
    uint32_t _remote_recv_block_size = 0;
    uint16_t _local_window_capacity = 0, _remote_window_capacity = 0;

    // Flow control (brpc semantics; single-threaded access from the
    // home shard except where noted).
    uint16_t _remote_rq_window_size = 0;
    uint16_t _sq_window_size = 0;
    uint16_t _sq_imm_window_size = 0;
    uint16_t _accumulated_ack = 0;
    uint16_t _unsolicited = 0;
    uint32_t _unsolicited_bytes = 0;
    uint16_t _sq_current = 0, _sq_unsignaled = 0, _sq_sent = 0;
    uint16_t _rq_received = 0; // CQE-completed RQ slots (staging cursor)
    uint16_t _rq_post = 0;     // next ring position to (re)post
    uint32_t _new_rq_wrs = 0;

    // Send backlog: put() appends here (ownership taken), the
    // completion driver drains it while windows allow. A non-null `mr`
    // marks a direct entry: the buffer is the caller's memory, `mr`
    // holds a registration pin released when the entry leaves the
    // deque (consumed by a WR) or the connection fails. Pool blocks
    // (copy path) carry a null `mr`.
    struct pending_send {
        temporary_buffer<char> buf;
        mr_h mr = nullptr; // direct path: pinned registration
    };
    std::deque<pending_send> _send_pending;
    size_t _send_pending_offset = 0; // bytes of front buffer consumed
    // Open staging block of the copy path: successive small puts
    // (e.g. rpc's 128K snd_buf chunks of one large message) accumulate
    // here instead of one pool block each, so a large message coalesces
    // into whole send blocks -- far fewer WRs and completions. Closed
    // (pushed to _send_pending) when full, before any direct entry is
    // queued, and by flush()/shutdown_output().
    temporary_buffer<char> _copy_block;
    size_t _copy_off = 0;
    // Direct-path MR registry. A registration stays alive while its
    // pin count is nonzero; pins are held by:
    //   - the cache slot (_mr_cache below),
    //   - each backlog entry (pending_send::mr),
    //   - each posted-not-completed SGE (inflight::mrs).
    // Deregistration happens exactly when the count reaches zero, so a
    // cache hit can never resurrect a dead handle.
    struct mr_entry {
        const char* buf; // buffer start the registration covers
        size_t size;     // registered length from buf
        unsigned pins;
    };
    std::unordered_map<mr_h, mr_entry> _mr_refs;
    // Recent registrations keyed by buffer start: repeated sends of
    // the same memory (rpc reuses its snd_buf storage) hit here and
    // skip the ~10us registration. Bounded; a miss just registers.
    // Sized to cover several concurrent streams' interleaved buffer
    // sets without thrashing. 
    static constexpr size_t mr_cache_slots = 64;
    std::unordered_map<const char*, mr_h> _mr_cache;
    // Posted-but-uncompleted sends, ring-indexed by SQ slot: the pool
    // blocks whose lifetimes the hardware still owns (a WR may span
    // several blocks when _remote_recv_block_size exceeds one block),
    // plus the direct-path MR pins (one per referencing SGE).
    struct inflight {
        std::vector<temporary_buffer<char>> bufs;
        std::vector<mr_h> mrs; // direct-path MR pins
    };
    std::vector<inflight> _sbuf; // ring of _sq_size - reserved_wr_num

    // Receive ring of pool blocks.
    std::vector<temporary_buffer<char>> _rbuf; // owns registered blocks
    std::vector<ibv_sge> _rsge;                // SGE per RQ slot
    // Staging for completed payloads, drained by get().
    std::deque<temporary_buffer<char>> _recv_staging;
    size_t _recv_staging_offset = 0;

    // Waiters (at most one each; seastar streams are sequential).
    std::optional<promise<temporary_buffer<char>>> _get_waiter;
    std::optional<promise<>> _window_waiter;
    std::optional<promise<>> _flush_waiter;

    bool _peer_fin = false;
    bool _failed = false;
    bool _declined = false; // client declined: fd handed back, don't close
    bool _sending_fin = false;
    bool _fin_queued = false; // FIN piggybacks on the next data WR
    bool _fin_sent = false;
    bool _destroyed_qp = false;

    friend class rdma_device;
};

} // namespace seastar::net::rdma

#endif // SEASTAR_HAVE_RDMA
