/*
 * Copyright 2026 Seastar contributors
 */

#include "rdma-endpoint.hh"

#ifdef SEASTAR_HAVE_RDMA

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>

#include <seastar/core/internal/buffer_allocator.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/util/log.hh>

namespace seastar::net::rdma {

static logger ep_log("rdma-ep");

// QP attributes (same values as brpc).
static constexpr uint8_t qp_timeout = 14;
static constexpr uint8_t qp_retry_cnt = 7;
// ~64us RNR NAK delay (encoding: 1 = 655.36ns, +1 doubles; 12 = 66us).
static constexpr uint8_t qp_min_rnr_timer = 12;
static constexpr uint8_t max_hop_limit = 16;

rdma_endpoint::rdma_endpoint(rdma_device* dev, pollable_fd tcp_fd)
    : _dev(dev), _tcp(std::move(tcp_fd))
    , _direct_min_size(dev->direct_min_size()) {}

rdma_endpoint::~rdma_endpoint() {
    _dev->unregister_endpoint(this);
    // Close the liveness fd first: this RDHUPs the peer's death watch
    // (failing its endpoint) and releases our own pending watch's
    // pollable_fd state. The pending watch future is then destroyed
    // without a continuation holding a reference.
    // Exception: the client-side decline path hands the fd back for the
    // TCP fallback on the same connection — both this endpoint and the
    // caller share the pollable_fd_state, so closing here (or in the
    // caller) would only drop the shared refcount. Mark _declined to
    // document the hand-off; the state dies when the fallback stream
    // releases its handle.
    if (!_declined) {
        _tcp.close();
    }
    release_all_mr_pins();
    if (!_destroyed_qp) {
        _dev->release_qp(std::move(_res));
    }
}

// ---- handshake ---------------------------------------------------------
//
// Read until `buf` holds at least `n` bytes (or EOF), with timeout.
// Returns false on EOF before n bytes.
static future<bool> read_exactly(pollable_fd& fd, std::vector<char>& buf,
                                 size_t n, lowres_clock::duration timeout) {
    struct alloc_t final : internal::buffer_allocator {
        temporary_buffer<char> allocate_buffer() override {
            return temporary_buffer<char>(4096);
        }
    } alloc;
    co_await with_timeout(lowres_clock::now() + timeout, [&]() -> future<> {
        while (buf.size() < n) {
            temporary_buffer<char> b = co_await fd.recv_some(&alloc);
            if (b.empty()) {
                break; // EOF
            }
            std::copy(b.begin(), b.end(), std::back_inserter(buf));
        }
    }());
    co_return buf.size() >= n;
}

// Write the whole buffer with timeout.
static future<> write_exactly(pollable_fd& fd, const char* data, size_t n,
                              lowres_clock::duration timeout) {
    co_await with_timeout(lowres_clock::now() + timeout, [&]() -> future<> {
        size_t sent = 0;
        while (sent < n) {
            // pollable_fd::write_some takes a span<iovec> at API 9.
            iovec iov{const_cast<char*>(data) + sent, n - sent};
            size_t k = co_await fd.write_some(std::span<iovec>{&iov, 1});
            if (k == 0) {
                throw std::runtime_error("rdma handshake: EOF during write");
            }
            sent += k;
        }
    }());
    co_return;
}

future<std::optional<lw_shared_ptr<rdma_endpoint>>> rdma_endpoint::connect(
        rdma_device* dev, pollable_fd& tcp_fd, socket_address remote,
        lowres_clock::duration timeout, std::vector<char>& prefix) {
    lw_shared_ptr<rdma_endpoint> ep;
    try {
        ep = make_lw_shared<rdma_endpoint>(dev, tcp_fd);
        co_await ep->run_client_handshake(timeout, prefix);
    } catch (std::runtime_error&) {
        // Declined by the peer (not RDMA-capable, EOF or garbage
        // reply). Fall back to TCP on the SAME connection: hand the fd
        // back to the caller and tear down the half-built endpoint
        // without closing the fd (it is shared via pollable_fd_state).
        ep->_declined = true;
        tcp_fd = ep->_tcp;
        ep = nullptr;
        co_return std::nullopt;
    } catch (...) {
        ep->_failed = true;
        throw;
    }
    co_return ep;
}

future<lw_shared_ptr<rdma_endpoint>> rdma_endpoint::accept(
        rdma_device* dev, pollable_fd tcp_fd, socket_address remote,
        const char* client_hello, lowres_clock::duration timeout) {
    auto ep = make_lw_shared<rdma_endpoint>(dev, std::move(tcp_fd));
    bool ok = false;
    try {
        ok = co_await ep->run_server_handshake(client_hello, timeout);
    } catch (...) {
        ep->_failed = true;
        throw;
    }
    if (!ok) {
        co_return nullptr;
    }
    co_return ep;
}

future<> rdma_endpoint::run_client_handshake(lowres_clock::duration timeout,
                                             std::vector<char>& prefix) {
    ep_log.debug("client handshake: start");
    // Allocate resources first (so the hello carries our qp_num).
    _res = _dev->acquire_qp();
    _sq_size = _dev->qp_depth();
    _recv_block_size = _dev->recv_block_size();
    // Keep the ring's registered memory footprint at ~qp_depth 8K
    // blocks worth: fewer slots for larger blocks. The floor of
    // min_qp_size (16) is what the handshake validation accepts.
    _rq_size = uint16_t(std::clamp<size_t>(
        size_t(_sq_size) * rdma_buffer_pool::class_sizes[0] / _recv_block_size,
        min_qp_size, _sq_size));
    init_rings();
    // Register with the completion dispatcher NOW: the peer may start
    // sending the moment it sees our ACK, and a completion dispatcher NOW: the peer may start
    // sending the moment it sees our ACK, and a completion landing in
    // an unowned CQ would be drained and discarded by the watcher.
    _dev->register_endpoint(this);

    hello_params local{};
    local.block_size = uint32_t(_recv_block_size);
    local.sq_size = _sq_size;
    local.rq_size = _rq_size;
    const gid_selection& sel = _dev->selection();
    local.lid = sel.lid;
    std::memcpy(local.peer_gid.raw.data(), sel.gid.raw, 16);
    local.qp_num = _res.qp->qp_num;

    char wire[hello_total_len];
    write_hello(wire, local);
    ep_log.debug("client handshake: sending hello qp_num={}", local.qp_num);
    co_await write_exactly(_tcp, wire, sizeof(wire), timeout);
    ep_log.debug("client handshake: hello sent, reading reply");

    // Read the server hello (magic + body).
    std::vector<char> in;
    bool got = co_await read_exactly(_tcp, in, hello_total_len, timeout);
    if (!got) {
        prefix = std::move(in); // partial bytes belong to the TCP stream
        throw std::runtime_error("rdma handshake: EOF from server");
    }
    if (std::memcmp(in.data(), hello_magic, hello_magic_len) != 0) {
        // Not an RDMA server: leave the bytes in prefix for the TCP
        // fallback stream.
        prefix = std::move(in);
        throw std::runtime_error("rdma handshake: declined");
    }
    hello_params remote{};
    if (parse_hello_body(in.data() + hello_magic_len, remote) != hello_result::ok) {
        prefix = std::move(in);
        throw std::runtime_error("rdma handshake: declined");
    }

    apply_remote_hello(remote);
    if (bring_up_qp(remote) != 0) {
        throw std::system_error(errno, std::system_category(), "rdma: bring_up_qp failed");
    }

    // ACK.
    char ack[hello_ack_len];
    write_hello_ack(ack, true);
    co_await write_exactly(_tcp, ack, sizeof(ack), timeout);

    watch_peer_death();
}

future<bool> rdma_endpoint::run_server_handshake(const char* client_hello,
                                                 lowres_clock::duration timeout) {
    ep_log.debug("server handshake: start");
    // client_hello holds the full 40 bytes already.
    hello_params remote{};
    if (parse_hello_body(client_hello + hello_magic_len, remote) != hello_result::ok) {
        // Decline politely: the client will fall back to TCP. The
        // magic was already consumed by the accept path's sniffing;
        // the decline is delivered as a zeroed body.
        char wire[hello_total_len];
        std::memcpy(wire, hello_magic, hello_magic_len);
        write_decline_body(wire + hello_magic_len);
        co_await write_exactly(_tcp, wire, sizeof(wire), timeout);
        // Drain the client's ACK (best effort).
        std::vector<char> sink;
        co_await read_exactly(_tcp, sink, hello_ack_len, timeout);
        co_return false;
    }

    _res = _dev->acquire_qp();
    _sq_size = _dev->qp_depth();
    _recv_block_size = _dev->recv_block_size();
    _rq_size = uint16_t(std::clamp<size_t>(
        size_t(_sq_size) * rdma_buffer_pool::class_sizes[0] / _recv_block_size,
        min_qp_size, _sq_size));
    init_rings();
    // Register before anything can complete in our CQs (same rationale
    // as the client side: the watcher discards completions of unowned
    // CQs, and the client may ACK — then send — at any moment).
    _dev->register_endpoint(this);
    apply_remote_hello(remote);
    if (bring_up_qp(remote) != 0) {
        throw std::system_error(errno, std::system_category(), "rdma: bring_up_qp failed");
    }

    hello_params local{};
    local.block_size = uint32_t(_recv_block_size);
    local.sq_size = _sq_size;
    local.rq_size = _rq_size;
    const gid_selection& sel = _dev->selection();
    local.lid = sel.lid;
    std::memcpy(local.peer_gid.raw.data(), sel.gid.raw, 16);
    local.qp_num = _res.qp->qp_num;

    char wire[hello_total_len];
    write_hello(wire, local);
    ep_log.debug("server handshake: sending hello qp_num={}", local.qp_num);
    co_await write_exactly(_tcp, wire, sizeof(wire), timeout);
    ep_log.debug("server handshake: hello sent, waiting ACK");

    // Wait for the client's ACK.
    std::vector<char> ack;
    bool got = co_await read_exactly(_tcp, ack, hello_ack_len, timeout);
    if (!got || !parse_hello_ack(ack.data())) {
        throw std::runtime_error("rdma handshake: client refused");
    }

    watch_peer_death();
    co_return true;
}

void rdma_endpoint::watch_peer_death() {
    // The TCP fd stays open after the handshake purely as a liveness
    // detector (brpc's HandleTcpEventAfterEstablished trick): a peer
    // crash closes it and we fail the endpoint. The continuation must
    // NOT hold a strong reference: both endpoints of a connection run
    // this watch, and each holds its fd open until its own endpoint
    // dies — strong refs would keep both alive forever (leaking the
    // QPs, the fds and the pool blocks). A weak capture lets the last
    // external reference destroy the endpoint; closing the fd in the
    // destructor/fail() then resolves the peer's watch normally.
    // gcc's -Wdangling-pointer false-positives on the intrusive-list
    // push inside weak_from_this() (the hook lives in the captured
    // copy, not in the temporary); split the capture to hide the store.
    weak_ptr<rdma_endpoint> weak = weak_from_this();
    (void)_tcp.poll_rdhup().then([weak = std::move(weak)] () mutable {
        if (weak && !weak->_failed) {
            weak->fail(ECONNRESET);
        }
    }).handle_exception([] (std::exception_ptr) {});
}

void rdma_endpoint::init_rings() {
    _sbuf.resize(size_t(_sq_size) - reserved_wr_num);
    _rbuf.resize(_rq_size);
    _rsge.resize(_rq_size);
}

void rdma_endpoint::apply_remote_hello(const hello_params& remote) {
    // Same math as brpc ApplyRemoteHello. The peer's block size is
    // honored up to our largest pool class: each WR we post must fit
    // into one of the peer's posted receive slots.
    _remote_recv_block_size = std::min<uint32_t>(
        remote.block_size,
        uint32_t(rdma_buffer_pool::class_sizes[rdma_buffer_pool::num_classes - 1]));
    _local_window_capacity = std::min<uint16_t>(_sq_size, remote.rq_size) - reserved_wr_num;
    _remote_window_capacity = std::min<uint16_t>(_rq_size, remote.sq_size) - reserved_wr_num;
    _sq_imm_window_size = reserved_wr_num;
    _remote_rq_window_size = _local_window_capacity;
    _sq_window_size = _local_window_capacity;
}

int rdma_endpoint::bring_up_qp(const hello_params& remote) {
    const verbs* v = verbs_provider();
    const gid_selection& sel = _dev->selection();

    // RESET -> INIT
    qp_attr_desc d{};
    d.qp_state = IBV_QPS_INIT;
    d.port_num = sel.port;
    d.qp_access_flags = IBV_ACCESS_REMOTE_WRITE;
    int mask = QP_STATE | QP_PKEY_INDEX | QP_PORT | QP_ACCESS_FLAGS;
    if (v->modify_qp(_res.qp, d, mask) != 0) {
        return -1;
    }

    // Post the full RQ while in INIT (mandatory: receiver must be
    // armed before RTR or the peer's first send finds no RQ).
    if (post_recv_ring(_rq_size) != 0) {
        return -1;
    }

    // INIT -> RTR
    d = qp_attr_desc{};
    d.qp_state = IBV_QPS_RTR;
    d.path_mtu = IBV_MTU_1024;
    d.dest_qp_num = remote.qp_num;
    d.rq_psn = 0;
    d.max_dest_rd_atomic = 0;
    // RNR backpressure: receive slots are re-posted only when the
    // application consumes data (flow control), so the RQ may run
    // empty while the peer still has window. A real RNR NAK (with an
    // infinite retry count and ~64us timer) makes the sender wait
    // instead of erroring the QP -- the transport-level equivalent of
    // TCP zero-window probing.
    d.min_rnr_timer = qp_min_rnr_timer;
    std::memcpy(d.dgid.raw, remote.peer_gid.raw.data(), sizeof(d.dgid.raw));
    d.sgid_index = sel.gid_index;
    d.hop_limit = max_hop_limit;
    d.dlid = remote.lid;
    d.ah_port_num = sel.port;
    mask = QP_STATE | QP_PATH_MTU | QP_MIN_RNR_TIMER | QP_AV |
           QP_MAX_DEST_RD_ATOMIC | QP_DEST_QPN | QP_RQ_PSN;
    if (v->modify_qp(_res.qp, d, mask) != 0) {
        return -1;
    }

    // RTR -> RTS
    d = qp_attr_desc{};
    d.qp_state = IBV_QPS_RTS;
    d.timeout = qp_timeout;
    d.retry_cnt = qp_retry_cnt;
    // rnr_retry = 7 means infinite RNR retries (see RTR note).
    d.rnr_retry = 7;
    d.sq_psn = 0;
    d.max_rd_atomic = 0;
    mask = QP_STATE | QP_RNR_RETRY | QP_RETRY_CNT | QP_TIMEOUT |
           QP_SQ_PSN | QP_MAX_QP_RD_ATOMIC;
    if (v->modify_qp(_res.qp, d, mask) != 0) {
        return -1;
    }

    return 0;
}

// ---- receive ring -------------------------------------------------------

int rdma_endpoint::post_recv_ring(uint32_t num) {
    const verbs* v = verbs_provider();
    for (uint32_t i = 0; i < num; ++i) {
        // borrow() may extend the pool; the block stays owned by
        // _rbuf[_rq_post] until the CQE retires it.
        _rbuf[_rq_post] = _dev->pool().borrow(_recv_block_size);
        _rsge[_rq_post] = ibv_sge{
            reinterpret_cast<uint64_t>(_rbuf[_rq_post].get_write()),
            uint32_t(_recv_block_size),
            lookup_lkey(_rbuf[_rq_post].get())};
        recv_wr_desc wr{&_rsge[_rq_post], 1};
        if (v->post_recv(_res.qp, wr) != 0) {
            return -1;
        }
        if (++_rq_post == _rq_size) {
            _rq_post = 0;
        }
    }
    return 0;
}

uint32_t rdma_endpoint::lookup_lkey(const char* block) {
    // The mock/real provider owns the MR registry; we route through
    // the pool which registered the memory.
    return _dev->pool().lkey_of(block);
}

// ---- direct-send MR registry ----------------------------------------------

mr_h rdma_endpoint::pin_direct_mr(const char* buf, size_t size) {
    // Cache hit: a live registration covering this exact buffer start
    // and at least `size` bytes. Returns with one additional pin held
    // by the caller (the backlog entry's pin, released when the entry
    // leaves _send_pending).
    auto cit = _mr_cache.find(buf);
    if (cit != _mr_cache.end()) {
        auto it = _mr_refs.find(cit->second);
        if (it != _mr_refs.end() && it->second.size >= size) {
            ++it->second.pins;
            return it->first;
        }
        // Stale cache entry (deregistered below zero elsewhere, or a
        // smaller registration): drop it and re-register.
        _mr_cache.erase(cit);
    }
    const verbs* v = verbs_provider();
    mr_h mr = v->reg_mr(_dev->pd(), const_cast<char*>(buf), size,
                        IBV_ACCESS_LOCAL_WRITE);
    if (!mr) {
        ep_log.debug("direct reg_mr failed: ep={} errno={}",
                     static_cast<void*>(this), errno);
        return nullptr;
    }
    // A provider may reuse the handle of a fully-deregistered MR; if
    // the stale entry is still cached, forget it first.
    auto stale = _mr_refs.find(mr);
    if (stale != _mr_refs.end()) {
        _mr_cache.erase(stale->second.buf);
        _mr_refs.erase(stale);
    }
    // Two pins at birth: the cache slot's and the caller's (backlog
    // entry). With only the cache pin, a fully-completed send would
    // drop the count to zero (WR pins come and go symmetrically; the
    // pop unpin consumes the caller's) and deregister the MR out from
    // under the cache -- the next re-send of the same memory would
    // miss and re-register.
    _mr_refs.emplace(mr, mr_entry{buf, size, 2});
    // Bound the cache: evict the oldest (first) entry. Eviction drops
    // only the CACHE pin; backlog/posted pins keep the registration
    // alive and a future re-send simply re-registers on the miss.
    if (_mr_cache.size() >= mr_cache_slots) {
        auto oldest = _mr_cache.begin();
        auto eit = _mr_refs.find(oldest->second);
        _mr_cache.erase(oldest);
        if (eit != _mr_refs.end()) {
            unpin_direct_mr(eit->first); // drops the cache pin
        }
    }
    _mr_cache.emplace(buf, mr);
    return mr;
}

void rdma_endpoint::unpin_direct_mr(mr_h mr) {
    auto it = _mr_refs.find(mr);
    if (it == _mr_refs.end()) {
        return; // already fully released
    }
    if (--it->second.pins == 0) {
        _mr_cache.erase(it->second.buf); // if cached
        verbs_provider()->dereg_mr(it->first);
        _mr_refs.erase(it);
    }
}

void rdma_endpoint::release_all_mr_pins() {
    // fail()/destructor: drop backlog pins, posted-WR pins, then the
    // cache pins; every registration ends fully released.
    for (auto& p : _send_pending) {
        if (p.mr) {
            unpin_direct_mr(p.mr);
            p.mr = nullptr;
        }
    }
    for (auto& slot : _sbuf) {
        for (mr_h mr : slot.mrs) {
            unpin_direct_mr(mr);
        }
        slot.mrs.clear();
    }
    while (!_mr_cache.empty()) {
        auto oldest = _mr_cache.begin();
        mr_h mr = oldest->second;
        _mr_cache.erase(oldest);
        unpin_direct_mr(mr);
    }
}

// ---- send path -----------------------------------------------------------

void rdma_endpoint::copy_stage(const char* data, size_t len) {
    // Accumulate into the open staging block; whole blocks are pushed
    // to the backlog as they fill. Successive small puts of one large
    // rpc message (128K snd_buf chunks) thus coalesce into whole
    // send-sized blocks instead of one pool block per chunk. 
    while (len > 0) {
        if (!_copy_block) {
            _copy_block = _dev->pool().borrow(_remote_recv_block_size);
            _copy_off = 0;
        }
        size_t n = std::min(len, _copy_block.size() - _copy_off);
        std::memcpy(_copy_block.get_write() + _copy_off, data, n);
        _copy_off += n;
        data += n;
        len -= n;
        if (_copy_off == _copy_block.size()) {
            close_copy_block();
        }
    }
}

void rdma_endpoint::close_copy_block() {
    if (_copy_block) {
        if (_copy_off > 0) {
            _copy_block.trim(_copy_off);
            _send_pending.push_back(pending_send{std::move(_copy_block), nullptr});
        }
        _copy_block = temporary_buffer<char>();
        _copy_off = 0;
    }
}

future<> rdma_endpoint::put(std::span<temporary_buffer<char>> bufs) {
    if (_failed) {
        throw std::system_error(ECONNRESET, std::system_category(), "rdma: connection failed");
    }
    // Take ownership synchronously. Two paths, chosen per buffer:
    //
    //   direct: buffers >= _direct_min_size are registered on the fly
    //   (via the pinned MR cache) and queued as-is -- the SGE points
    //   at the caller's memory, zero copies. The default threshold
    //   (256K) is where reg_mr (~20us syscall) roughly equals the
    //   memcpy it replaces; below it a cache MISS costs many times
    //   the copy. A HIT (callers that re-send the same memory, e.g.
    //   rpc stream sinks) is pure win at any size >= threshold.
    //
    //   copy: smaller buffers accumulate into the open staging block
    //   (pool blocks of the negotiated send size) -- packing many
    //   small puts into whole blocks. A buffer larger than the staging
    //   size spans several blocks. The registration would be more
    //   expensive than the copy at that size.
    //
    // After this section the caller's storage may die (iostream
    // contract) -- no co_await may appear before ownership is taken.
    try {
        for (auto& b : bufs) {
            if (b.size() == 0) {
                continue;
            }
            // Direct-send economics: registration (or a pool-resident
            // buffer) pays off only when the entry leaves the sender in
            // few WRs. Every WR costs a completion on both sides plus,
            // for registered memory, an MR pin/unpin pair of map
            // operations. A WR is bounded by the peer's receive block,
            // so a buffer much larger than the negotiated block size
            // would be cut into many WRs: the copy path (pool blocks
            // with address-derived lkeys, one pin-free WR each) is
            // cheaper there. Direct is taken only when the buffer is at
            // least _direct_min_size (a registration must buy back its
            // ~20us against the memcpy it saves, on a cache miss) and
            // the WR cut (see post_more_sends) stays small: past that
            // the per-WR pin/CQE overhead swamps the win.
            constexpr size_t direct_max_wrs = 64;
            const size_t direct_wrs =
                (b.size() + _remote_recv_block_size - 1) / _remote_recv_block_size;
            // Buffers already living in a registered pool region (pooled
            // receive staging echoed back, or app buffers carved from the
            // pool): send zero-copy with no registration at all -- the
            // lkey is derivable from the address (pool path). These are
            // exempt from the block-size gate: no registration and no
            // pin accounting cost anything here, and pool-resident
            // buffers are at most one pool class in size.
            if (b.size() >= _direct_min_size &&
                    _dev->pool().lkey_of_safe(b.get()) != 0) {
                close_copy_block(); // ordering: bytes must not overtake
                _send_pending.push_back(pending_send{std::move(b), nullptr});
                continue;
            }
            if (b.size() >= _direct_min_size && direct_wrs <= direct_max_wrs) {
                mr_h mr = pin_direct_mr(b.get(), b.size());
                if (mr) {
                    close_copy_block(); // ordering: bytes must not overtake
                    _send_pending.push_back(
                        pending_send{std::move(b), mr});
                    continue;
                }
                // Registration refused (pinning limits, OOM): fall back to
                // the copy path for this buffer rather than failing the
                // connection.
            }
            copy_stage(b.get(), b.size());
        }
    } catch (...) {
        // A pool borrow failed mid-staging: the backlog state stays
        // consistent (only fully-owned blocks are queued; the open
        // block holds no caller bytes... but may hold some -- drop it
        // rather than resend a partially-filled block later).
        _copy_block = temporary_buffer<char>();
        _copy_off = 0;
        throw;
    }

    post_more_sends();
    while (!_send_pending.empty() && !_failed) {
        // Arm before checking: a window opening between the last
        // post_more_sends() and the sleep would otherwise be lost (iits
        // wake finds no waiter and evaporates) and put() would sleep
        // forever.
        if (!_window_waiter) {
            _window_waiter = promise<>();
        }
        post_more_sends();
        if (_send_pending.empty() || _failed) {
            _window_waiter = std::nullopt;
            break;
        }
        co_await _window_waiter->get_future();
        _window_waiter = std::nullopt;
    }
    if (_failed) {
        throw std::system_error(ECONNRESET, std::system_category(), "rdma: connection failed");
    }
}

void rdma_endpoint::post_more_sends() {
    const verbs* v = verbs_provider();
    const size_t max_sge = size_t(std::max(1, _dev->caps().max_sge));
    std::vector<ibv_sge> sges;
    sges.reserve(max_sge);

    while (!_send_pending.empty() && _remote_rq_window_size > 0 && _sq_window_size > 0) {
        sges.clear();
        size_t this_len = 0;
        // Blocks consumed by this WR; moved into the inflight ring
        // below so their memory outlives the post_send until the CQE
        // retires the SQ slot.
        std::vector<temporary_buffer<char>> consumed;
        // Direct-path MR pins taken over by this WR's SGEs (one per
        // referencing WR; on_send_wc drops them).
        std::vector<mr_h> consumed_mrs;
        // Fill up to max_sge SGEs or remote_recv_block_size bytes,
        // cutting at buffer boundaries (brpc's cut loop).
        //
        // Pin accounting (direct entries): the entry's backlog pin
        // lives until the entry pops; additionally every WR that
        // references the MR takes one pin of its own (bumped at SGE
        // push, dropped by the send CQE). A buffer split across
        // several WRs is thus covered per-WR, and the backlog pin
        // covers the queuing window. All bumps/releases are symmetric:
        // put() pins / pop unpins; WR posts pin / CQEs unpin; the
        // cache pins / eviction (or release_all) unpins.
        while (this_len < _remote_recv_block_size && sges.size() < max_sge &&
               !_send_pending.empty()) {
            pending_send& front = _send_pending.front();
            const char* base = front.buf.get() + _send_pending_offset;
            size_t avail = front.buf.size() - _send_pending_offset;
            size_t cap = _remote_recv_block_size - this_len;
            // Both pool blocks (staged at the negotiated send size) and
            // direct buffers are cut only by the remaining WR room: a
            // WR never exceeds the peer's advertised receive slot, and
            // the peer's slots each hold a full block.
            size_t take = std::min({avail, cap});
            if (take == 0) {
                break;
            }
            if (front.mr) {
                sges.push_back(ibv_sge{
                    reinterpret_cast<uint64_t>(base),
                    uint32_t(take),
                    front.mr->lkey});
                // One extra pin per WR referencing the MR (dedup'd
                // within the WR: multiple SGEs of the same MR here
                // share the WR's single pin).
                if (consumed_mrs.empty() || consumed_mrs.back() != front.mr) {
                    consumed_mrs.push_back(front.mr);
                    ++_mr_refs[front.mr].pins;
                }
            } else {
                sges.push_back(ibv_sge{
                    reinterpret_cast<uint64_t>(base),
                    uint32_t(take),
                    lookup_lkey(front.buf.get())});
            }
            this_len += take;
            _send_pending_offset += take;
            if (_send_pending_offset == front.buf.size()) {
                // Buffer fully consumed: hand it to the WR's lifetime
                // and release the backlog pin.
                consumed.push_back(std::move(front.buf));
                if (front.mr) {
                    unpin_direct_mr(front.mr);
                }
                _send_pending.pop_front();
                _send_pending_offset = 0;
            }
        }
        if (sges.empty()) {
            break;
        }

        // "last" must account for staged-but-unqueued bytes: with an
        // open copy block more data is coming, so this WR is not the
        // final one (FIN piggyback and SOLICITED depend on it).
        bool last = _send_pending.empty() && _copy_off == 0;
        uint32_t ack = _new_rq_wrs;
        _new_rq_wrs = 0;
        // The FIN rides the last data WR when shutdown_output() raced
        // with queued data (or its pure-IMM attempt lost the window).
        bool fin = (_sending_fin || _fin_queued) && last;
        _fin_queued = _fin_queued && !fin;
        // imm_data goes on the wire in network order.
        uint32_t imm = imm_hton(host_to_imm(ack, fin));

        // Solicited decision (brpc rules).
        bool solicited = false;
        if (_remote_rq_window_size == 1 || _sq_window_size == 1 || last) {
            solicited = true;
        } else if (_unsolicited > _local_window_capacity / 4) {
            solicited = true;
        } else if (_accumulated_ack > _remote_window_capacity / 4) {
              solicited = true;
        } else if (_unsolicited_bytes > 1 << 20) {
            solicited = true;
        } else {
            ++_unsolicited;
            _unsolicited_bytes += this_len;
            _accumulated_ack += ack;
        }
        if (solicited) {
            _unsolicited = 0;
            _unsolicited_bytes = 0;
            _accumulated_ack = 0;
        }

        // Signaled batching (brpc rules): wr_id counts the batch. The
        // WR that empties the backlog is always signaled: unlike brpc
        // we need a full-drain flush(), and only a signaled CQE covers
        // the trailing unsignaled WRs (a signaled completion implies
        // all earlier SQ entries completed).
        ++_sq_unsignaled;
        bool signaled = _sq_unsignaled >= _local_window_capacity / 4 || last;

        send_wr_desc wr{};
        wr.wr_id = signaled ? _sq_unsignaled : 0;
        wr.with_imm = true;
        wr.imm_data = imm;
        wr.sg_list = sges.data();
        wr.num_sge = int(sges.size());
        wr.solicited = solicited;
        wr.signaled = signaled;
        if (v->post_send(_res.qp, wr) != 0) {
            ep_log.debug("post_send failed: ep={} errno={}", static_cast<void*>(this), errno);
            // The not-yet-posted pins taken for this WR must go back.
            for (mr_h mr : consumed_mrs) {
                if (mr) {
                    unpin_direct_mr(mr);
                }
            }
            fail(errno);
            return;
        }
        if (fin) {
            ep_log.debug("FIN piggybacked on data WR: ep={}", static_cast<void*>(this));
            _fin_sent = true;
        }
        if (signaled) {
            _sq_unsignaled = 0;
        }

        // Pool blocks stay alive (registered + unreturned) until the
        // send CQE frees this SQ slot. Direct-path MR pins are
        // released by the same CQE.
        _sbuf[_sq_current] = inflight{std::move(consumed), std::move(consumed_mrs)};

        ++_sq_current;
        if (_sq_current == _sq_size - reserved_wr_num) {
            _sq_current = 0;
        }
        --_remote_rq_window_size;
        --_sq_window_size;
    }
    // No wake here: every caller either runs in a loop that re-checks
    // on its own (put/flush) or follows this with an explicit wake
    // (on_send_wc). Waking unconditionally would resolve/spin the
    // caller's own wait loop.
}

int rdma_endpoint::send_imm(uint32_t host_imm) {
    if (host_imm == 0) {
        return 0;
    }
    if (_sq_imm_window_size == 0) {
        // No room for pure-IMM right now; the FIN/ack count stays
        // pending and will piggyback on the next data WR (callers
        // treat 0 as "not sent"; distinguish from success by the
        // window check, which callers do before calling or via the
        // retry loop in shutdown_output).
        return -1;
    }
    const verbs* v = verbs_provider();
    send_wr_desc wr{};
    wr.wr_id = 0;
    wr.with_imm = true;
    wr.imm_data = imm_hton(host_imm);
    wr.solicited = true;
    wr.signaled = true;
    int rc = v->post_send(_res.qp, wr);
    if (rc == 0) {
        --_sq_imm_window_size;
    } else {
        ep_log.debug("send_imm post_send failed: ep={} rc={} errno={}",
                     static_cast<void*>(this), rc, errno);
    }
    return rc;
}

void rdma_endpoint::send_ack_if_due() {
    if (_new_rq_wrs > _remote_window_capacity / 2) {
        // Only consume the pending count when the IMM actually goes
        // out; with no imm-window room it piggybacks on the next data
        // WR instead (clearing it here would leak credits).
        if (send_imm(host_to_imm(_new_rq_wrs, false)) == 0) {
            _new_rq_wrs = 0;
        }
    }
}

// ---- receive / completion dispatch ---------------------------------------

// Recycle the ring slot whose payload the consumer just took: re-post a
// fresh receive block and return the RQ credit to the peer. Runs from
// get() (application context), so a pool miss throws to the caller
// instead of killing the completion driver -- the get() caller retries
// or fails the connection explicitly.
void rdma_endpoint::recv_consume(size_t payload_len) {
    if (_failed || _destroyed_qp) {
        return;
    }
    // The payload occupied one posted RQ slot: account one credit. It
    // piggybacks on the next data WR (send_ack_if_due), or goes out as
    // a pure IMM once half the remote window is owed.
    _new_rq_wrs += payload_len ? 1 : 0;
    // Replenish the slot the staging block vacated. _rq_post cursor
    // tracks the next ring position to (re)post: everything between
    // _rq_post and _rq_received is currently in flight to hardware or
    // sitting in staging.
    const verbs* v = verbs_provider();
    try {
        _rbuf[_rq_post] = _dev->pool().borrow(_recv_block_size);
    } catch (...) {
        // Pool exhausted: the consumer will see this on the next get()
        // (failed endpoint). Not fatal to the shard.
        ep_log.debug("recv replenish out of pool blocks: ep={}",
                     static_cast<void*>(this));
        fail(ENOMEM);
        return;
    }
    _rsge[_rq_post] = ibv_sge{
        reinterpret_cast<uint64_t>(_rbuf[_rq_post].get_write()),
        uint32_t(_recv_block_size),
        lookup_lkey(_rbuf[_rq_post].get())};
    recv_wr_desc wr{&_rsge[_rq_post], 1};
    if (v->post_recv(_res.qp, wr) != 0) {
        fail(errno);
        return;
    }
    if (++_rq_post == _rq_size) {
        _rq_post = 0;
    }
    send_ack_if_due();
}

future<temporary_buffer<char>> rdma_endpoint::get() {
    while (true) {
        if (!_recv_staging.empty()) {
            auto buf = std::move(_recv_staging.front());
            _recv_staging.pop_front();
            size_t consumed = buf.size();
            if (_recv_staging_offset) {
                consumed -= std::min(_recv_staging_offset, buf.size());
                buf.trim_front(_recv_staging_offset);
                _recv_staging_offset = 0;
            }
            recv_consume(consumed);
            co_return buf;
        }
        if (_peer_fin) {
            co_return temporary_buffer<char>(); // EOF
        }
        if (_failed) {
            throw std::system_error(ECONNRESET, std::system_category(),
                                    "rdma: connection failed");
        }
        // Wait for the completion driver.
        promise<temporary_buffer<char>> p;
        _get_waiter = std::move(p);
        co_await _get_waiter->get_future();
        _get_waiter = std::nullopt;
    }
}

future<> rdma_endpoint::flush() {
    // TCP-flush semantics: wait only until every queued byte has been
    // POSTED to the SQ. Waiting for the send CQEs here (the RC ACK from
    // the peer, one wire-time per WR) serializes the rpc send chain --
    // with batched flushes the next message's write() parks on
    // _in_batch until this flush returns, so every message pays a full
    // in-flight drain and the window never fills. The posted buffers
    // are owned by the inflight ring until their CQEs; only a
    // connection teardown (shutdown_output / fail) needs that drain.
    close_copy_block();
    while (!_send_pending.empty()) {
        if (_failed) {
            throw std::system_error(ECONNRESET, std::system_category(),
                                    "rdma: connection failed");
        }
        // Arm before checking (same lost-wakeup guard as put).
        if (!_flush_waiter) {
            _flush_waiter = promise<>();
        }
        post_more_sends();
        if (_send_pending.empty()) {
            _flush_waiter = std::nullopt;
            break;
        }
        co_await _flush_waiter->get_future();
        _flush_waiter = std::nullopt;
    }
}

future<> rdma_endpoint::drain_all() {
    // Staged-but-unqueued bytes must join the backlog before the drain
    // check below, or they would never be sent.
    close_copy_block();
    while (!_send_pending.empty() || _sq_window_size != _local_window_capacity ||
           _fin_queued) {
        if (_failed) {
            throw std::system_error(ECONNRESET, std::system_category(),
                                    "rdma: connection failed");
        }
        // Arm before checking (same lost-wakeup guard as put).
        if (!_flush_waiter) {
            _flush_waiter = promise<>();
        }
        // A queued FIN may have been waiting for this very data.
        post_more_sends();
        if (_send_pending.empty() && _sq_window_size == _local_window_capacity &&
            !_fin_queued) {
            _flush_waiter = std::nullopt;
            break;
        }
        co_await _flush_waiter->get_future();
        _flush_waiter = std::nullopt;
    }
}

future<> rdma_endpoint::shutdown_output() {
    _sending_fin = true;
    // Flush pending, then FIN. Full drain: the FIN (and the teardown
    // that follows the half-close) must not overtake data whose WRs the
    // hardware still owns.
    close_copy_block();
    co_await drain_all();
    if (_failed) {
        co_return;
    }
    if (!_fin_sent) {
        // The FIN rides a pure-IMM WR out of the reserved imm window.
        // If the imm window is exhausted, queue it to piggyback on the
        // next data WR (post_more_sends sets it on the drain edge);
        // with nothing queued either, wait for imm completions to
        // reopen the window and retry.
        while (!_fin_sent && !_failed) {
            if (send_imm(host_to_imm(0, true)) == 0) {
                _fin_sent = true;
                break;
            }
            if (!_send_pending.empty()) {
                // The FIN goes out with the pending data.
                _fin_queued = true;
                break;
            }
            // Nothing would go out: wait for a completion to open the
            // window. Use a fresh waiter (the earlier drain_all() may
            // have consumed the promise object already).
            if (!_flush_waiter) {
                _flush_waiter = promise<>();
            }
            co_await _flush_waiter->get_future();
            _flush_waiter = std::nullopt;
        }
    }
    // Best-effort drain of the FIN WR: the peer may already have torn
    // its side down (RNR/ENOMEM from post_send is surfaced by fail(),
    // which drain_all() then throws). Half-close completing late is
    // fine; the peer learns of the close via the liveness fd instead.
    try {
        co_await drain_all();
    } catch (...) {
        if (!_fin_sent) {
            throw;
        }
    }
}

bool rdma_endpoint::poll_cq_and_dispatch() {
    if (_destroyed_qp) {
        return false;
    }
    const verbs* v = verbs_provider();
    const int batch = int(_dev->cqe_poll_batch());
    std::vector<ibv_wc> wcs(batch);
    bool did_work = false;

    // brpc's PollCq order: drain recv CQ, then send CQ (each poll
    // loop). Event mode additionally re-arms before returning; the
    // watcher re-arms (rdma_device::channel_watch_loop), so here we
    // only poll.
    cq_h cq = _dev->polling_mode() ? _res.polling_cq : _res.recv_cq;
    while (true) {
        int n = v->poll_cq(cq, batch, wcs.data());
        if (n <= 0) {
            break;
        }
        did_work = true;
        for (int i = 0; i < n; ++i) {
            if (wcs[i].status != IBV_WC_SUCCESS) {
                fail(ECONNRESET);
                return did_work;
            }
            switch (wcs[i].opcode) {
            case IBV_WC_RECV:
                on_recv_wc(wcs[i]);
                break;
            case IBV_WC_SEND:
                on_send_wc(wcs[i]);
                break;
            default:
                break;
            }
        }
        if (_dev->polling_mode()) {
            break; // poll mode: one pass per reactor tick
        }
    }

    if (!_dev->polling_mode()) {
        cq = _res.send_cq;
        while (true) {
            int n = v->poll_cq(cq, batch, wcs.data());
            if (n <= 0) {
                break;
            }
            did_work = true;
            for (int i = 0; i < n; ++i) {
                if (wcs[i].status != IBV_WC_SUCCESS) {
                    fail(ECONNRESET);
                    return did_work;
                }
                if (wcs[i].opcode == IBV_WC_SEND) {
                    on_send_wc(wcs[i]);
                }
            }
        }
    }
    return did_work;
}

void rdma_endpoint::on_send_wc(ibv_wc& wc) {
    if (wc.wr_id == 0) {
        // Pure-IMM completion: restore the imm window.
        ++_sq_imm_window_size;
        // A pending shutdown_output() may be blocked in its retry loop
        // waiting for imm window room.
        if (!_send_pending.empty() || _sq_window_size != _local_window_capacity) {
            wake_flush();
        }
        send_ack_if_due();
        return;
    }
    uint16_t batch = uint16_t(wc.wr_id);
    for (uint16_t i = 0; i < batch; ++i) {
        // Release pool block(s) and the WR's direct-path MR pins.
        for (mr_h mr : _sbuf[_sq_sent].mrs) {
            unpin_direct_mr(mr);
        }
        _sbuf[_sq_sent] = inflight{};
        if (++_sq_sent == _sq_size - reserved_wr_num) {
            _sq_sent = 0;
        }
    }
    _sq_window_size += batch;
    if (_sq_window_size == _local_window_capacity && _send_pending.empty()) {
        wake_flush();
    }
    post_more_sends();
    if (_send_pending.empty()) {
        // flush() (post-drain semantics) is satisfied now. drain_all()
        // re-checks its stricter condition after the wake and re-arms
        // if unmet -- a spurious wake costs one loop iteration.
        wake_flush();
    }
    wake_window();
}

void rdma_endpoint::on_recv_wc(ibv_wc& wc) {
    ep_log.debug("recv wc: ep={} byte_len={} imm={} flags={}",
                 static_cast<void*>(this), wc.byte_len,
                 (wc.wc_flags & IBV_WC_WITH_IMM) ? imm_ntoh(wc.imm_data) : 0,
                 wc.wc_flags);
    if (wc.byte_len > 0) {
        // Zero-copy staging: hand the RQ ring block itself to the
        // consumer. The slot stays unposted until get() returns the
        // block (see recv_consume), which re-borrows, re-posts and
        // returns the credit to the peer. This bounds in-flight
        // registered memory to the ring and gives the connection
        // TCP-like end-to-end flow control: a slow consumer stops
        // returning credits, the sender's window closes.
        //
        // The block is released here (borrowed in post_recv_ring, held
        // by _rbuf): zero extra pool pressure from receiving.
        _recv_staging.push_back(std::move(_rbuf[_rq_received]));
        _recv_staging.back().trim(wc.byte_len);
    } else {
        // Pure-IMM (credit-only) receive: no payload to stage, so the
        // slot can be re-posted immediately (no consumer dependency).
        // recv_consume with zero length does exactly that and adds no
        // credit.
        recv_consume(0);
    }
    if (wc.wc_flags & IBV_WC_WITH_IMM) {
        uint32_t host = imm_ntoh(wc.imm_data);
        if (imm_is_fin(host)) {
            _peer_fin = true;
        }
        uint32_t acks = imm_ack_count(host);
        if (acks) {
            _remote_rq_window_size = uint16_t(std::min<uint32_t>(
                _remote_rq_window_size + acks, _local_window_capacity));
            // Credits reopen the remote window: wake BOTH drain drivers.
            // put() waits on the window; but with large negotiated blocks
            // a whole message fits into the open staging block, so put()
            // returns without suspending and flush() is the suspended
            // driver. Waking only the window waiter loses that wake and
            // deadlocks both sides once in-flight data exceeds the
            // window (nothing else would ever call post_more_sends).
            wake_window();
            wake_flush();
        }
    }
    // The RQ slot is NOT replenished here (see above): _rbuf[slot] is
    // either owned by staging or released. post_recv_ring reposts it
    // from get() once the consumer is done with the payload.
    if (++_rq_received == _rq_size) {
        _rq_received = 0;
    }
    // Do not account a receive credit yet: the peer's window only
    // reopens when the application consumes the payload (recv_consume).

    wake_get();
}

void rdma_endpoint::wake_get() {
    if (_get_waiter) {
        auto p = std::move(*_get_waiter);
        _get_waiter = std::nullopt;
        p.set_value(temporary_buffer<char>()); // sentinel wake
    }
}

void rdma_endpoint::wake_window() {
    if (_window_waiter) {
        auto p = std::move(*_window_waiter);
        _window_waiter = std::nullopt;
        p.set_value();
    }
}

void rdma_endpoint::wake_flush() {
    if (_flush_waiter) {
        auto p = std::move(*_flush_waiter);
        _flush_waiter = std::nullopt;
        p.set_value();
    }
}

void rdma_endpoint::fail(int err) {
    if (_failed) {
        return;
    }
    _failed = true;
    // Resolve all waiters with the error.
    if (_get_waiter) {
        auto p = std::move(*_get_waiter);
        _get_waiter = std::nullopt;
        p.set_exception(std::system_error(err, std::system_category(), "rdma: connection failed"));
    }
    wake_window();
    wake_flush();
    // Failure with pending puts: their co_await will observe _failed
    // and throw.
    release_all_mr_pins();
    _dev->unregister_endpoint(this);
    // Signal the peer (closing the liveness fd) so ITS death watch
    // fires instead of waiting for our own destruction.
    _tcp.close();
}

} // namespace seastar::net::rdma

#endif // SEASTAR_HAVE_RDMA
