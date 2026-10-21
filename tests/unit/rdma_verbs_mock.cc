/*
 * Copyright 2026 Seastar contributors
 */

#include "rdma_verbs_mock.hh"

#ifdef SEASTAR_HAVE_RDMA

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <sys/eventfd.h>
#include <unistd.h>

namespace seastar::net::rdma {

namespace {

// ---------------------------------------------------------------------------
// mock object layouts
// ---------------------------------------------------------------------------

// All mock handles are pointers to these structs cast through the
// verbs handle aliases. The production code reads a few fields through
// the REAL verbs.h layouts (ibv_qp::qp_num at offset 52, ibv_mr::lkey
// at offset 32), so the mock structs embed a matching header and keep
// their own state after it.
constexpr int mock_qp_state_reset = 0;
constexpr int mock_qp_state_init = 1;
constexpr int mock_qp_state_rtr = 2;
constexpr int mock_qp_state_rts = 3;

// Header matching struct ibv_qp up to and including qp_num.
struct ibv_qp_header {
    void* context = nullptr;      // ibv_qp::context
    void* qp_context = nullptr;
    void* pd = nullptr;
    void* send_cq = nullptr;
    void* recv_cq = nullptr;
    void* srq = nullptr;
    uint32_t handle = 0;
    uint32_t qp_num = 0;
};
static_assert(offsetof(ibv_qp, qp_num) == offsetof(ibv_qp_header, qp_num));

// Header matching struct ibv_mr up to and including rkey.
struct ibv_mr_header {
    void* context = nullptr;
    void* pd = nullptr;
    void* addr = nullptr;
    size_t length = 0;
    uint32_t handle = 0;
    uint32_t lkey = 0;
    uint32_t rkey = 0;
};
static_assert(offsetof(ibv_mr, lkey) == offsetof(ibv_mr_header, lkey));

struct mock_cq {
    uint64_t id;
    std::vector<ibv_wc> pending;    // completed but not polled
    bool notified = false;          // req_notify armed
    bool solicited_only = false;
    comp_channel_h channel = nullptr; // may be null (poll mode)
    uint64_t unacked = 0;
};

struct mock_qp {
    ibv_qp_header hdr;              // must be first (ibv_qp layout)
    int state = mock_qp_state_reset;
    mock_cq* send_cq = nullptr;
    mock_cq* recv_cq = nullptr;
    uint32_t max_send_wr = 0, max_recv_wr = 0;
    uint32_t outstanding_send = 0;  // posted, not completed
    uint32_t posted_recv = 0;       // receive WRs currently on the RQ
    mock_qp* peer = nullptr;
    uint32_t max_send_sge = 16;
    // Posted receive WRs in FIFO order: the mock copies send payloads
    // into the next posted buffer (what real hardware DMAs into).
    std::vector<ibv_sge> recv_sges;

    uint32_t& qp_num() { return hdr.qp_num; }
    const uint32_t& qp_num() const { return hdr.qp_num; }
};

struct mock_mr {
    ibv_mr_header hdr;              // must be first (ibv_mr layout)
    char* start = nullptr;
    size_t size = 0;

    uint32_t& lkey() { return hdr.lkey; }
};

struct mock_channel {
    int efd; // eventfd
    std::vector<cq_h> events; // queued CQs (channel implied)
};

struct mock_pd {
    uint64_t id;
};

struct mock_context {
    std::string name;
};

// ---------------------------------------------------------------------------
// global mock state
// ---------------------------------------------------------------------------

struct mock_state {
    std::mutex mtx;
    uint32_t next_qp_num = 1;
    uint32_t next_lkey = 0x10000;
    std::map<uintptr_t, mock_qp*> qps;           // qp_h -> mock
    std::map<uintptr_t, mock_cq*> cqs;           // cq_h -> mock
    std::map<uintptr_t, mock_mr*> mrs;           // mr_h -> mock
    std::map<uintptr_t, mock_channel*> channels; // comp_channel_h -> mock
    std::map<uint32_t, mock_qp*> qp_by_num;      // qp_num -> mock
    std::map<uintptr_t, mock_pd*> pds;
    std::map<std::string, int> fail_next_op;

    // lkey lookup: address -> mr (regions are non-overlapping).
    struct region {
        char* start;
        size_t size;
        uint32_t lkey;
        mock_mr* mr;
    };
    std::vector<region> regions;

    ~mock_state() {
        for (auto& [k, v] : qps) delete v;
        for (auto& [k, v] : cqs) delete v;
        // MRs do NOT own the memory they register (pool regions are
        // operator-new'd by rdma_buffer_pool; direct-path MRs cover
        // caller buffers). Only drop the bookkeeping.
        for (auto& [k, v] : mrs) delete v;
        for (auto& [k, v] : channels) {
            ::close(v->efd);
            delete v;
        }
        for (auto& [k, v] : pds) delete v;
    }

    // Total ibv_reg_mr calls served (tests assert on the count to
    // prove the endpoint's MR cache avoids re-registration).
    uint64_t reg_mr_calls = 0;
    // Subset covering only non-pool-sized registrations, i.e. the
    // direct/zcopy path (pool region growth is background noise for
    // those assertions).
    uint64_t direct_reg_mr_calls = 0;

    bool should_fail(const char* op) {
        auto it = fail_next_op.find(op);
        if (it == fail_next_op.end()) {
            return false;
        }
        int err = it->second;
        fail_next_op.erase(it);
        errno = err;
        return true;
    }
};

mock_state& state() {
    static mock_state st;
    return st;
}

mock_qp* as_qp(qp_h h) { return reinterpret_cast<mock_qp*>(h); }
mock_cq* as_cq(cq_h h) { return reinterpret_cast<mock_cq*>(h); }
mock_channel* as_channel(comp_channel_h h) {
    return reinterpret_cast<mock_channel*>(h);
}
mock_pd* as_pd(pd_h h) { return reinterpret_cast<mock_pd*>(h); }

// Find the lkey valid for [addr, addr+len).
bool lookup_lkey(uint64_t addr, size_t len, uint32_t& lkey) {
    for (auto& r : state().regions) {
        if (addr >= uint64_t(r.start) && addr + len <= uint64_t(r.start) + r.size) {
            lkey = r.lkey;
            return true;
        }
    }
    return false;
}

void push_cq_event(mock_cq* cq) {
    if (cq->channel && cq->notified) {
        mock_channel* ch = as_channel(cq->channel);
        ch->events.emplace_back(reinterpret_cast<cq_h>(cq));
        cq->notified = false; // consumed the arm
        uint64_t one = 1;
        ::write(ch->efd, &one, sizeof(one));
    }
}

// ---------------------------------------------------------------------------
// provider implementation
// ---------------------------------------------------------------------------
bool mock_enumerate_devices(const char* device_name, uint8_t port,
                            gid_selection& out, device_caps& caps) {
    std::lock_guard<std::mutex> g(state().mtx);
    if (state().should_fail("enumerate_devices")) {
        return false;
    }
    // One virtual device "mock0", one port, RoCE-style GID.
    if (device_name && *device_name && std::string(device_name) != "mock0") {
        errno = ENODEV;
        return false;
    }
    out.device_name = "mock0";
    out.port = port ? port : 1;
    out.lid = 0;
    out.gid_index = 0;
    std::memset(&out.gid, 0, sizeof(out.gid));
    out.gid.raw[0] = 0xfe;
    out.gid.raw[1] = 0x80;
    out.gid.raw[8] = 0x02;
    out.gid.raw[15] = 0x01;
    caps.max_sge = 8;
    caps.max_qp_wr = 1024;
    caps.max_cqe = 4096;
    caps.gid_tbl_len = 1;
    return true;
}

context_h mock_open_device(const gid_selection& sel) {
    std::lock_guard<std::mutex> g(state().mtx);
    if (state().should_fail("open_device")) {
        return nullptr;
    }
    return reinterpret_cast<context_h>(new mock_context{sel.device_name});
}

void mock_close_device(context_h ctx) {
    delete reinterpret_cast<mock_context*>(ctx);
}

pd_h mock_alloc_pd(context_h) {
    std::lock_guard<std::mutex> g(state().mtx);
    if (state().should_fail("alloc_pd")) {
        return nullptr;
    }
    return reinterpret_cast<pd_h>(new mock_pd{});
}

void mock_dealloc_pd(pd_h pd) {
    delete as_pd(pd);
}

cq_h mock_create_cq(context_h, int cqe, comp_channel_h channel, int) {
    std::lock_guard<std::mutex> g(state().mtx);
    if (state().should_fail("create_cq")) {
        return nullptr;
    }
    auto* cq = new mock_cq{};
    cq->id = state().next_qp_num++; // unique
    cq->channel = channel;
    (void)cqe;
    state().cqs[reinterpret_cast<uintptr_t>(cq)] = cq;
    return reinterpret_cast<cq_h>(cq);
}

int mock_destroy_cq(cq_h h) {
    std::lock_guard<std::mutex> g(state().mtx);
    auto it = state().cqs.find(reinterpret_cast<uintptr_t>(h));
    if (it == state().cqs.end()) {
        return EINVAL;
    }
    delete it->second;
    state().cqs.erase(it);
    return 0;
}

comp_channel_h mock_create_comp_channel(context_h) {
    std::lock_guard<std::mutex> g(state().mtx);
    if (state().should_fail("create_comp_channel")) {
        return nullptr;
    }
    int efd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (efd < 0) {
        return nullptr;
    }
    auto* ch = new mock_channel{efd, {}};
    state().channels[reinterpret_cast<uintptr_t>(ch)] = ch;
    return reinterpret_cast<comp_channel_h>(ch);
}

int mock_destroy_comp_channel(comp_channel_h h) {
    std::lock_guard<std::mutex> g(state().mtx);
    auto it = state().channels.find(reinterpret_cast<uintptr_t>(h));
    if (it == state().channels.end()) {
        return EINVAL;
    }
    ::close(it->second->efd);
    delete it->second;
    state().channels.erase(it);
    return 0;
}

int mock_get_comp_channel_fd(comp_channel_h h) {
    return as_channel(h)->efd;
}

qp_h mock_create_qp(pd_h, const qp_init_desc& d) {
    std::lock_guard<std::mutex> g(state().mtx);
    if (state().should_fail("create_qp")) {
        return nullptr;
    }
    auto* qp = new mock_qp{};
    qp->qp_num() = state().next_qp_num++;
    qp->send_cq = as_cq(d.send_cq);
    qp->recv_cq = as_cq(d.recv_cq);
    qp->max_send_wr = d.max_send_wr;
    qp->max_recv_wr = d.max_recv_wr;
    qp->max_send_sge = d.max_send_sge;
    state().qps[reinterpret_cast<uintptr_t>(qp)] = qp;
    state().qp_by_num[qp->qp_num()] = qp;
    return reinterpret_cast<qp_h>(qp);
}

int mock_destroy_qp(qp_h h) {
    std::lock_guard<std::mutex> g(state().mtx);
    auto it = state().qps.find(reinterpret_cast<uintptr_t>(h));
    if (it == state().qps.end()) {
        return EINVAL;
    }
    // Detach the surviving side of a pairing: its back-pointer must not
    // dangle after we delete this QP (real hardware removes the
    // association when the QP leaves RTS).
    if (it->second->peer) {
        if (it->second->peer->peer == it->second) {
            it->second->peer->peer = nullptr;
        }
        it->second->peer = nullptr;
    }
    state().qp_by_num.erase(it->second->qp_num());
    delete it->second;
    state().qps.erase(it);
    return 0;
}

int mock_modify_qp(qp_h h, const qp_attr_desc& d, int mask) {
    std::lock_guard<std::mutex> g(state().mtx);
    mock_qp* qp = as_qp(h);

    // Transition legality (RESET->INIT->RTR->RTS, anything->RESET).
    auto has = [&](int bit) { return mask & bit; };
    if (has(QP_STATE)) {
        switch (d.qp_state) {
        case mock_qp_state_reset:
            // Real hardware flushes the SQ/RQ on RESET: outstanding and
            // posted work requests are dropped and the QP disconnects.
            // Without this, a pool-recycled QP keeps its stale peer
            // pointer, posted_recv count and recv_sges FIFO (pointing
            // at the previous connection's pool blocks), contaminating
            // the next renter: recv posts land behind stale entries,
            // incoming sends are copied into reused blocks, and the
            // counter/queue drift crashes front() on an empty vector.
            qp->state = mock_qp_state_reset;
            qp->posted_recv = 0;
            qp->recv_sges.clear();
            qp->outstanding_send = 0;
            if (qp->peer) {
                if (qp->peer->peer == qp) {
                    qp->peer->peer = nullptr;
                }
                qp->peer = nullptr;
            }
            return 0;
        case mock_qp_state_init:
            if (qp->state != mock_qp_state_reset &&
                qp->state != mock_qp_state_init) {
                errno = EINVAL;
                return -1;
            }
            if (!has(QP_PORT) || !has(QP_ACCESS_FLAGS) || !has(QP_PKEY_INDEX)) {
                errno = EINVAL;
                return -1;
            }
            break;
        case mock_qp_state_rtr:
            if (qp->state != mock_qp_state_init) {
                errno = EINVAL;
                return -1;
            }
            if (!has(QP_PATH_MTU) || !has(QP_DEST_QPN) || !has(QP_AV)) {
                errno = EINVAL;
                return -1;
            }
            break;
        case mock_qp_state_rts:
            if (qp->state != mock_qp_state_rtr) {
                errno = EINVAL;
                return -1;
            }
            if (!has(QP_SQ_PSN)) {
                errno = EINVAL;
                return -1;
            }
            break;
        default:
            errno = EINVAL;
            return -1;
        }
qp->state = d.qp_state;
        if (qp->state == mock_qp_state_rtr) {
            // Resolve the peer via dest_qp_num.
            auto it = state().qp_by_num.find(d.dest_qp_num);
            if (it != state().qp_by_num.end()) {
                qp->peer = it->second;
                // Pairing is bidirectional.
                if (it->second->peer == nullptr) {
                    it->second->peer = qp;
                }
            }
        }
    }
    return 0;
}

mr_h mock_reg_mr(pd_h, void* addr, size_t size, int) {
    if (state().should_fail("reg_mr")) {
        return nullptr;
    }
    auto* mr = new mock_mr{};
    mr->start = static_cast<char*>(addr);
    mr->size = size;
    {
        std::lock_guard<std::mutex> g(state().mtx);
        state().reg_mr_calls++;
        // Non-pool registration: pool regions come in exactly the
        // class geometry sizes (128*8K, 64*64K, 4*2M); anything else
        // is a direct-path (zcopy) registration. Tests that assert
        // MR-cache effectiveness only care about the latter (pool
        // growth is legitimate background traffic).
        if (size != (128u << 10) && size != (4u << 20) && size != (8u << 20)) {
            state().direct_reg_mr_calls++;
        }
        mr->lkey() = state().next_lkey++;
        mr->hdr.rkey = mr->lkey();
        state().mrs[reinterpret_cast<uintptr_t>(mr)] = mr;
        state().regions.push_back({mr->start, mr->size, mr->lkey(), mr});
    }
    return reinterpret_cast<mr_h>(mr);
}

int mock_dereg_mr(mr_h h) {
    std::lock_guard<std::mutex> g(state().mtx);
    auto it = state().mrs.find(reinterpret_cast<uintptr_t>(h));
    if (it == state().mrs.end()) {
        return EINVAL;
    }
    // Match by the MR itself, not by start address: the same VA may be
    // re-registered after the previous MR was deregistered (rpc
    // reuses snd_buf storage), making start-address matching ambiguous.
    auto& regions = state().regions;
    for (size_t i = 0; i < regions.size(); ++i) {
        if (regions[i].mr == it->second) {
            regions.erase(regions.begin() + i);
            break;
        }
    }
    delete it->second;
    state().mrs.erase(it);
    return 0;
}

int mock_post_send(qp_h h, const send_wr_desc& d) {
    std::lock_guard<std::mutex> g(state().mtx);
    mock_qp* qp = as_qp(h);
    if (state().should_fail("post_send")) {
        return -1;
    }
    if (qp->state != mock_qp_state_rts) {
        errno = EINVAL;
        return -1;
    }
    if (qp->outstanding_send >= qp->max_send_wr) {
        errno = ENOMEM;
        return -1;
    }

    // Validate every SGE against registered MRs. The lkey names the MR:
    // several live MRs may overlap the same VA (a re-registration over
    // still-pinned memory), so the check is "this lkey's MR covers the
    // SGE", not "some MR covers the SGE".
    for (int i = 0; i < d.num_sge; ++i) {
        bool valid = false;
        for (auto& r : state().regions) {
            if (r.lkey == d.sg_list[i].lkey &&
                d.sg_list[i].addr >= uint64_t(r.start) &&
                d.sg_list[i].addr + d.sg_list[i].length <= uint64_t(r.start) + r.size) {
                valid = true;
                break;
            }
        }
        if (!valid) {
            errno = EINVAL; // unregistered memory or bad lkey
            return -1;
        }
    }
    if (d.num_sge > int(qp->max_send_sge)) {
        errno = EINVAL;
        return -1;
    }

    qp->outstanding_send++;

    // Deliver a RECV completion to the peer, copying the payload into
    // its next posted receive buffer (what real hardware DMAs).
    // (The drop path below decrements outstanding_send again.)
    mock_qp* peer = qp->peer;
    if (peer && peer->recv_cq && peer->posted_recv > 0 &&
        !peer->recv_sges.empty()) {
        peer->posted_recv--;
        ibv_wc wc{};
        wc.wr_id = 0;
        wc.status = IBV_WC_SUCCESS;
        wc.opcode = IBV_WC_RECV;
        wc.byte_len = 0;
        // Gather into the posted SGEs (v1 posts one SGE per WR).
        ibv_sge rsg = peer->recv_sges.front();
        peer->recv_sges.erase(peer->recv_sges.begin());
        char* dst = reinterpret_cast<char*>(rsg.addr);
        size_t dst_room = rsg.length;
        for (int i = 0; i < d.num_sge; ++i) {
            size_t n = std::min<size_t>(d.sg_list[i].length, dst_room);
            std::memcpy(dst, reinterpret_cast<const char*>(d.sg_list[i].addr), n);
            dst += n;
            dst_room -= n;
            wc.byte_len += d.sg_list[i].lengtH;
        }
        if (d.with_imm) {
            wc.wc_flags |= IBV_WC_WITH_IMM;
            wc.imm_data = d.imm_data; // network order, as on the wire
        }
        peer->recv_cq->pending.push_back(wc);
        push_cq_event(peer->recv_cq);
    } else if (peer) {
        // No posted receive: real hardware would RNR-Nak. Since both
        // sides pre-post their full RQ and replenish on every CQE, a
        // miss here means a bug (e.g. a QP that kept stale RQ state
        // from a previous connection) -- surface it, don't hide it.
        errno = ENOMEM;
        qp->outstanding_send--;
        return -1;
    }

    // Own SEND completion (SIGNALED only, matching suppression).
    if (d.signaled) {
        ibv_wc wc{};
        wc.wr_id = d.wr_id;
        wc.status = IBV_WC_SUCCESS;
        wc.opcode = IBV_WC_SEND;
        qp->send_cq->pending.push_back(wc);
        push_cq_event(qp->send_cq);
    }
    // UNSIGNALED sends complete implicitly when a later signaled WR
    // completes; the endpoint's wr_id batching models that.
    if (peer && !(peer->recv_cq && peer->posted_recv > 0 &&
                  !peer->recv_sges.empty())) {
        // The send was dropped on the floor (no posted receive): do not
        // count it as completed.
        return 0;
    }
    qp->outstanding_send--;
    return 0;
}

int mock_post_recv(qp_h h, const recv_wr_desc& d) {
    std::lock_guard<std::mutex> g(state().mtx);
    mock_qp* qp = as_qp(h);
    if (qp->state == mock_qp_state_reset) {
        errno = EINVAL;
        return -1;
    }
    if (qp->posted_recv >= qp->max_recv_wr) {
        errno = ENOMEM;
        return -1;
    }
    // Validate the receive SGE is registered memory.
    if (d.num_sge > 0) {
        uint32_t lkey = 0;
        if (!lookup_lkey(d.sg_list[0].addr, d.sg_list[0].length, lkey) ||
            lkey != d.sg_list[0].lkey) {
            errno = EINVAL;
            return -1;
        }
    }
    for (int i = 0; i < d.num_sge; ++i) {
        qp->recv_sges.push_back(d.sg_list[i]);
    }
    qp->posted_recv++;
    return 0;
}

int mock_poll_cq(cq_h h, int num_wc, ibv_wc* wc) {
    std::lock_guard<std::mutex> g(state().mtx);
    mock_cq* cq = as_cq(h);
    int n = 0;
    while (n < num_wc && !cq->pending.empty()) {
        wc[n++] = cq->pending.front();
        cq->pending.erase(cq->pending.begin());
    }
    return n;
}

int mock_req_notify_cq(cq_h h, bool solicited_only) {
    std::lock_guard<std::mutex> g(state().mtx);
    mock_cq* cq = as_cq(h);
    cq->notified = true;
    cq->solicited_only = solicited_only;
    // If completions are already pending, fire immediately (real
    // hardware semantics: an armed CQ with pending CQEs generates an
    // event).
    if (!cq->pending.empty()) {
        push_cq_event(cq);
    }
    return 0;
}

int mock_get_cq_event(comp_channel_h h, cq_h& cq) {
    std::lock_guard<std::mutex> g(state().mtx);
    mock_channel* ch = as_channel(h);
    if (ch->events.empty()) {
        errno = EAGAIN;
        return -1;
    }
    cq = ch->events.front();
    ch->events.erase(ch->events.begin());
    // Drain one eventfd tick, mirroring the real ibv_get_cq_event
    // (which reads the channel fd): without this the counter never
    // drops and the watcher's readable() spins.
    uint64_t one = 0;
    (void)::read(ch->efd, &one, sizeof(one));
    return 0;
}

void mock_ack_cq_events(cq_h h, unsigned) {
    // Nothing to do: the event was already dequeued by get_cq_event.
    (void)h;
}

const verbs mock_verbs = {
    mock_enumerate_devices,
    mock_open_device,
    mock_close_device,
    mock_alloc_pd,
    mock_dealloc_pd,
    mock_create_cq,
    mock_destroy_cq,
    mock_create_comp_channel,
    mock_destroy_comp_channel,
    mock_get_comp_channel_fd,
    mock_create_qp,
    mock_destroy_qp,
    mock_modify_qp,
    mock_reg_mr,
    mock_dereg_mr,
    mock_post_send,
    mock_post_recv,
    mock_poll_cq,
    mock_req_notify_cq,
    mock_get_cq_event,
    mock_ack_cq_events,
};

} // anonymous namespace

const verbs* install_verbs_mock() {
    const verbs* prev = verbs_provider();
    set_verbs_provider_for_test(&mock_verbs);
    return prev;
}

namespace rdma_mock {

size_t live_qps() { return state().qps.size(); }
size_t live_cqs() { return state().cqs.size(); }
size_t live_mrs() { return state().mrs.size(); }
uint64_t reg_mr_calls() { return state().reg_mr_calls; }
uint64_t direct_reg_mr_calls() { return state().direct_reg_mr_calls; }

void pair_qps(qp_h a, qp_h b) {
    std::lock_guard<std::mutex> g(state().mtx);
    as_qp(a)->peer = as_qp(b);
    as_qp(b)->peer = as_qp(a);
}

void fail_next(const std::string& op, int err) {
    state().fail_next_op[op] = err;
}

void clear_failures() {
    state().fail_next_op.clear();
}

void reset() {
    auto& st = state();
    std::lock_guard<std::mutex> g(st.mtx);
    for (auto& [k, v] : st.qps) delete v;
    for (auto& [k, v] : st.cqs) delete v;
    // MRs don't own the registered memory (see ~mock_state).
    for (auto& [k, v] : st.mrs) delete v;
    for (auto& [k, v] : st.channels) {
        ::close(v->efd);
        delete v;
    }
    for (auto& [k, v] : st.pds) delete v;
    st.qps.clear();
    st.qp_by_num.clear();
    st.cqs.clear();
    st.mrs.clear();
    st.channels.clear();
    st.pds.clear();
    st.regions.clear();
    st.fail_next_op.clear();
}

} // namespace rdma_mock

} // namespace seastar::net::rdma

#endif // SEASTAR_HAVE_RDMA

