/*
 * Copyright 2026 Seastar contributors
 */

//
// rdma-verbs.hh: the single seam between seastar's RDMA stack and
// libibverbs.
//
//
// ibv_post_send(), ibv_poll_cq() and ibv_req_notify_cq() are `static
// inline` wrappers in <infiniband/verbs.h> which dispatch through
// qp/cq->context->ops, so they cannot be interposed at link time. To
// keep the rest of the RDMA stack testable without hardware, all verbs
// entry points are funneled through an explicit function table
// (`verbs`). The real table is provided by rdma-verbs.cc; unit tests
// install a loopback mock via set_verbs_provider_for_test() before
// the engine starts.
//
// This is the ONLY file in the seastar tree that is allowed to include
// <infiniband/verbs.h>.
// 

#pragma once
#ifdef SEASTAR_HAVE_RDMA

#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdint>
#include <seastar/core/sstring.hh>

namespace seastar::net::rdma {

// Opaque-ish handle aliases: reuse the verbs types inside the table so
// the real provider needs no glue. Mock providers provide their own
// fake objects cast to these types.
using device_desc = ibv_device*;
using context_h = ibv_context*;
using pd_h = ibv_pd*;
using cq_h = ibv_cq*;
using qp_h = ibv_qp*;
using mr_h = ibv_mr*;
using comp_channel_h = ibv_comp_channel*;

// A selected GID + port, produced by enumerate_devices().
struct gid_selection {
    sstring device_name;
    uint8_t port = 0;
    uint16_t lid = 0;
    int gid_index = -1;
    ibv_gid gid{};
};

// Parameters for creating a QP (mirrors the subset of ibv_qp_init_attr
// we use; kept explicit so the mock can validate).
struct qp_init_desc {
    cq_h send_cq;
    cq_h recv_cq;
    uint32_t max_send_wr;
    uint32_t max_recv_wr;
    uint32_t max_send_sge;
    uint32_t max_recv_sge = 1;
    // IBV_QPT_RC only.
    int qp_type = IBV_QPT_RC;
};

// Subset of ibv_qp_attr we program (RESET->INIT->RTR->RTS).
struct qp_attr_desc {
    int qp_state;
    int pkey_index = 0;
    uint8_t port_num = 0;
    int qp_access_flags = 0;
    int path_mtu = 0;
    // QP numbers are 24-bit on the wire but kept 32-bit: real HCAs
    // routinely hand out QPNs >= 2^16, and truncation silently
    // addresses a wrong/nonexistent QP (observed on mlx5).
    uint32_t dest_qp_num = 0;
    uint32_t rq_psn = 0;
    uint32_t sq_psn = 0;
    int max_dest_rd_atomic = 0;
    int max_rd_atomic = 0;
    int min_rnr_timer = 0;
    int retry_cnt = 0;
    int rnr_retry = 0;
    int timeout = 0;
    // Address handle (RTR).
    ibv_gid dgid{};
    int sgid_index = 0;
    int hop_limit = 0;
    uint16_t dlid = 0;
    uint8_t ah_port_num = 0;
};

// Mask bits, mirroring ibv_qp_attr_mask values so the mock can check
// transition legality without duplicating verbs.h constants.
enum qp_attr_mask_bits : int {
    QP_STATE = IBV_QP_STATE,
    QP_PKEY_INDEX = IBV_QP_PKEY_INDEX,
    QP_PORT = IBV_QP_PORT,
    QP_ACCESS_FLAGS = IBV_QP_ACCESS_FLAGS,
    QP_PATH_MTU = IBV_QP_PATH_MTU,
    QP_MIN_RNR_TIMER = IBV_QP_MIN_RNR_TIMER,
    QP_AV = IBV_QP_AV,
    QP_MAX_DEST_RD_ATOMIC = IBV_QP_MAX_DEST_RD_ATOMIC,
    QP_DEST_QPN = IBV_QP_DEST_QPN,
    QP_RQ_PSN = IBV_QP_RQ_PSN,
    QP_RNR_RETRY = IBV_QP_RNR_RETRY,
    QP_RETRY_CNT = IBV_QP_RETRY_CNT,
    QP_TIMEOUT = IBV_QP_TIMEOUT,
    QP_SQ_PSN = IBV_QP_SQ_PSN,
    QP_MAX_QP_RD_ATOMIC = IBV_QP_MAX_QP_RD_ATOMIC,
};

// A send work request, minus the linked-list plumbing. The endpoint
// hands a single desc to post_send().
struct send_wr_desc {
    uint64_t wr_id = 0;
    // IBV_WR_SEND_WITH_IMM is the only opcode used by this stack.
    bool with_imm = true;
    uint32_t imm_data = 0; // network byte order (as in ibv_send_wr)
    const ibv_sge* sg_list = nullptr;
    int num_sge = 0;
    bool solicited = false;
    bool signaled = false;
};

struct recv_wr_desc {
    const ibv_sge* sg_list = nullptr;
    int num_sge = 1;
};

// Device capabilities needed for sizing.
struct device_caps {
    int max_sge = 0;
    int max_qp_wr = 0;
    int max_cqe = 0;
    int gid_tbl_len = 0;
};

//
// The verbs function table. Semantics match the corresponding ibv_*
// functions: 0 (or non-null) on success, errno on failure.
//
struct verbs {
     // -- enumeration / lifecycle --------------------------------------
     // Enumerate devices, pick the one matching `device_name` ("" = first
     // with an ACTIVE port), return its selection. Returns false and sets
     // errno if none. Also returns caps of the selected device.
     bool (*enumerate_devices)(const char* device_name, uint8_t port,
                              gid_selection& out, device_caps& caps);
    context_h (*open_device)(const gid_selection& sel);
    void (*close_device)(context_h);

    pd_h (*alloc_pd)(context_h);
    void (*dealloc_pd)(pd_h);

    cq_h (*create_cq)(context_h, int cqe, comp_channel_h channel, int comp_vector);
    int (*destroy_cq)(cq_h);

    comp_channel_h (*create_comp_channel)(context_h);
    int (*destroy_comp_channel)(comp_channel_h);
    int (*get_comp_channel_fd)(comp_channel_h);

    qp_h (*create_qp)(pd_h, const qp_init_desc&);
    int (*destroy_qp)(qp_h);
    int (*modify_qp)(qp_h, const qp_attr_desc&, int mask);

    mr_h (*reg_mr)(pd_h, void* addr, size_t size, int access);
    int (*dereg_mr)(mr_h);

    // -- data plane ----------------------------------------------------
    int (*post_send)(qp_h, const send_wr_desc& wr);
    int (*post_recv)(qp_h, const recv_wr_desc& wr);
    int (*poll_cq)(cq_h, int num_wc, ibv_wc* wc);
    int (*req_notify_cq)(cq_h, bool solicited_only);
    // Returns 0 on success, EAGAIN when no event queued.
    int (*get_cq_event)(comp_channel_h, cq_h& cq);
    void (*ack_cq_events)(cq_h, unsigned n);
};

// The active provider. The first call installs the real table; never
// returns nullptr.
const verbs* verbs_provider() noexcept;

// Test-only injection point. Must be called before any rdma code runs
// (i.e. before the engine starts). Passing nullptr restores the real
// table.
void set_verbs_provider_for_test(const verbs* v) noexcept;

} // namespace seastar::net::rdma

#else // SEASTAR_HAVE_RDMA
// Without RDMA support everything compiles to nothing; the registration
// in reactor.cc is also compiled out.
namespace seastar::net::rdma {
inline constexpr bool rdma_supported = false;
}

#endif // SEASTAR_HAVE_RDMA
