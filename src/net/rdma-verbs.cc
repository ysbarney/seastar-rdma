/*
 * Copyright 2026 Seastar contributors
 */
// The real libibverbs provider behind rdma-verbs.hh. See the header
// for the rationale of the function-table seam.

#include "rdma-verbs.hh"

#ifdef SEASTAR_HAVE_RDMA

#include <atomic>
#include <cerrno>

namespace seastar::net::rdma {

namespace {

// ---- real-provider wrappers -------------------------------------------
//
// ibv_post_send/ibv_poll_cq/ibv_req_notify_cq are static inline in
// verbs.h; wrapping them in plain functions lets the table point at
// them while keeping the mock substitutable.

bool real_enumerate_devices(const char* device_name, uint8_t port,
                            gid_selection& out, device_caps& caps) {
    int num = 0;
    ibv_device** devices = ibv_get_device_list(&num);
    if (!devices || num == 0) {
        errno = ENODEV;
        return false;
    }
    bool found = false;
    gid_selection selection;
    for (int i = 0; i < num && !found; ++i) {
        ibv_context* ctx = ibv_open_device(devices[i]);
        if (!ctx) {
            continue;
        }
        ibv_port_attr attr;
        if (ibv_query_port(ctx, port, &attr) == 0 && attr.state == IBV_PORT_ACTIVE) {
            bool name_match = !device_name || !*device_name ||
                              sstring(device_name) == ibv_get_device_name(devices[i]);
            if (name_match && !found) {
                selection.device_name = ibv_get_device_name(devices[i]);
                selection.port = port;
                selection.lid = attr.lid;
                // Prefer the last valid GID (RoCEv2-style, IP-based);
                // mirrors brpc's FindRdmaGid.
                for (int g = attr.gid_tbl_len - 1; g >= 0; --g) {
                    ibv_gid gid;
                    if (ibv_query_gid(ctx, port, g, &gid) == 0 &&
                        gid.global.interface_id != 0) {
                        selection.gid_index = g;
                        selection.gid = gid;
                        break;
                    }
                }
                if (selection.gid_index >= 0) {
                    found = true;
                }
            }
        }
        ibv_close_device(ctx);
    }
    ibv_free_device_list(devices);
    if (!found) {
        errno = ENODEV;
        return false;
    }
    out = selection;

    // Re-open the chosen device for its capabilities. ibv_open_device()
    // takes the ibv_device*, so re-enumerate to find it by name.
    ibv_context* ctx = nullptr;
    ibv_device** devs2 = ibv_get_device_list(nullptr);
    if (devs2) {
        for (int i = 0; devs2[i]; ++i) {
            if (sstring(ibv_get_device_name(devs2[i])) == selection.device_name) {
                ctx = ibv_open_device(devs2[i]);
                break;
            }
        }
        ibv_free_device_list(devs2);
    }
    if (!ctx) {
        errno = ENODEV;
        return false;
    }
    ibv_device_attr dattr;
    int rc = ibv_query_device(ctx, &dattr);
    if (rc == 0) {
        caps.max_sge = dattr.max_sge;
        caps.max_qp_wr = dattr.max_qp_wr;
        caps.max_cqe = dattr.max_cqe;
        ibv_port_attr pattr;
        if (ibv_query_port(ctx, selection.port, &pattr) == 0) {
            caps.gid_tbl_len = pattr.gid_tbl_len;
        }
    }
    ibv_close_device(ctx);
    return rc == 0;
}

context_h real_open_device(const gid_selection& sel) {
    ibv_context* ctx = nullptr;
    ibv_device** devs = ibv_get_device_list(nullptr);
    if (devs) {
        for (int i = 0; devs[i]; ++i) {
            if (sstring(ibv_get_device_name(devs[i])) == sel.device_name) {
                ctx = ibv_open_device(devs[i]);
                break;
            }
        }
        ibv_free_device_list(devs);
    }
    return ctx;
}

void real_close_device(context_h ctx) {
    ibv_close_device(ctx);
}

pd_h real_alloc_pd(context_h ctx) {
    return ibv_alloc_pd(ctx);
}

void real_dealloc_pd(pd_h pd) {
    ibv_dealloc_pd(pd);
}

cq_h real_create_cq(context_h ctx, int cqe, comp_channel_h channel, int comp_vector) {
    return ibv_create_cq(ctx, cqe, nullptr, channel, comp_vector);
}

int real_destroy_cq(cq_h cq) {
    return ibv_destroy_cq(cq);
}

comp_channel_h real_create_comp_channel(context_h ctx) {
    return ibv_create_comp_channel(ctx);
}

int real_destroy_comp_channel(comp_channel_h ch) {
    return ibv_destroy_comp_channel(ch);
}

int real_get_comp_channel_fd(comp_channel_h ch) {
    return ch->fd;
}

qp_h real_create_qp(pd_h pd, const qp_init_desc& d) {
    ibv_qp_init_attr attr{};
    attr.send_cq = d.send_cq;
    attr.recv_cq = d.recv_cq;
    attr.cap.max_send_wr = d.max_send_wr;
    attr.cap.max_recv_wr = d.max_recv_wr;
    attr.cap.max_send_sge = d.max_send_sge;
    attr.cap.max_recv_sge = d.max_recv_sge;
    attr.qp_type = ibv_qp_type(d.qp_type);
    return ibv_create_qp(pd, &attr);
}

int real_destroy_qp(qp_h qp) {
    return ibv_destroy_qp(qp);
}

int real_modify_qp(qp_h qp, const qp_attr_desc& d, int mask) {
    ibv_qp_attr attr{};
    attr.qp_state = ibv_qp_state(d.qp_state);
    attr.pkey_index = d.pkey_index;
    attr.port_num = d.port_num;
    attr.qp_access_flags = d.qp_access_flags;
    attr.path_mtu = (ibv_mtu)d.path_mtu;
    attr.dest_qp_num = d.dest_qp_num;
    attr.rq_psn = d.rq_psn;
    attr.sq_psn = d.sq_psn;
    attr.max_dest_rd_atomic = d.max_dest_rd_atomic;
    attr.max_rd_atomic = d.max_rd_atomic;
    attr.min_rnr_timer = d.min_rnr_timer;
    attr.retry_cnt = d.retry_cnt;
    attr.rnr_retry = d.rnr_retry;
    attr.timeout = d.timeout;
    attr.ah_attr.grh.dgid = d.dgid;
    attr.ah_attr.grh.sgid_index = d.sgid_index;
    attr.ah_attr.grh.hop_limit = d.hop_limit;
    attr.ah_attr.dlid = d.dlid;
    attr.ah_attr.port_num = d.ah_port_num;
    attr.ah_attr.is_global = 1;
    return ibv_modify_qp(qp, &attr, (ibv_qp_attr_mask)mask);
}

mr_h real_reg_mr(pd_h pd, void* addr, size_t size, int access) {
    return ibv_reg_mr(pd, addr, size, access);
}

int real_dereg_mr(mr_h mr) {
    return ibv_dereg_mr(mr);
}

int real_post_send(qp_h qp, const send_wr_desc& d) {
    ibv_send_wr wr{};
    wr.wr_id = d.wr_id;
    wr.opcode = d.with_imm ? IBV_WR_SEND_WITH_IMM : IBV_WR_SEND;
    wr.imm_data = d.imm_data;
    wr.sg_list = const_cast<ibv_sge*>(d.sg_list);
    wr.num_sge = d.num_sge;
    if (d.solicited) {
        wr.send_flags |= IBV_SEND_SOLICITED;
    }
    if (d.signaled) {
        wr.send_flags |= IBV_SEND_SIGNALED;
    }
    ibv_send_wr* bad = nullptr;
    return ibv_post_send(qp, &wr, &bad);
}

int real_post_recv(qp_h qp, const recv_wr_desc& d) {
    ibv_recv_wr wr{};
    wr.sg_list = const_cast<ibv_sge*>(d.sg_list);
    wr.num_sge = d.num_sge;
    ibv_recv_wr* bad = nullptr;
    return ibv_post_recv(qp, &wr, &bad);
}

int real_poll_cq(cq_h cq, int num_wc, ibv_wc* wc) {
    return ibv_poll_cq(cq, num_wc, wc);
}

int real_req_notify_cq(cq_h cq, bool solicited_only) {
    return ibv_req_notify_cq(cq, solicited_only ? 1 : 0);
}

int real_get_cq_event(comp_channel_h ch, cq_h& cq) {
    ibv_cq* c = nullptr;
    void* ctx = nullptr;
    int rc = ibv_get_cq_event(ch, &c, &ctx);
    if (rc != 0) {
        // ibv_get_cq_event returns -1 and sets errno (EAGAIN when the
        // channel holds no pending event). Normalize to errno.
        return errno ? errno : EIO;
    }
    cq = c;
    return 0;
}

void real_ack_cq_events(cq_h cq, unsigned n) {
    ibv_ack_cq_events(cq, n);
}

const verbs real_verbs = {
    real_enumerate_devices,
    real_open_device,
    real_close_device,
    real_alloc_pd,
    real_dealloc_pd,
    real_create_cq,
    real_destroy_cq,
    real_create_comp_channel,
    real_destroy_comp_channel,
    real_get_comp_channel_fd,
    real_create_qp,
    real_destroy_qp,
    real_modify_qp,
    real_reg_mr,
    real_dereg_mr,
    real_post_send,
    real_post_recv,
    real_poll_cq,
    real_req_notify_cq,
    real_get_cq_event,
    real_ack_cq_events,
};

} // anonymous namespace

// The active table. The real table is installed at static-init time;
// a test may replace it via set_verbs_provider_for_test() before the
// engine starts (single-threaded by then, no synchronization needed).
static std::atomic<const verbs*> active_verbs{nullptr};

const verbs* verbs_provider() noexcept {
    const verbs* v = active_verbs.load(std::memory_order_acquire);
    if (!v) {
        v = &real_verbs;
        active_verbs.store(v, std::memory_order_release);
    }
    return v;
}

void set_verbs_provider_for_test(const verbs* v) noexcept {
    active_verbs.store(v, std::memory_order_release);
}

} // namespace seastar::net::rdma

#endif // SEASTAR_HAVE_RDMA
