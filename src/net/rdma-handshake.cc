/*
 * Copyright 2026 Seastar contributors
 */ 

#include "rdma-handshake.hh"

#ifdef SEASTAR_HAVE_RDMA

namespace seastar::net::rdma {

namespace {

// Big-endian scalar packers (same layout as brpc's RawPacker).
void put_u16(char* p, uint16_t v) {
    p[0] = char(v >> 8);
    p[1] = char(v);
}

void put_u32(char* p, uint32_t v) {
    p[0] = char(v >> 24);
    p[1] = char(v >> 16);
    p[2] = char(v >> 8);
    p[3] = char(v);
}

uint16_t get_u16(const char* p) {
    return uint16_t(uint8_t(p[0]) << 8 | uint8_t(p[1]));
}

uint32_t get_u32(const char* p) {
    return uint32_t(uint8_t(p[0])) << 24 | uint32_t(uint8_t(p[1])) << 16 |
           uint32_t(uint8_t(p[2])) << 8 | uint32_t(uint8_t(p[3]));
}

} // anonymous namespace

void write_hello(char* out, const hello_params& p) {
    std::memcpy(out, hello_magic, hello_magic_len);
    char* b = out + hello_magic_len;
    // msg_len counts the whole message including magic (as in brpc).
    put_u16(b + 0, hello_total_len);
    put_u16(b + 2, hello_v2_version);
    put_u16(b + 4, impl_v2_version);
    put_u32(b + 6, p.block_size);
    put_u16(b + 10, p.sq_size);
    put_u16(b + 12, p.rq_size);
    put_u16(b + 14, p.lid);
    std::memcpy(b + 16, p.peer_gid.raw.data(), 16);
    put_u32(b + 32, p.qp_num);
    static_assert(hello_body_len == 36);
}

hello_result parse_hello_body(const char* in, hello_params& out) {
    uint16_t msg_len = get_u16(in);
    uint16_t hello_ver = get_u16(in + 2);
    uint16_t impl_ver = get_u16(in + 4);
    // A valid hello declares the full v2 length; anything else that
    // still parses as (version, impl) mismatch is a decline; garbage
    // framing is malformed.
    if (msg_len < hello_total_len) {
        return hello_result::malformed;
    }
    out.block_size = get_u32(in + 6);
    out.sq_size = get_u16(in + 10);
    out.rq_size = get_u16(in + 12);
    out.lid = get_u16(in + 14);
    std::memcpy(out.peer_gid.raw.data(), in + 16, 16);
    out.qp_num = get_u32(in + 32);

    if (hello_ver != hello_v2_version || impl_ver != impl_v2_version) {
        return hello_result::decline;
    }
    if (out.block_size < min_block_size || out.sq_size < min_qp_size ||
        out.rq_size < min_qp_size || out.sq_size > max_qp_size ||
        out.rq_size > max_qp_size || out.qp_num == 0) {
        // Valid framing, unusable parameters: treat as decline so the
        // connection can fall back to TCP instead of dying.
        return hello_result::decline;
    }
    return hello_result::ok;
}

void write_decline_body(char* out) {
    std::memset(out, 0, hello_body_len);
    put_u16(out, hello_total_len); // plausible framing
}

void write_hello_ack(char* out, bool rdma_ok) {
    put_u32(out, rdma_ok ? hello_ack_rdma_ok : 0);
}

bool parse_hello_ack(const char* in) {
    return get_u32(in) & hello_ack_rdma_ok;
}

} // namespace seastar::net::rdma

#endif // SEASTAR_HAVE_RDMA
