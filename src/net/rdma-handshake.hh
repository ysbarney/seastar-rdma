/*
 * Copyright 2026 Seastar contributors
 */

//
// rdma-handshake.hh: wire format of the RDMA connection handshake.
//
// Byte-compatible with brpc's v2 format ("RDMA" magic):
//
//   [ "RDMA" 4B ][ msg_len u16 ][ hello_ver u16 = 2 ][ impl_ver u16 = 1 ]
//   [ block_size u32 ][ sq_size u16 ][ rq_size u16 ][ lid u16 ]
//   [ gid 16B raw ][ qp_num u32 ]            -- big-endian, 40 bytes total
//
// A reply with hello_ver == 0 (or any invalid field) is a decline:
// the replying side cannot do RDMA, and the connection falls back to
// plain TCP on the same socket.
//
// The 4-byte ACK (sent after the QPs are up) carries flags in
// big-endian: bit 0 = RDMA_OK.
//
// In-band flow-control and FIN are carried in the immediate data of
// SEND_WITH_IMM work requests: bit 0 = FIN, bits 1..31 = the number of
// receive work requests the sender has consumed (piggybacked ACK).
// Note that this diverges from brpc (which uses all 32 bits for the
// ACK count) because seastar needs an in-band EOF.
//

#pragma once

#ifdef SEASTAR_HAVE_RDMA

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include <seastar/net/socket_defs.hh>

namespace seastar::net::rdma {

// Handshake magic and lengths.
inline constexpr char hello_magic[4] = {'R', 'D', 'M', 'A'};
inline constexpr size_t hello_magic_len = 4;
inline constexpr size_t hello_body_len = 36; // fields without magic
inline constexpr size_t hello_total_len = hello_magic_len + hello_body_len; // 40

// v2 hello version fields. A valid hello must carry exactly these.
inline constexpr uint16_t hello_v2_version = 2;
inline constexpr uint16_t impl_v2_version = 1;

// Validation bounds (same as brpc).
inline constexpr uint16_t min_qp_size = 16;
inline constexpr uint16_t max_qp_size = 4096;
inline constexpr uint32_t min_block_size = 1024;

// The number of reserved work requests at the tail of the SQ/RQ which
// are never counted into the flow-control windows (same as brpc).
inline constexpr uint16_t reserved_wr_num = 3;

// ACK word (big-endian on the wire).
inline constexpr size_t hello_ack_len = 4;
inline constexpr uint32_t hello_ack_rdma_ok = 0x1;

// Immediate-data encoding (host order on our side of the wire helper;
// the verbs layer delivers imm_data in network order, use
// imm_to_host()/host_to_imm() for the swap).
inline constexpr uint32_t imm_fin_bit = 0x1;
inline constexpr uint32_t imm_ack_shift = 1;

inline constexpr uint32_t host_to_imm(uint32_t ack_count, bool fin) {
    return (ack_count << imm_ack_shift) | (fin ? imm_fin_bit : 0);
}

// Returns true if the imm carried a FIN (decode before ack count).
inline constexpr bool imm_is_fin(uint32_t host_imm) {
    return host_imm & imm_fin_bit;
}

inline constexpr uint32_t imm_ack_count(uint32_t host_imm) {
    return host_imm >> imm_ack_shift;
}

// Byte swap for imm_data: verbs gives/takes network order.
inline constexpr uint32_t imm_hton(uint32_t host) {
    return __builtin_bswap32(host);
}

inline constexpr uint32_t imm_ntoh(uint32_t net) {
    return __builtin_bswap32(net);
}

struct gid {
    std::array<uint8_t, 16> raw{};

    bool operator==(const gid&) const = default;
};

// Negotiated parameters of the local side, advertised in our hello.
struct hello_params {
    uint32_t block_size = 0; // our receive block size
    uint16_t sq_size = 0;
    uint16_t rq_size = 0;
    uint16_t lid = 0;
    gid peer_gid{};
    // QP numbers are 24-bit on the wire; keep 32-bit in memory (HCAs
    // allocate QPNs >= 2^16 in practice).
    uint32_t qp_num = 0;
};

// Serialize our hello (magic + body) into `out` (>= hello_total_len).
void write_hello(char* out, const hello_params& p);

// Outcome of parsing the peer's hello body (36 bytes, magic already
// consumed).
enum class hello_result {
    ok,             // valid, `out` filled
    decline,        // well-formed framing but peer refused / not RDMA
    malformed,      // garbage on the wire
};

// Parse the peer's hello body (exactly hello_body_len bytes at `in`).
hello_result parse_hello_body(const char* in, hello_params& out);

// Build the decline body (hello_ver = 0) into `out` (>= hello_body_len).
void write_decline_body(char* out);

// ACK word helpers (4B big-endian).
void write_hello_ack(char* out, bool rdma_ok);
bool parse_hello_ack(const char* in);

} // namespace seastar::net::rdma

#endif // SEASTAR_HAVE_RDMA
