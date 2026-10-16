/*
 * Copyright 2026 Seastar contributors
 */

// Wire-format and flow-control math tests for the RDMA stack. Pure
// functions, no engine.

#define BOOST_TEST_MODULE rdma_wire

#include <boost/test/unit_test.hpp>
#include <cstring>

#include <seastar/net/socket_defs.hh>

#include "net/rdma-handshake.hh"

using namespace seastar::net::rdma;

BOOST_AUTO_TEST_CASE(hello_roundtrip) {
    hello_params p{};
    p.block_size = 8192;
    p.sq_size = 128;
    p.rq_size = 128;
    p.lid = 0x1234;
    for (int i = 0; i < 16; ++i) {
        p.peer_gid.raw[i] = uint8_t(i);
    }
    p.qp_num = 0xdeadbeef;

    char wire[hello_total_len];
    write_hello(wire, p);

    // Magic.
    BOOST_CHECK(std::memcmp(wire, hello_magic, hello_magic_len) == 0);

    // Roundtrip.
    hello_params back{};
    BOOST_CHECK(parse_hello_body(wire + hello_magic_len, back) == hello_result::ok);
    BOOST_CHECK_EQUAL(back.block_size, p.block_size);
    BOOST_CHECK_EQUAL(back.sq_size, p.sq_size);
    BOOST_CHECK_EQUAL(back.rq_size, p.rq_size);
    BOOST_CHECK_EQUAL(back.lid, p.lid);
    BOOST_CHECK(back.peer_gid == p.peer_gid);
    BOOST_CHECK_EQUAL(back.qp_num, p.qp_num);
}

BOOST_AUTO_TEST_CASE(hello_golden_vector) {
    // Byte-exact golden vector of the v2 hello (big-endian fields).
    hello_params p{};
    p.block_size = 0x00002000; // 8192
    p.sq_size = 0x0080;        // 128
    p.rq_size = 0x0080;
    p.lid = 0x0001;
    p.peer_gid.raw = {0xfe, 0x80, 0, 0, 0, 0, 0, 0,
                      0x02, 0xc9, 0xd0, 0xff, 0xfe, 0x12, 0x34, 0x56};
    p.qp_num = 0x0000002a;

    char wire[hello_total_len];
    write_hello(wire, p);

    static const unsigned char expected[hello_total_len] = {
        'R', 'D', 'M', 'A',                    // magic
        0x00, 0x28,                            // msg_len = 40
        0x00, 0x02,                            // hello_ver = 2
        0x00, 0x01,                            // impl_ver = 1
        0x00, 0x00, 0x20, 0x00,                // block_size
        0x00, 0x80,                            // sq_size
        0x00, 0x80,                            // rq_size
        0x00, 0x01,                            // lid
        0xfe, 0x80, 0, 0, 0, 0, 0, 0,          // gid
        0x02, 0xc9, 0xd0, 0xff, 0xfe, 0x12, 0x34, 0x56,
        0x00, 0x00, 0x00, 0x2a,                // qp_num
    };
    BOOST_CHECK(std::memcmp(wire, expected, hello_total_len) == 0);
}

BOOST_AUTO_TEST_CASE(hello_validation_matrix) {
    char wire[hello_total_len];
    hello_params good{};
    good.block_size = 8192;
    good.sq_size = 128;
    good.rq_size = 128;
    good.qp_num = 42;
    write_hello(wire, good);

    hello_params out{};

    // Baseline valid.
    BOOST_CHECK(parse_hello_body(wire + hello_magic_len, out) == hello_result::ok);

    // Version mismatch -> decline (same framing, different protocol).
    char bad[hello_total_len];
    std::memcpy(bad, wire, sizeof(bad));
    bad[hello_magic_len + 2] = 0; // hello_ver = 0
    bad[hello_magic_len + 3] = 0;
    BOOST_CHECK(parse_hello_body(bad + hello_magic_len, out) == hello_result::decline);

    // impl_ver mismatch -> decline.
    std::memcpy(bad, wire, sizeof(bad));
    bad[hello_magic_len + 4] = 0; // impl_ver = 0
    bad[hello_magic_len + 5] = 0;
    BOOST_CHECK(parse_hello_body(bad + hello_magic_len, out) == hello_result::decline);

    // Truncated framing -> malformed.
    std::memcpy(bad, wire, sizeof(bad));
    bad[hello_magic_len + 0] = 0; // msg_len = 0
    bad[hello_magic_len + 1] = 0;
    BOOST_CHECK(parse_hello_body(bad + hello_magic_len, out) == hello_result::malformed);

    // block_size below minimum -> decline.
    std::memcpy(bad, wire, sizeof(bad));
    bad[hello_magic_len + 6] = 0;
    bad[hello_magic_len + 7] = 0;
    bad[hello_magic_len + 8] = 0x03; // 1023 < min_block_size
    bad[hello_magic_len + 9] = 0xff;
    BOOST_CHECK(parse_hello_body(bad + hello_magic_len, out) == hello_result::decline);

    // sq_size below minimum -> decline.
    std::memcpy(bad, wire, sizeof(bad));
    bad[hello_magic_len + 10] = 0;
    bad[hello_magic_len + 11] = 15; // < min_qp_size
    BOOST_CHECK(parse_hello_body(bad + hello_magic_len, out) == hello_result::decline);

    // rq_size below minimum -> decline.
    std::memcpy(bad, wire, sizeof(bad));
    bad[hello_magic_len + 12] = 0;
    bad[hello_magic_len + 13] = 15;
    BOOST_CHECK(parse_hello_body(bad + hello_magic_len, out) == hello_result::decline);

    // qp_num == 0 -> decline.
    std::memcpy(bad, wire, sizeof(bad));
    bad[hello_magic_len + 32] = 0;
    bad[hello_magic_len + 33] = 0;
    bad[hello_magic_len + 34] = 0;
    bad[hello_magic_len + 35] = 0;
    BOOST_CHECK(parse_hello_body(bad + hello_magic_len, out) == hello_result::decline);

    // Boundary: exactly min sizes are valid.
    hello_params minp{};
    minp.block_size = min_block_size;
    minp.sq_size = min_qp_size;
    minp.rq_size = min_qp_size;
    minp.qp_num = 1;
    write_hello(bad, minp);
    BOOST_CHECK(parse_hello_body(bad + hello_magic_len, out) == hello_result::ok);

    // Boundary: exactly max qp sizes are valid.
    hello_params maxp{};
    maxp.block_size = min_block_size;
    maxp.sq_size = max_qp_size;
    maxp.rq_size = max_qp_size;
    maxp.qp_num = 1;
    write_hello(bad, maxp);
    BOOST_CHECK(parse_hello_body(bad + hello_magic_len, out) == hello_result::ok);
}

BOOST_AUTO_TEST_CASE(decline_body) {
    char body[hello_body_len];
    write_decline_body(body);
    // Decline is a zeroed body with plausible framing; parsing it must
    // never yield ok.
    hello_params out{};
    BOOST_CHECK(parse_hello_body(body, out) == hello_result::decline);
}

BOOST_AUTO_TEST_CASE(ack_word) {
    char ack[hello_ack_len];
    write_hello_ack(ack, true);
    BOOST_CHECK(parse_hello_ack(ack));
    // Big-endian: 0x00000001.
    BOOST_CHECK_EQUAL(uint8_t(ack[0]), 0);
    BOOST_CHECK_EQUAL(uint8_t(ack[1]), 0);
    BOOST_CHECK_EQUAL(uint8_t(ack[2]), 0);
    BOOST_CHECK_EQUAL(uint8_t(ack[3]), 1);

    write_hello_ack(ack, false);
    BOOST_CHECK(!parse_hello_ack(ack));
}

BOOST_AUTO_TEST_CASE(imm_encoding) {
    // FIN bit and ack count share the 32-bit imm; FIN is bit 0.
    BOOST_CHECK(!imm_is_fin(host_to_imm(0, false)));
    BOOST_CHECK(imm_is_fin(host_to_imm(0, true)));
    BOOST_CHECK(imm_is_fin(host_to_imm(100, true)));
    BOOST_CHECK_EQUAL(imm_ack_count(host_to_imm(100, false)), 100u);
    BOOST_CHECK_EQUAL(imm_ack_count(host_to_imm(100, true)), 100u);
    BOOST_CHECK_EQUAL(imm_ack_count(host_to_imm(0, true)), 0u);

    // A brpc peer (all 32 bits = ack count) sending ack=1 would be
    // read by us as FIN -- the documented divergence.
    BOOST_CHECK(imm_is_fin(1));
    BOOST_CHECK_EQUAL(imm_ack_count(1), 0u);

    // Byte swap.
    BOOST_CHECK_EQUAL(imm_hton(0x12345678u), 0x78563412u);
    BOOST_CHECK_EQUAL(imm_ntoh(0x78563412u), 0x12345678u);
    BOOST_CHECK_EQUAL(imm_ntoh(imm_hton(0xdeadbeefu)), 0xdeadbeefu);

    // Full 31-bit ack range.
    uint32_t big = 0x7fffffffu;
    BOOST_CHECK_EQUAL(imm_ack_count(host_to_imm(big, false)), big);
}

BOOST_AUTO_TEST_CASE(window_math) {
    // The capacity computation mirrors brpc's ApplyRemoteHello:
    // min(local sq, remote rq) - reserved.
    const uint16_t sq = 128, rq = 64;
    const uint16_t remote_rq = 32, remote_sq = 256;
    uint16_t local_cap = std::min(sq, remote_rq) - reserved_wr_num;
    uint16_t remote_cap = std::min(rq, remote_sq) - reserved_wr_num;
    BOOST_CHECK_EQUAL(local_cap, 32u - 3u);
    BOOST_CHECK_EQUAL(remote_cap, 64u - 3u);

    // Asymmetric the other way.
    local_cap = std::min(rq, remote_sq) - reserved_wr_num;
    BOOST_CHECK_EQUAL(local_cap, 64u - 3u);
}
