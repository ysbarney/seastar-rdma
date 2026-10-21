/*
 * Copyright 2026 Seastar contributors
 */

// End-to-end tests of the RDMA network stack with a large negotiated
// receive block size (RUN_ARGS: --network-stack rdma
// --rdma-block-size 65536). With 64K blocks the receive ring is
// shallow (16 slots), so these tests also walk the reduced-window
// flow-control paths: credit accounting with _rq_slots < qp_depth,
// WRs up to the negotiated block size, and the send-side coalescing
// of successive copy-path puts into whole 64K staging blocks.
//
// The mock (rdma_verbs_mock.cc) delivers completions synchronously
// inside post_send, so a single reactor tick fully drains both
// directions.

#include <boost/test/unit_test.hpp>
#include <chrono>
#include <cstring>
#include <random>

#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/api.hh>
#include <seastar/testing/random.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/seastar_test.hh>

#include "rdma_verbs_mock.hh"

using namespace seastar;
using namespace seastar::net;

// Install the mock provider before the engine starts (static init
// runs before seastar's main).
static const bool mock_installed = [] {
#ifdef SEASTAR_HAVE_RDMA
    rdma::install_verbs_mock();
#endif
    return true;
}();

static socket_address random_local_addr() {
    std::default_random_engine& rnd = testing::local_random_engine;
    auto distr = std::uniform_int_distribution<uint16_t>(12000, 65000);
    return make_ipv4_address({"127.0.0.1", distr(rnd)});
}

// Accept one connection and echo everything back until EOF.
static future<> echo_server(server_socket& listener) {
    accept_result ar = co_await listener.accept();
    auto c = std::move(ar.connection);
    auto out = c.output();
    auto in = c.input();
    while (true) {
        temporary_buffer<char> buf = co_await in.read();
        if (buf.empty()) {
            break;
        }
        co_await out.write(std::move(buf));
        co_await out.flush();
    }
    co_await out.close();
    co_await in.close();
    c.shutdown_input();
}

// Accept one connection and drain it to EOF (no reply traffic; used
// where the assertion counts registrations and echo replies would
// pollute the count).
static future<> sink_server(server_socket& listener) {
    accept_result ar = co_await listener.accept();
    auto c = std::move(ar.connection);
    auto in = c.input();
    while (true) {
        temporary_buffer<char> b = co_await in.read();
        if (b.empty()) {
            break;
        }
    }
    co_await in.close();
}

#ifdef SEASTAR_HAVE_RDMA

static sstring make_payload(size_t total) {
    sstring payload(sstring::initialized_later(), total);
    for (size_t i = 0; i < total; ++i) {
        payload[i] = char('a' + (i % 26));
    }
    return payload;
}

// One write+read-echo round of `total` bytes with verification.
// `total` must not exceed the peer's receive ring capacity (see
// rdma_large_block_echo).
static future<> echo_round(output_stream<char>& out, input_stream<char>& in,
                           size_t total) {
    sstring payload = make_payload(total);
    co_await out.write(payload);
    co_await out.flush();
    temporary_buffer<char> b = co_await in.read_exactly(total);
    BOOST_REQUIRE_EQUAL(b.size(), total);
    BOOST_REQUIRE(std::memcmp(b.get(), payload.data(), total) == 0);
}

SEASTAR_TEST_CASE(rdma_large_block_echo) {
    // Sizes crossing every staging/cut boundary under 64K blocks:
    // 1B, exactly 64K, 64K+1 (two staging blocks / two WRs), 1MB
    // (rpc-style: many small puts coalesced into whole 64K staging
    // blocks), plus an odd size spanning several blocks.
    //
    // A single write->flush->read echo round must stay at or below
    // the receive ring capacity (16 x 64K = 1MB here): credits are
    // owed on consumption, so in-flight data per direction is
    // bounded by the ring, and a peer echoing while we refuse to
    // read (our flush is still draining) deadlocks past that bound
    // -- the same bound under which a TCP echo server deadlocks
    // once the transfer exceeds the socket buffers. rpc-style use
    // (interleaved request/reply consumption) never hits it.
    auto sa = random_local_addr();
    server_socket listener = engine().net().listen(sa, listen_options());
    future<> server = echo_server(listener);

    connected_socket c = co_await engine().net().socket().connect(sa);
    auto out = c.output();
    auto in = c.input();
    for (size_t total : {size_t(1), size_t(8192), size_t(65536),
                         size_t(65536 + 1), size_t(1 << 20),
                         size_t(3 * 65536 + 5)}) {
        co_await echo_round(out, in, total);
    }
    co_await out.close();
    co_await in.close();
    co_await std::move(server);
}

SEASTAR_TEST_CASE(rdma_large_block_direct_send_echo) {
    // Direct sends (>= the 256K zcopy threshold) over 64K-block
    // peers: the WR cut loop now allows an SGE to span the full
    // negotiated block size, and copy staging must close the open
    // block before the direct entry is queued (ordering).
    auto sa = random_local_addr();
    server_socket listener = engine().net().listen(sa, listen_options());
    future<> server = echo_server(listener);

    connected_socket c = co_await engine().net().socket().connect(sa);
    auto out = c.output();
    auto in = c.input();

    // A small write (stays in the open copy block), then a direct
    // send: the staged bytes must be ordered before the direct
    // buffer's bytes.
    sstring small = make_payload(30000);
    co_await out.write(small);
    static constexpr size_t big = 256 << 10;
    temporary_buffer<char> w(big);
    std::memset(w.get_write(), 'd', big);
    co_await out.write(std::move(w));
    co_await out.flush();
    temporary_buffer<char> b = co_await in.read_exactly(small.size() + big);
    BOOST_REQUIRE_EQUAL(b.size(), small.size() + big);
    BOOST_REQUIRE(std::memcmp(b.get(), small.data(), small.size()) == 0);
    for (size_t i = 0; i < big; ++i) {
        BOOST_REQUIRE_EQUAL(b.get()[small.size() + i], 'd');
    }

    // Odd direct size around the threshold boundary (all within the
    // 1MB ring bound; direct sends need no receive staging on the
    // sender's side but the echo still flows through the ring).
    for (size_t total : {size_t(600), size_t(65536 + 1),
                         size_t(256 << 10), size_t((256 << 10) + 3)}) {
        co_await echo_round(out, in, total);
    }
    co_await out.close();
    co_await in.close();
    co_await std::move(server);
}

SEASTAR_TEST_CASE(rdma_large_block_mr_reuse) {
    // Re-sending the same >=256K memory over a 64K-block peer must
    // hit the MR cache: no new registrations after the first send.
    // The server only drains (an echo would stage its reply in fresh
    // 256K buffers -- one honest cache-miss registration per round
    // on the server side, unrelated to the client cache under test).
    auto sa = random_local_addr();
    server_socket listener = engine().net().listen(sa, listen_options());
    future<> server = sink_server(listener);

    connected_socket c = co_await engine().net().socket().connect(sa);
    auto out = c.output();
    static constexpr size_t total = 256 << 10;
    temporary_buffer<char> buf(total);
    std::memset(buf.get_write(), 'm', total);
    co_await out.write(buf.share());
    co_await out.flush();
    uint64_t regs0 = rdma::rdma_mock::direct_reg_mr_calls();
    for (int round = 0; round < 3; ++round) {
        co_await out.write(buf.share());
        co_await out.flush();
    }
    uint64_t regs1 = rdma::rdma_mock::direct_reg_mr_calls();
    BOOST_CHECK_EQUAL(regs1 - regs0, uint64_t(0));

    co_await out.close();
    co_await std::move(server);
    c.shutdown_input();
}

SEASTAR_TEST_CASE(rdma_large_block_fin_order) {
    // shutdown_output() after a large write: the open staging block
    // must be closed and the peer must drain all bytes before EOF
    // (exercises the flush()-closes-staging path with a partially
    // filled 64K block).
    auto sa = random_local_addr();
    server_socket listener = engine().net().listen(sa, listen_options());

    // 3 * 64K + 1000: the tail (1000 bytes) sits in the open copy
    // block when shutdown_output() runs.
    static constexpr size_t total = 3 * 65536 + 1000;
    future<> server = [] (server_socket& listener) -> future<> {
        accept_result ar = co_await listener.accept();
        auto c = std::move(ar.connection);
        auto in = c.input();
        size_t got = 0;
        while (true) {
            temporary_buffer<char> b = co_await in.read();
            got += b.size();
            if (b.empty()) {
                break;
            }
        }
        BOOST_REQUIRE_EQUAL(got, total);
        co_await in.close();
    }(listener);

    connected_socket c = co_await engine().net().socket().connect(sa);
    auto out = c.output();
    co_await out.write(sstring(total, 'z'));
    co_await out.close(); // FIN after flush

    co_await std::move(server);
}

SEASTAR_TEST_CASE(rdma_large_block_pipelined_echo) {
    // rpc-style pipelining: 64 request/reply rounds over one
    // connection, each direction exceeding the receive ring in total
    // (64 x 200K >> 1MB ring). The writer keeps queueing new requests
    // (flush() is batched and returns immediately, exactly like rpc's
    // send_entry loop) while older replies are still unread -- put()
    // must interleave with independent get() consumption, and credit
    // returns must not stall either side (a real-machine p64 write-1m
    // hang repro). Writes are serialized like rpc does (its
    // _outgoing_queue_ready chain): concurrent put() on one endpoint
    // is not a supported use of the sink.
    auto sa = random_local_addr();
    server_socket listener = engine().net().listen(sa, listen_options());
    future<> server = echo_server(listener);

    connected_socket c = co_await engine().net().socket().connect(sa);
    auto out = c.output();
    auto in = c.input();

    static constexpr unsigned concurrency = 64;
    static constexpr size_t payload_size = 200000; // > ring (16 x 64K)

    // Reader: consume all replies, independently of the writer. The
    // stream is passed as a parameter, not a capture: a capturing
    // lambda coroutine would keep reading its (dead temporary) capture
    // block after every resume.
    future<> reader = [](input_stream<char>& in,
                         size_t expected_total) -> future<> {
        size_t got = 0;
        while (got < expected_total) {
            temporary_buffer<char> b = co_await in.read();
            BOOST_REQUIRE(!b.empty());
            got += b.size();
        }
        BOOST_REQUIRE_EQUAL(got, expected_total);
    }(in, size_t(concurrency) * payload_size);

    // Writer: serialized rounds, not waiting for the replies.
    sstring payload = make_payload(payload_size);
    for (unsigned i = 0; i < concurrency; ++i) {
        co_await out.write(payload);
        co_await out.flush();
    }
    co_await out.close();
    co_await std::move(reader);
    co_await in.close();
    co_await std::move(server);
}

SEASTAR_TEST_CASE(rdma_large_block_qp_recycling) {
    // Sequential connections under 64K blocks must not leak QPs/CQs;
    // the 2M-class regions used by send staging return to the pool.
    size_t before_qps = rdma::rdma_mock::live_qps();
    size_t before_cqs = rdma::rdma_mock::live_cqs();
    for (int i = 0; i < 10; ++i) {
        auto sa = random_local_addr();
        server_socket listener = engine().net().listen(sa, listen_options());
        future<> server = echo_server(listener);
        connected_socket c = co_await engine().net().socket().connect(sa);
        auto out = c.output();
        auto in = c.input();
        co_await echo_round(out, in, 200000); // spans several 64K blocks
        co_await out.close();
        co_await in.close();
        co_await std::move(server);
    }
    co_await sleep(std::chrono::milliseconds(100));
    size_t after_qps = rdma::rdma_mock::live_qps();
    size_t after_cqs = rdma::rdma_mock::live_cqs();
    BOOST_REQUIRE_LE(after_qps, before_qps + 2);
    BOOST_REQUIRE_LE(after_cqs, before_cqs + 4);
}

#endif // SEASTAR_HAVE_RDMA
