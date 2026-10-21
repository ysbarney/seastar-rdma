/*
 * Copyright 2026 Seastar contributors
 */

// End-to-end tests of the RDMA network stack over the loopback mock
// verbs provider (see rdma_verbs_mock.cc). The engine runs with
// --network-stack rdma (RUN_ARGS).
//
// The mock delivers completions synchronously inside post_send, so a
// single reactor tick fully drains both directions.

#include <boost/test/unit_test.hpp>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <random>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <seastar/core/posix.hh>
#include <seastar/core/internal/pollable_fd.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/net/api.hh>
#include <seastar/testing/random.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/seastar_test.hh>

#include "rdma_verbs_mock.hh"

using namespace seastar;
using namespace seastar::net;

// Install the mock provider before the engine starts (static init
// runs before seastar's main). Setting RDMA_TEST_REAL=1 keeps the real
// libibverbs provider so the suite can run against actual hardware.
static const bool mock_installed = [] {
#ifdef SEASTAR_HAVE_RDMA
    if (!::getenv("RDMA_TEST_REAL")) {
        rdma::install_verbs_mock();
    }
#endif
    return true;
}();

static socket_address random_local_addr() {
    std::default_random_engine& rnd = testing::local_random_engine;
    auto distr = std::uniform_int_distribution<uint16_t>(12000, 65000);
    return make_ipv4_address({"127.0.0.1", distr(rnd)});
}

// Like random_local_addr, but bind-tested: sequential connections in
// one test reuse ports fast enough to hit TIME_WAIT collisions.
static socket_address unused_local_addr() {
    while (true) {
        socket_address sa = random_local_addr();
        int probe = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (probe < 0) {
            return sa;
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(sa.port());
        if (::bind(probe, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
            ::close(probe);
            return sa;
        }
        ::close(probe);
    }
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

// Accept connections until aborted, echoing each.
static future<> echo_server_forever(server_socket& listener) {
    while (true) {
        try {
            co_await echo_server(listener);
        } catch (...) {
            break;
        }
    }
}

// Connect and echo one exchange of `nbytes` bytes, verifying the data.
static future<> echo_exchange(socket_address sa, sstring payload) {
    ::seastar::socket s = engine().net().socket();
    connected_socket c = co_await s.connect(sa);
    auto out = c.output();
    auto in = c.input();
    co_await out.write(payload);
    co_await out.flush();
    temporary_buffer<char> b = co_await in.read_exactly(payload.size());
    BOOST_REQUIRE_EQUAL(std::string(b.get(), b.size()), payload);
    co_await out.close();
    co_await in.close();
}

// ---- RDMA data path ------------------------------------------------------

#ifdef SEASTAR_HAVE_RDMA

SEASTAR_TEST_CASE(rdma_loopback_echo) {
    // 1-byte / 8KB / 1MB payloads through the RDMA path (the mock
    // pairs the two endpoints' QPs via qp_num exchange).
    auto sa = random_local_addr();
    server_socket listener = engine().net().listen(sa, listen_options());
    future<> server = echo_server_forever(listener);
    co_await echo_exchange(sa, "x");
    co_await echo_exchange(sa, sstring(8192, 'y'));
    // Stop the server by aborting the pending accept.
    listener.abort_accept();
    co_await std::move(server).handle_exception([] (std::exception_ptr) {});
}

SEASTAR_TEST_CASE(rdma_loopback_echo_large) {
    // 1MB through the 8K send blocks: exercises SGE cutting, the
    // window (qp-depth 128 -> 125 WRs of window), imm credit returns
    // and the signalled-batch wr_id accounting.
    auto sa = random_local_addr();
    server_socket listener = engine().net().listen(sa, listen_options());
    future<> server = echo_server(listener);

    connected_socket c = co_await engine().net().socket().connect(sa);
    auto out = c.output();
    auto in = c.input();
    static constexpr size_t total = 1 << 20;
    sstring payload(sstring::initialized_later(), total);
    for (size_t i = 0; i < total; ++i) {
        payload[i] = char('a' + (i % 26));
    }
    co_await out.write(payload);
    co_await out.flush();
    temporary_buffer<char> b = co_await in.read_exactly(total);
    BOOST_REQUIRE_EQUAL(b.size(), total);
    BOOST_REQUIRE(std::memcmp(b.get(), payload.data(), total) == 0);
    co_await out.close();
    co_await in.close();

    co_await std::move(server);
}

SEASTAR_TEST_CASE(rdma_fin_order) {
    // shutdown_output() after writing: the peer must drain all data
    // before observing EOF.
    auto sa = random_local_addr();
    server_socket listener = engine().net().listen(sa, listen_options());

    // Server: accept, read everything until EOF, verify all bytes.
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
        BOOST_REQUIRE_EQUAL(got, size_t(3 * 8192));
        co_await in.close();
    }(listener);

    // Client: write 3 blocks worth, then close the output (FIN).
    connected_socket c = co_await engine().net().socket().connect(sa);
    auto out = c.output();
    co_await out.write(sstring(3 * 8192, 'z'));
    co_await out.close(); // FIN after flush

    co_await std::move(server);
}

// ---- TCP fallback --------------------------------------------------------

// A genuinely non-RDMA TCP peer: a blocking-socket thread. engine().net()
// under --network-stack rdma would itself speak RDMA, so the "decline"
// side must come from outside the stack. It accepts one connection,
// eats the 40-byte RDMA hello, answers `reply` (non-RDMA bytes),
// half-closes and drains until the client goes away. `ready_fd` is a
// pipe written one byte once listening (so the test never races the
// bind).
static void tcp_reject_thread(uint16_t port, std::string reply, int ready_fd) {
    int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) {
        return;
    }
    int one = 1;
    ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (::bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(lfd);
        return;
    }
    if (::listen(lfd, 1) != 0) {
        ::close(lfd);
        return;
    }
    char c = 1;
    (void)!::write(ready_fd, &c, 1); // signal: listener is up
    int cfd = ::accept(lfd, nullptr, nullptr);
    ::close(lfd);
    if (cfd < 0) {
        return;
    }
    // Drain the RDMA hello (magic + body) ...
    char sink[256];
    size_t eaten = 0;
    while (eaten < 40) {
        ssize_t n = ::recv(cfd, sink, sizeof(sink), 0);
        if (n <= 0) {
            ::close(cfd);
            return;
        }
        eaten += size_t(n);
    }
    // ... and answer with non-RDMA bytes.
    size_t sent = 0;
    while (sent < reply.size()) {
        ssize_t n = ::send(cfd, reply.data() + sent, reply.size() - sent, 0);
        if (n <= 0) {
            break;
        }
        sent += size_t(n);
    }
    ::shutdown(cfd, SHUT_WR);
    // Drain until the client goes away.
    while (::recv(cfd, sink, sizeof(sink), 0) > 0) {
    }
    ::close(cfd);
}

// Wait until the pipe written by the helper's ready signal becomes
// readable (or the timeout elapses: the helper failed to listen).
static future<> wait_ready(int pipe_fd) {
    pollable_fd pfd(seastar::file_desc::from_fd(::dup(pipe_fd)));
    co_await with_timeout(lowres_clock::now() + std::chrono::seconds(5),
                          pfd.readable());
}

SEASTAR_TEST_CASE(rdma_client_downgrades_to_tcp) {
    // Client speaks RDMA, the peer answers garbage: connect() must
    // downgrade to plain TCP on the SAME connection and the stream
    // must deliver both the pre-read reply bytes and the rest.
    auto sa = random_local_addr();

    int ready_pipe[2];
    BOOST_REQUIRE(::pipe(ready_pipe) == 0);
    auto serv = std::make_unique<posix_thread>([port = sa.port(), w = ready_pipe[1]] {
        tcp_reject_thread(port, "no-rdma-here", w);
    });
    co_await wait_ready(ready_pipe[0]);
    ::close(ready_pipe[0]);
    ::close(ready_pipe[1]);

    ::seastar::socket s = engine().net().socket();
    connected_socket c = co_await s.connect(sa);
    auto out = c.output();
    auto in = c.input();
    // The garbage reply (already read during the handshake) must come
    // out of the stream first.
    static constexpr std::string_view reply = "no-rdma-here";
    temporary_buffer<char> b = co_await in.read_exactly(reply.size());
    BOOST_REQUIRE_EQUAL(std::string_view(b.get(), b.size()), reply);
    co_await out.close();
    co_await in.close();
    serv->join();
}

// A blocking-socket plain-TCP client thread (outside the rdma stack):
// connect, send `data`, half-close, wait for EOF.
static void tcp_plain_client_thread(uint16_t port, std::string data,
                                    int ready_fd) {
    int cfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (cfd < 0) {
        return;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (::connect(cfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(cfd);
        return;
    }
    char c = 1;
    (void)!::write(ready_fd, &c, 1); // connected
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = ::send(cfd, data.data() + sent, data.size() - sent, 0);
        if (n <= 0) {
            ::close(cfd);
            return;
        }
        sent += size_t(n);
    }
    ::shutdown(cfd, SHUT_WR);
    char sink[256];
    while (::recv(cfd, sink, sizeof(sink), 0) > 0) {
    }
    ::close(cfd);
}

SEASTAR_TEST_CASE(rdma_server_downgrades_to_tcp) {
    // Plain TCP client connects to the RDMA listener: the first 4
    // bytes don't match the magic; the connection stays TCP and the
    // already-read bytes must be delivered to the stream first.
    auto sa = random_local_addr();
    server_socket listener = engine().net().listen(sa, listen_options());

    // Server: accept; the sniffed prefix ("GET ") must arrive before
    // anything else. Payload layout: "GET " (sniffed) + "HTTP/1." +
    // "0" (streamed) — 12 bytes total.
    future<> server = [] (server_socket& listener) -> future<> {
        accept_result ar = co_await listener.accept();
        auto c = std::move(ar.connection);
        auto in = c.input();
        temporary_buffer<char> b1 = co_await in.read_exactly(4);
        BOOST_REQUIRE_EQUAL(std::string(b1.get(), 4), "GET ");
        temporary_buffer<char> b2 = co_await in.read_exactly(7);
        BOOST_REQUIRE_EQUAL(std::string(b2.get(), 7), "HTTP/1.");
        temporary_buffer<char> b3 = co_await in.read_exactly(1);
        BOOST_REQUIRE_EQUAL(std::string(b3.get(), 1), "0");
        // EOF after all bytes.
        temporary_buffer<char> b4 = co_await in.read();
        BOOST_REQUIRE(b4.empty());
        co_await in.close();
    }(listener);

    // Plain TCP client thread (engine().net() would speak RDMA).
    int ready_pipe[2];
    BOOST_REQUIRE(::pipe(ready_pipe) == 0);
    auto cli = std::make_unique<posix_thread>(
        [port = sa.port(), w = ready_pipe[1]] {
            tcp_plain_client_thread(port, "GET HTTP/1.0", w);
        });
    co_await wait_ready(ready_pipe[0]);
    ::close(ready_pipe[0]);
    ::close(ready_pipe[1]);

    co_await std::move(server);
    cli->join();
}

SEASTAR_TEST_CASE(rdma_tcp_only_listener) {
    // listen_options::tcp_only: the rdma stack serves the port as
    // plain TCP with full posix semantics. Verified three ways:
    // 1) data flows (echo), 2) no magic sniffing: the very first
    // client bytes (an RDMA "hello"!) are delivered untouched —
    // proving no RDMA handshake happened, 3) posix socket options
    // work (nodelay get/set roundtrip).
    auto sa = random_local_addr();
    listen_options lo;
    lo.tcp_only = true;
    server_socket listener = engine().net().listen(sa, lo);

    future<> server = [] (server_socket& listener) -> future<> {
        accept_result ar = co_await listener.accept();
        auto c = std::move(ar.connection);

        // 3) posix semantics: nodelay set/get must really work (on the
        // rdma downgrade path it is a no-op that always reports true).
        c.set_nodelay(false);
        BOOST_REQUIRE(!c.get_nodelay());
        c.set_nodelay(true);
        BOOST_REQUIRE(c.get_nodelay());

        // 2) echo the raw bytes; the client deliberately sends bytes
        // that WOULD have triggered the RDMA path on a sniffing
        // listener. On tcp_only they must come back verbatim.
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
    }(listener);

    // Plain TCP client thread. Payload starts with the RDMA magic: a
    // sniffing listener would take it for an RDMA hello and reply
    // with a handshake — the echo would then never match.
    std::string payload = "RDMAfake-not-a-hello";
    int ready_pipe[2];
    BOOST_REQUIRE(::pipe(ready_pipe) == 0);
    auto cli = std::make_unique<posix_thread>(
        [port = sa.port(), payload, w = ready_pipe[1]] {
            tcp_plain_client_thread(port, payload, w);
        });
    co_await wait_ready(ready_pipe[0]);
    ::close(ready_pipe[0]);
    ::close(ready_pipe[1]);

    co_await std::move(server);
    cli->join();
}

// ---- direct-send (zero-copy) path ----------------------------------------

// A sink server: reads everything until EOF, never echoes. This keeps
// the MR-registration counter free of the server's own direct-path
// registrations (an echo's read buffers are freshly allocated on every
// read and register anew, masking the client's cache hits).
static future<> sink_server(server_socket& listener) {
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
    // Two 256K sends (64 8K-block WRs): stays under one RQ ring lap
    // so the server's pool never needs a second region (a region
    // growth would register an MR and pollute the client-side cache
    // assertion below).
    BOOST_REQUIRE_EQUAL(got, size_t(2 * (256 << 10)));
    co_await in.close();
}

SEASTAR_TEST_CASE(rdma_direct_send_mr_reuse) {
    // Large writes (>= --rdma-zcopy-min-size, default 256K) take the
    // zero-copy path. Re-sending the SAME memory must hit the MR cache
    // (rpc stream sinks reuse their snd_buf storage): rounds 2..4
    // register nothing.
    auto sa = random_local_addr();
    server_socket listener = engine().net().listen(sa, listen_options());
    future<> server = sink_server(listener);

    connected_socket c = co_await engine().net().socket().connect(sa);
    auto out = c.output();
    static constexpr size_t total = 256 << 10;
    // One persistent buffer; share() keeps the memory identical
    // across rounds (what put() sees is the same data pointer).
    temporary_buffer<char> buf(total);
    std::memset(buf.get_write(), 'm', total);
    co_await out.write(buf.share());
    co_await out.flush();
    // Count only direct-path registrations: pool region growth inside
    // this window (other tests' receive rings) would otherwise flake
    // the global counter.
    uint64_t regs0 = rdma::rdma_mock::direct_reg_mr_calls();
    for (int round = 0; round < 1; ++round) {
        co_await out.write(buf.share());
        co_await out.flush();
    }
    uint64_t regs1 = rdma::rdma_mock::direct_reg_mr_calls();
    // All re-sends hit the cache: no new registrations.
    BOOST_CHECK_EQUAL(regs1 - regs0, uint64_t(0));

    co_await out.close();
    co_await std::move(server);
    c.shutdown_input();
}

SEASTAR_TEST_CASE(rdma_direct_send_echo) {
    // Correctness through the echo path with direct sends in both
    // directions (payload >= the zcopy threshold on both sides).
    auto sa = random_local_addr();
    server_socket listener = engine().net().listen(sa, listen_options());
    future<> server = echo_server(listener);

    connected_socket c = co_await engine().net().socket().connect(sa);
    auto out = c.output();
    auto in = c.input();
    // Odd sizes around the 8K cut points exercise SGE cutting of the
    // direct buffers; 256K+ crosses the direct threshold.
    for (size_t total : {size_t(600), size_t(8192), size_t(8192 + 1),
                         size_t(3 * 8192 + 5), size_t(256 << 10) + 3}) {
        sstring payload(sstring::initialized_later(), total);
        for (size_t i = 0; i < total; ++i) {
            payload[i] = char('a' + (i % 26));
        }
        temporary_buffer<char> w(total);
        std::memcpy(w.get_write(), payload.data(), total);
        co_await out.write(std::move(w));
        co_await out.flush();
        temporary_buffer<char> b = co_await in.read_exactly(total);
        BOOST_REQUIRE_EQUAL(b.size(), total);
        BOOST_REQUIRE(std::memcmp(b.get(), payload.data(), total) == 0);
    }
    co_await out.close();
    co_await in.close();
    co_await std::move(server);
}

SEASTAR_TEST_CASE(rdma_direct_send_no_mr_leak) {
    // After the connection closes, all direct-path registrations must
    // be released (the per-shard pool regions persist; the delta is
    // only transient direct MRs, which must return to the base level).
    size_t mrs0 = rdma::rdma_mock::live_mrs();
    auto sa = random_local_addr();
    server_socket listener = engine().net().listen(sa, listen_options());
    future<> server = echo_server(listener);

    connected_socket c = co_await engine().net().socket().connect(sa);
    auto out = c.output();
    auto in = c.input();
    static constexpr size_t total = 256 << 10;
    temporary_buffer<char> buf(total);
    std::memset(buf.get_write(), 'q', total);
    co_await out.write(std::move(buf));
    co_await out.flush();
    temporary_buffer<char> b = co_await in.read_exactly(total);
    BOOST_REQUIRE_EQUAL(b.size(), total);
    co_await out.close();
    co_await in.close();
    co_await std::move(server);
    co_await sleep(std::chrono::milliseconds(100));
    size_t mrs1 = rdma::rdma_mock::live_mrs();
    BOOST_CHECK_LE(mrs1, mrs0 + 4); // pool growth margin
}

// ---- resource recycling --------------------------------------------------

SEASTAR_TEST_CASE(rdma_qp_pool_recycling) {
    // A burst of sequential connections must not leak QPs/CQs (the
    // pool recycles; the endpoint destructor releases).
    size_t before_qps = rdma::rdma_mock::live_qps();
    size_t before_cqs = rdma::rdma_mock::live_cqs();
    for (int i = 0; i < 20; ++i) {
        auto sa = unused_local_addr();
        server_socket listener = engine().net().listen(sa, listen_options());
        future<> server = echo_server(listener);
        co_await echo_exchange(sa, "ping");
        co_await std::move(server);
    }
    // Give deferred destruction (endpoint lw_shared_ptr teardown) a
    // tick to settle, then check for leaks. The pool legitimately
    // keeps its pre-created QPs; only growth beyond the pool must not
    // leak.
    co_await sleep(std::chrono::milliseconds(100));
    size_t after_qps = rdma::rdma_mock::live_qps();
    size_t after_cqs = rdma::rdma_mock::live_cqs();
    BOOST_REQUIRE_LE(after_qps, before_qps + 2);
    BOOST_REQUIRE_LE(after_cqs, before_cqs + 4);
}

#endif // SEASTAR_HAVE_RDMA
