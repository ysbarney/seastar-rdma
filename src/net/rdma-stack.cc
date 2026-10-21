/*
 * Copyright 2026 Seastar contributors
 */

//
// The "rdma" network stack (see include/seastar/net/rdma-stack.hh).
//
// Structurally a mirror of posix-stack.cc: plain TCP sockets carry the
// connection setup, and an RDMA handshake over that TCP connection
// upgrades the data path to an RC QP (see rdma-endpoint.hh). If either
// side declines RDMA, the connection downgrades to TCP on the same
// socket -- the already-read bytes are preserved in a prefix buffer.
//

#include <seastar/net/rdma-stack.hh>
#ifdef SEASTAR_HAVE_RDMA

#include <cstring>
#include <memory>
#include <stdexcept>
#include <variant>
#include <vector>

#include <seastar/core/internal/buffer_allocator.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/posix-stack.hh>
#include <seastar/net/stack.hh>
#include <seastar/util/log.hh>

#include "rdma-endpoint.hh"

namespace seastar::net::rdma {

static logger stack_log("rdma-stack");

rdma_stack_options::rdma_stack_options()
    : program_options::option_group(nullptr, "RDMA networking stack options")
    // All options carry the --rdma- prefix so they never collide with
    // application options of the same name (e.g. rpc_tester's --port).
    , device(*this, "rdma-device", "", "RDMA device name (empty = first active)")
    , port(*this, "rdma-port", 1u, "RDMA port number on the device")
    , gid_index(*this, "rdma-gid-index", "", "GID index (empty = auto)")
    , polling(*this, "rdma-polling", false,
              "busy-poll completion queues from the reactor loop (lower latency, higher CPU)")
    , qp_pool_size(*this, "rdma-qp-pool-size", 256u,
                   "pre-created QP/CQ sets per shard (grows on demand)")
    , qp_depth(*this, "rdma-qp-depth", 128u, "SQ/RQ depth of each QP")
    , block_size(*this, "rdma-block-size", 8192u, "receive block size")
    , zcopy_min_size(*this, "rdma-zcopy-min-size", 256u << 10,
                     "minimum payload for a zero-copy direct-send registration")
    , cqe_poll_batch(*this, "rdma-cqe-poll-batch", 32u, "completions reaped per CQ poll")
    , handshake_timeout_ms(*this, "rdma-handshake-timeout-ms", 10000u,
                           "RDMA handshake timeout in milliseconds")
    , max_regions(*this, "rdma-max-regions", 64u,
                  "maximum registered memory regions per size class per shard") {
}

namespace {

// A simple allocator for handshake reads (fixed 4K chunks; the
// handshake messages are 40B).
class handshake_buffer_allocator final : public internal::buffer_allocator {
public:
    temporary_buffer<char> allocate_buffer() override {
        return temporary_buffer<char>(4096);
    }
};

// ---------------------------------------------------------------------------
// connection_context: an established connection that is either
// RDMA-backed or (after downgrade) plain TCP. The optional prefix holds
// bytes read during sniffing/handshake that belong to the application
// stream in TCP mode.
// ---------------------------------------------------------------------------
struct connection_context {
    std::variant<lw_shared_ptr<rdma_endpoint>, pollable_fd> v;
    std::vector<char> prefix; // pre-read TCP bytes (fallback mode)
    socket_address local;
    socket_address remote;

    bool is_rdma() const noexcept { return v.index() == 0; }
    rdma_endpoint& ep() noexcept { return *std::get<0>(v); }
    pollable_fd& fd() noexcept { return std::get<1>(v); }
};

class rdma_connected_socket_impl final : public connected_socket_impl {
    lw_shared_ptr<connection_context> _ctx;

public:
    explicit rdma_connected_socket_impl(lw_shared_ptr<connection_context> ctx)
        : _ctx(std::move(ctx)) {}

    data_source source() override {
        return source(connected_socket_input_stream_config());
    }

    data_source source(connected_socket_input_stream_config cfg) override;

    data_sink sink() override;

    void shutdown_input() override {
        if (_ctx->is_rdma()) {
          // Best effort: stop delivering; the peer learns via FIN or
          // connection teardown.
          _ctx->ep().fail(ENOTCONN); // resolves readers
        } else {
            _ctx->fd().shutdown(SHUT_RD);
        }
    }

  void shutdown_output() override {
      if (_ctx->is_rdma()) {
          // Async FIN; errors surface via streams.
          (void)_ctx->ep().shutdown_output().handle_exception([] (std::exception_ptr) {});
      } else {
          _ctx->fd().shutdown(SHUT_WR);
      }
  }

    void set_nodelay(bool) override {}
    bool get_nodelay() const override { return true; }
    void set_keepalive(bool) override {}
    bool get_keepalive() const override { return false; }
    void set_keepalive_parameters(const keepalive_params&) override {}
    keepalive_params get_keepalive_parameters() const override {
        return tcp_keepalive_params{};
    }
    void set_sockopt(int, int, const void*, size_t) override {}
    int get_sockopt(int, int, void*, size_t) const override { return 0; }
    socket_address local_address() const noexcept override { return _ctx->local; }
    socket_address remote_address() const noexcept override { return _ctx->remote; }

    size_t snd_chunk_hint() const noexcept override {
        // Large negotiated receive blocks mean the direct-send path can
        // take a big rpc message in one piece; marshalling into
        // matching chunks (one buffer per WR-sized unit) lets it.
        if (_ctx->is_rdma()) {
            return _ctx->ep().remote_recv_block_size();
        }
        return 0;
    }

    future<> wait_input_shutdown() override {
        if (_ctx->is_rdma()) {
            // RDMA has no kernel-side RDHUP on the data path; wait on
            // the TCP fd kept open as the liveness detector.
            return _ctx->ep().tcp_fd().poll_rdhup();
        }
        return _ctx->fd().poll_rdhup();
    }
};

// ---------------------------------------------------------------------------
// data source
// ---------------------------------------------------------------------------
class rdma_data_source_impl final : public data_source_impl,
                                    private internal::buffer_allocator {
    lw_shared_ptr<connection_context> _ctx;
    connected_socket_input_stream_config _config;
    size_t _prefix_off = 0;
    bool _eof = false;

private:
    temporary_buffer<char> allocate_buffer() override {
        return temporary_buffer<char>(_config.buffer_size);
    }

public:
    explicit rdma_data_source_impl(lw_shared_ptr<connection_context> ctx,
                                   connected_socket_input_stream_config cfg)
        : _ctx(std::move(ctx)), _config(cfg) {}

    future<temporary_buffer<char>> get() override {
        if (_ctx->is_rdma()) {
            co_return co_await _ctx->ep().get();
        }
        // TCP fallback: prefix first, then the fd.
        if (_prefix_off < _ctx->prefix.size()) {
            size_t n = std::min(_ctx->prefix.size() - _prefix_off, size_t(_config.buffer_size));
            temporary_buffer<char> buf(n);
            std::memcpy(buf.get_write(), _ctx->prefix.data() + _prefix_off, n);
            _prefix_off += n;
            co_return buf;
        }
        if (_eof) {
            co_return temporary_buffer<char>();
        }
	temporary_buffer<char> b = co_await _ctx->fd().recv_some(
                static_cast<internal::buffer_allocator*>(this));
        if (b.empty()) {
            _eof = true;
        }
        co_return b;
    }

    future<> close() override {
        if (_ctx->is_rdma()) {
            // Mirror SHUT_RD semantics: a blocked get() must wake (the
            // rpc layer abandons the connection here; nobody else will
            // drain the endpoint). fail() resolves all waiters with an
            // error and closes the liveness fd, which also lets the
            // peer's death watch tear its side down.
            _ctx->ep().fail(ENOTCONN);
        } else {
            _ctx->fd().shutdown(SHUT_RD);
        }
        return make_ready_future<>();
    }
};

// ---------------------------------------------------------------------------
// data sink
// ---------------------------------------------------------------------------
class rdma_data_sink_impl final : public data_sink_impl {
    lw_shared_ptr<connection_context> _ctx;
    internal::wrapped_iovecs _vecs;

public:
    explicit rdma_data_sink_impl(lw_shared_ptr<connection_context> ctx)
        : _ctx(std::move(ctx)) {}

    future<> put(std::span<temporary_buffer<char>> bufs) override {
        if (_ctx->is_rdma()) {
            // The endpoint copies synchronously into registered blocks
            // before its first suspension.
            co_return co_await _ctx->ep().put(bufs);
        }
        auto [total, del] = _vecs.populate(bufs);
        co_return co_await _ctx->fd().write_all(_vecs.v).finally([del = std::move(del)] {});
    }

    future<> flush() override {
        if (_ctx->is_rdma()) {
            co_return co_await _ctx->ep().flush();
        }
        co_return;
    }

    future<> close() override {
        if (_ctx->is_rdma()) {
            co_return co_await _ctx->ep().shutdown_output();
        }
        _ctx->fd().shutdown(SHUT_WR);
        co_return;
    }

    bool can_batch_flushes() const noexcept override { return true; }

    void on_batch_flush_error() noexcept override {
        if (_ctx->is_rdma()) {
            _ctx->ep().fail(ECONNRESET);
        } else {
            _ctx->fd().shutdown(SHUT_RD);
        }
    }
};

data_source rdma_connected_socket_impl::source(connected_socket_input_stream_config cfg) {
    return data_source(std::make_unique<rdma_data_source_impl>(_ctx, cfg));
}

data_sink rdma_connected_socket_impl::sink() {
    return data_sink(std::make_unique<rdma_data_sink_impl>(_ctx));
}

// ---------------------------------------------------------------------------
// server socket for tcp_only listeners: plain TCP accept, no RDMA
// sniffing, full posix semantics via the posix connected-socket impl.
// ---------------------------------------------------------------------------
class rdma_tcp_only_server_socket_impl final : public server_socket_impl {
    pollable_fd _lfd;

public:
    explicit rdma_tcp_only_server_socket_impl(pollable_fd lfd)
        : _lfd(std::move(lfd)) {}

    future<accept_result> accept() override {
        auto [fd, remote] = co_await _lfd.accept();
        int protocol = remote.is_af_unix() ? 0 : int(transport::TCP);
        co_return accept_result{
            internal::make_posix_connected_socket(remote.u.sa.sa_family,
                                                  protocol, std::move(fd)),
            remote};
    }

    void abort_accept() override {
        _lfd.shutdown(SHUT_RD, pollable_fd::shutdown_kernel_only::yes);
    }

    socket_address local_address() const override {
        return _lfd.get_file_desc().get_address();
    }
};

// ---------------------------------------------------------------------------
// server socket: plain TCP listen + sniffing accept
// ---------------------------------------------------------------------------
class rdma_server_socket_impl final : public server_socket_impl {
    pollable_fd _lfd;
    rdma_device* _dev;
    lowres_clock::duration _handshake_timeout;

    // Read at least the 4-byte magic sniff (a single recv may deliver
    // the whole 40-byte hello on loopback). Returns the bytes read; on
    // EOF/timeout returns what it has.
    future<std::vector<char>> sniff_magic(pollable_fd& fd) {
        std::vector<char> head;
        handshake_buffer_allocator alloc;
        co_await with_timeout(lowres_clock::now() + _handshake_timeout,
                              [&]() -> future<> {
            while (head.size() < hello_magic_len) {
                temporary_buffer<char> b = co_await fd.recv_some(&alloc);
                if (b.empty()) {
                    break;
                }
                std::copy(b.begin(), b.end(), std::back_inserter(head));
            }
        }());
        co_return head;
    }

    // Read the rest of the hello body after the magic matched. `head`
    // may already hold part (or all) of the body: loopback routinely
    // delivers the full hello in one recv.
    future<std::vector<char>> read_body(pollable_fd& fd, std::vector<char>& head) {
        std::vector<char> body(head.begin() + std::min(head.size(), hello_magic_len),
                                head.end());
        head.resize(std::min(head.size(), hello_magic_len));
        co_await with_timeout(lowres_clock::now() + _handshake_timeout,
                              [&]() -> future<> {
            handshake_buffer_allocator alloc;
            while (body.size() < hello_body_len) {
                temporary_buffer<char> b = co_await fd.recv_some(&alloc);
                if (b.empty()) {
                    break;
                }
                std::copy(b.begin(), b.end(), std::back_inserter(body));
            }
        }());
        co_return body;
    }

public:
    rdma_server_socket_impl(pollable_fd lfd, rdma_device* dev,
                            lowres_clock::duration timeout)
        : _lfd(std::move(lfd)), _dev(dev), _handshake_timeout(timeout) {}

    future<accept_result> accept() override {
        auto [fd, remote] = co_await _lfd.accept();
        auto local = fd.get_file_desc().get_address();
        stack_log.debug("accept: from {}", remote);

        std::vector<char> head = co_await sniff_magic(fd);
        stack_log.debug("accept: sniffed {} bytes", head.size());
        // The sniff may over-read into the body (loopback delivers the
        // whole hello in one recv); the magic test only looks at the
        // first 4 bytes.
        bool is_rdma = head.size() >= hello_magic_len &&
                       std::memcmp(head.data(), hello_magic, hello_magic_len) == 0;

        auto ctx = make_lw_shared<connection_context>();
        ctx->local = local;
        ctx->remote = remote;

	if (is_rdma) {
            std::vector<char> body = co_await read_body(fd, head);
            stack_log.debug("accept: body {} bytes", body.size());
            if (body.size() == hello_body_len) {
                std::vector<char> hello = std::move(head);
                hello.insert(hello.end(), body.begin(), body.end());
                lw_shared_ptr<rdma_endpoint> ep =
                    co_await rdma_endpoint::accept(_dev, std::move(fd), remote,
                                                   hello.data(), _handshake_timeout);
                stack_log.debug("accept: rdma endpoint {}", static_cast<void*>(ep.get()));
                if (ep) {
                    ctx->v = std::move(ep);
                    co_return accept_result{connected_socket(
                        std::make_unique<rdma_connected_socket_impl>(std::move(ctx))),
                        remote};
                }
                // Client declined during handshake: not reachable (the
                // magic matched), treat as protocol error below.
                throw std::runtime_error("rdma: server handshake failed");
            }
            // Short body: the peer died or is not RDMA after all.
            ctx->v = std::move(fd);
            ctx->prefix = std::move(head);
            ctx->prefix.insert(ctx->prefix.end(), body.begin(), body.end());
            co_return accept_result{connected_socket(
                std::make_unique<rdma_connected_socket_impl>(std::move(ctx))),
                remote};
        }

        // Plain TCP connection: downgrade on the same socket.
        ctx->v = std::move(fd);
        ctx->prefix = std::move(head);
        co_return accept_result{connected_socket(
            std::make_unique<rdma_connected_socket_impl>(std::move(ctx))),
            remote};
    }

    void abort_accept() override {
        _lfd.shutdown(SHUT_RD, pollable_fd::shutdown_kernel_only::yes);
    }

    socket_address local_address() const override {
        return _lfd.get_file_desc().get_address();
    }
};

// ---------------------------------------------------------------------------
// client socket
// ---------------------------------------------------------------------------
class rdma_socket_impl final : public socket_impl {
    rdma_device* _dev;
    lowres_clock::duration _handshake_timeout;
    pollable_fd _fd;
    bool _reuseaddr = false;
    const int _sock_flags;

    future<pollable_fd> tcp_connect(socket_address sa, socket_address local) {
        if (local.is_unspecified()) {
            local = net::inet_address(sa.addr().in_family());
        }
        _fd = pollable_fd(file_desc::socket(sa.u.sa.sa_family, _sock_flags, 0));
        _fd.get_file_desc().setsockopt(SOL_SOCKET, SO_REUSEADDR, int(_reuseaddr));
        if (!local.is_wildcard() && !local.is_af_unix()) {
            _fd.get_file_desc().bind(local.u.sa, local.length());
        }
        co_await internal::posix_connect(_fd, sa, local);
        // Return a copy, NOT a move: shutdown() must be able to abort
        // the established connection later (rpc client::stop relies on
        // it). pollable_fd shares the underlying state (refcounted), so
        // the endpoint, the fallback path and this socket all hold
        // references to the same fd.
        co_return _fd;
    }

public:
    rdma_socket_impl(rdma_device* dev, lowres_clock::duration timeout)
        : _dev(dev), _handshake_timeout(timeout)
        , _sock_flags(SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK) {}

    future<connected_socket> connect(socket_address sa, socket_address local,
                                     transport proto = transport::TCP) override {
        pollable_fd fd = co_await tcp_connect(sa, local);
        auto ctx = make_lw_shared<connection_context>();
        ctx->local = fd.get_file_desc().get_address();
        ctx->remote = sa;

        std::vector<char> prefix;
        std::optional<lw_shared_ptr<rdma_endpoint>> ep =
            co_await rdma_endpoint::connect(_dev, fd, sa, _handshake_timeout,
                                            prefix);
        if (!ep) {
            // Server declined RDMA: continue as plain TCP on the SAME
            // socket. The pre-read reply bytes belong to the stream.
            ctx->v = std::move(fd);
            ctx->prefix = std::move(prefix);
            co_return connected_socket(
                std::make_unique<rdma_connected_socket_impl>(std::move(ctx)));
        }
        ctx->v = std::move(*ep);
        co_return connected_socket(
            std::make_unique<rdma_connected_socket_impl>(std::move(ctx)));
    }

    void set_reuseaddr(bool reuseaddr) override {
        _reuseaddr = reuseaddr;
    }
    bool get_reuseaddr() const override { return _reuseaddr; }
    void shutdown() override {
        if (_fd) {
            try {
                _fd.shutdown(SHUT_RDWR);
            } catch (std::system_error& e) {
                if (e.code().value() != ENOTCONN) {
                    throw;
                }
            }
        }
    }
};

} // anonymous namespace

// ---------------------------------------------------------------------------
// the stack
// ---------------------------------------------------------------------------
class rdma_network_stack final : public network_stack {
    // The option group is owned by the reactor's option machinery and
    // outlives the stack; keep a typed reference only.
    const rdma_stack_options& _opts;
    std::unique_ptr<rdma_device> _device;
    std::optional<gid_selection> _selection;

public:
    explicit rdma_network_stack(const program_options::option_group& opts)
        : _opts(static_cast<const rdma_stack_options&>(opts)) {}

    future<> initialize() override {
        // Shard 0 selects the device/GID; the choice is broadcast so
        // every shard opens the same device.
        if (!_selection) {
            gid_selection sel = co_await rdma_device::select_device(_opts);
            _selection = sel;
        }
        _device = std::make_unique<rdma_device>(_opts);
        co_await _device->init_shard(*_selection);
        // Stop the event-mode channel watcher during exit tasks, while
        // the reactor can still resume it; otherwise it stays parked on
        // readable() forever and leaks its frame.
        internal::at_exit([dev = _device.get()] { return dev->stop(); });
    }

    server_socket listen(socket_address sa, listen_options opts) override {
        if (sa.is_unspecified()) {
            sa = inet_address(inet_address::family::INET);
        }
        lowres_clock::duration timeout =
            std::chrono::milliseconds(_opts.handshake_timeout_ms.get_value());
        pollable_fd lfd = internal::posix_listen(sa, opts);
        if (opts.tcp_only) {
            // Caller-declared plain-TCP port (e.g. external client
            // traffic alongside an internal RDMA port): no sniffing,
            // no handshake wait, full posix socket semantics.
            return server_socket(std::make_unique<rdma_tcp_only_server_socket_impl>(
                std::move(lfd)));
        }
        return server_socket(std::make_unique<rdma_server_socket_impl>(
            std::move(lfd), _device.get(), timeout));
    }

    ::seastar::socket socket() override {
        lowres_clock::duration timeout =
            std::chrono::milliseconds(_opts.handshake_timeout_ms.get_value());
        return ::seastar::socket(std::make_unique<rdma_socket_impl>(_device.get(), timeout));
    }

    net::udp_channel make_udp_channel(const socket_address&) override {
        throw std::runtime_error("udp is not supported over the rdma stack");
    }
    net::datagram_channel make_unbound_datagram_channel(sa_family_t) override {
        throw std::runtime_error("datagram channels are not supported over the rdma stack");
    }
    net::datagram_channel make_bound_datagram_channel(const socket_address&) override {
        throw std::runtime_error("datagram channels are not supported over the rdma stack");
    }

    bool has_per_core_namespace() override { return false; }

    statistics stats(unsigned) override { return statistics{}; }
    void clear_stats(unsigned) override {}
};

network_stack_entry register_rdma_stack() {
    return network_stack_entry{
        "rdma",
        std::make_unique<rdma_stack_options>(),
        [](const program_options::option_group& opts) {
            return make_ready_future<std::unique_ptr<network_stack>>(
                std::make_unique<rdma_network_stack>(opts));
        },
        false};
}

} // namespace seastar::net::rdma

#endif // SEASTAR_HAVE_RDMA
