/*
 * Copyright 2026 Seastar contributors
 */

#pragma once

#include <seastar/net/net.hh>
#include <seastar/util/program-options.hh>

#ifdef SEASTAR_HAVE_RDMA

namespace seastar {

struct network_stack_entry;

namespace net::rdma {

/// RDMA stack configuration.
///
/// The stack runs RDMA over a TCP connection: TCP carries the initial
/// handshake (and stays open as a liveness detector), while data flows
/// over an RC QP pair established by the handshake. Connections to
/// peers that do not speak the RDMA handshake downgrade to TCP on the
/// same connection.
struct rdma_stack_options : public program_options::option_group {
    /// RDMA device name (empty = first device with an active port).
    program_options::value<std::string> device;
    /// RDMA port number on the device.
    program_options::value<unsigned> port;
    /// GID index to use (empty = auto-select, preferring RoCEv2-style
    /// GIDs at the end of the table).
    program_options::value<std::string> gid_index;
    /// Prefer busy-polling the completion queues from the reactor loop
    /// instead of waiting for completion-channel events (higher CPU,
    /// lower latency).
    program_options::value<bool> polling;
    /// Number of pre-created QP/CQ pairs per shard. The pool grows on
    /// demand; recycled QPs are returned here.
    program_options::value<unsigned> qp_pool_size;
    /// SQ and RQ depth of each QP.
   program_options::value<unsigned> qp_depth;
    /// Receive block size (also the negotiation unit for the peer's
    /// send granularity).
    program_options::value<unsigned> block_size;
    /// Minimum payload bytes for a receive buffer to be handed out
    /// zero-copy; smaller messages are copied.
    program_options::value<unsigned> zcopy_min_size;
    /// Maximum completions reaped per CQ poll.
    program_options::value<unsigned> cqe_poll_batch;
    /// Handshake timeout in milliseconds.
    program_options::value<unsigned> handshake_timeout_ms;
    /// Maximum registered-memory regions per size class per shard.
    program_options::value<unsigned> max_regions;

    rdma_stack_options();
};

network_stack_entry register_rdma_stack();

} // namespace net::rdma

} // namespace seastar

#else

namespace seastar::net::rdma {
inline constexpr bool rdma_supported = false;
}

#endif // SEASTAR_HAVE_RDMA
