/*
 * Copyright 2026 Seastar contributors
 */

//
// rdma_verbs_mock: a loopback mock of the verbs provider for unit
// tests (the test machine has no RDMA hardware).
//
// The mock models:
//   - one virtual device with active port, GID table and caps;
//   - MRs backed by real malloc'd memory: post_send validates that
//     every SGE's (addr, len, lkey) falls inside a registered MR;
//   - RC QPs with a RESET->INIT->RTR->RTS state machine (mask
//     validation, illegal transitions return EINVAL);
//   - QP pairing via qp_num: a send on QP A delivers a RECV
//     completion to its paired peer's CQ, and a SEND completion
//     (honoring SIGNALED suppression) to A's SQ;
//   - completion queues plus a comp_channel driven by an eventfd (so
//     the real pollable_fd-based watcher works);
//   - req_notify_cq armed semantics.
//
// Delivery is synchronous inside post_send (both endpoints of a test
// connection live on the same shard).
//
// Delivery is synchronous inside post_send (both endpoints of a test
// connection live on the same shard).
//

#pragma once

#ifdef SEASTAR_HAVE_RDMA

#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "net/rdma-verbs.hh"

namespace seastar::net::rdma {

// Install the mock globally. Must run before the engine starts.
// Returns the previous provider (to restore).
const verbs* install_verbs_mock();

// Test helpers on the mock's state.
namespace rdma_mock {

// Total number of currently-alive mock QPs / CQs / MRs (leak checks).
size_t live_qps();
size_t live_cqs();
size_t live_mrs();

// Total ibv_reg_mr calls served so far (MR-cache effectiveness checks).
uint64_t reg_mr_calls();

// Same, but only registrations NOT matching the buffer-pool region
// geometry (i.e. the direct/zcopy path). Use this for MR-cache
// assertions: pool region growth during the measured window is
// legitimate and would otherwise make them flaky.
uint64_t direct_reg_mr_calls();

// Pair two QPs as if they were connected peers (what real hardware
// does via the wire handshake parameters). Both QPs must be in RTR.
void pair_qps(qp_h a, qp_h b);

// Inject a failure into the next call of the named operation
// ("create_qp", "post_send", ...). The call returns the given errno.
void fail_next(const std::string& op, int err = EINVAL);

// Drop the injected failures (also done automatically after firing).
void clear_failures();

// Reset all mock state (between test cases).
void reset();

} // namespace rdma_mock

} // namespace seastar::net::rdma

#endif // SEASTAR_HAVE_RDMA

