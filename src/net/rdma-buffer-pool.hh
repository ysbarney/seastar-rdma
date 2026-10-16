/*
 * Copyright 2026 Seastar contributors
 */

//
// rdma-buffer-pool.hh: per-shard pool of ibv-reg_mr'd memory blocks.
//
// Port of brpc's block_pool (src/brpc/rdma/block_pool.cpp): blocks are
// carved out of a small number of large registered regions, so the
// lkey of any block is derivable from its address. Blocks are handed
// out as pooled temporary_buffers whose deleter returns them to this
// pool -- possibly round-tripping through smp::submit_to() when the
// buffer dies on a foreign shard.
//
// Unlike brpc we do not need a TLS cache: one pool per shard already
// means one pool per thread.
//

#pragma once

#ifdef SEASTAR_HAVE_RDMA

#include <cstddef>
#include <cstdint>
#include <vector>

#include <seastar/core/temporary_buffer.hh>

#include "rdma-verbs.hh"

namespace seastar::net::rdma {

class rdma_buffer_pool {
public:
    // Block size classes (same as brpc). Index 0 is the class used for
    // receive blocks and default send staging.
    static constexpr size_t num_classes = 3;
    static constexpr size_t class_sizes[num_classes] = {8 << 10, 64 << 10, 2 << 20};

    rdma_buffer_pool() = default;
    rdma_buffer_pool(const rdma_buffer_pool&) = delete;
    rdma_buffer_pool& operator=(const rdma_buffer_pool&) = delete;

    // Registers the first region of every class. `max_regions` caps
    // region count per class. Throws std::system_error on failure.
    void init(pd_h pd, unsigned max_regions);

    // Returns a registered buffer of exactly the class size >=
    // min(size, largest class). Grows the pool (up to the region cap)
    // when the freelist is empty; throws when the cap is hit.
    temporary_buffer<char> borrow(size_t size);

    // Number of regions (all classes).
    size_t registered_regions() const noexcept { return _regions.size(); }

    // Total registered bytes.
    size_t registered_bytes() const noexcept { return _registered_bytes; }
    
    // lkey of the region containing `block` (which must come from this
    // pool). Aborts on foreign pointers.
    uint32_t lkey_of(const char* block) const noexcept;
    
    // Non-aborting probe: is `block` inside a registered region of
    // this pool? Returns the lkey, or 0 when foreign.
    uint32_t lkey_of_safe(const char* block) const noexcept;
    
    // Return a borrowed block to its home pool. Must be called on the
    // owning shard (the buffer deleter guarantees this).
    void return_block(char* block);
    
    // Test/teardown: release all regions. All borrowed buffers must
    // have been returned.
    ~rdma_buffer_pool();

private:
    struct region {
        char* start = nullptr;
        size_t size = 0;
        size_t block_size = 0; // class size
        size_t block_count = 0;
        uint32_t lkey = 0;
        mr_h mr = nullptr;
    };

    // Free blocks of one class, threaded through the block itself.
    struct free_node {
        free_node* next;
    };

    static constexpr size_t blocks_per_region(size_t class_size) {
        // Enough blocks to amortize registration; small classes get
        // more blocks per region (brpc uses ~1GB initial, we scale
        // down: 8K->1MB/region, 64K->4MB, 2M->8MB).
        return class_size <= class_sizes[0] ? 128
             : class_size <= class_sizes[1] ? 64
                                            : 4;
    }

    pd_h _pd = nullptr;
    unsigned _max_regions_per_class = 16;
    std::vector<region> _regions;
    free_node* _free[num_classes] = {};
    size_t _free_count[num_classes] = {};
    size_t _registered_bytes = 0;

    static size_t class_of(size_t size);
    void add_region(size_t class_idx);
};

} // namespace seastar::net::rdma
