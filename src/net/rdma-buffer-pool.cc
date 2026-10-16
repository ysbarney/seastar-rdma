/*
 * Copyright 2026 Seastar contributors
 */

#include "rdma-buffer-pool.hh"

#ifdef SEASTAR_HAVE_RDMA

#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <new>
#include <stdexcept>
#include <system_error>

#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

namespace seastar::net::rdma {

static logger bufpool_log("rdma-pool");

namespace {

// The shard-local pools. One pool object per shard lives inside
// rdma_device; the deleter below needs to reach it from a raw pointer
// captured in the buffer, so pools self-register here.
thread_local rdma_buffer_pool* local_pool = nullptr;

// Deleter for pooled buffers. Runs on whatever shard drops the
// buffer; if that is not the home shard, hop back.
void release_pooled_block_local(char* block) {
    if (local_pool) {
        local_pool->return_block(block);
    }
}

} // anonymous namespace

void release_pooled_block(char* block, shard_id home) {
    if (home == this_shard_id()) {
        release_pooled_block_local(block);
        return;
    }
    // Fire-and-forget hop home (same pattern as packet::free_on_cpu).
    (void)smp::submit_to(home, [block] {
        release_pooled_block_local(block);
    });
}

size_t rdma_buffer_pool::class_of(size_t size) {
    for (size_t i = 0; i < num_classes; ++i) {
        if (size <= class_sizes[i]) {
            return i;
        }
    }
    return num_classes - 1;
}

void rdma_buffer_pool::add_region(size_t class_idx) {
    size_t class_size = class_sizes[class_idx];
    size_t count = blocks_per_region(class_size);
    size_t region_size = class_size * count;

    // Region start must be suitably aligned for the block class.
    size_t alignment = std::max(alignof(std::max_align_t), class_size >= 4096 ? size_t(4096) : size_t(64));
    void* mem = ::operator new(region_size + alignment, std::align_val_t(alignment));
    char* start = reinterpret_cast<char*>(
        (reinterpret_cast<uintptr_t>(mem) + alignment - 1) & ~(alignment - 1));

    const verbs* v = verbs_provider();
    mr_h mr = v->reg_mr(_pd, start, region_size,
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_RELAXED_ORDERING);
    if (!mr) {
        // Some devices do not support RELAXED_ORDERING; retry plain
        // LOCAL_WRITE (same fallback as brpc's RdmaRegisterMemory).
        mr = v->reg_mr(_pd, start, region_size, IBV_ACCESS_LOCAL_WRITE);
    }
    if (!mr) {
        ::operator delete(mem, std::align_val_t(alignment));
        throw std::system_error(errno, std::system_category(),
                                "rdma_buffer_pool: ibv_reg_mr failed");
    }

    region r;
    r.start = start;
    r.size = region_size;
    r.block_size = class_size;
    r.block_count = count;
    r.lkey = mr->lkey;
    r.mr = mr;
    for (size_t i = 0; i < count; ++i) {
        auto* node = reinterpret_cast<free_node*>(start + i * class_size);
        node->next = _free[class_idx];
        _free[class_idx] = node;
    }
    _free_count[class_idx] += count;
    _registered_bytes += region_size;
    _regions.push_back(r);
    bufpool_log.debug("added region: class={} count={} total_regions={}",
                      class_size, count, _regions.size());
}

void rdma_buffer_pool::init(pd_h pd, unsigned max_regions) {
    _pd = pd;
    _max_regions_per_class = max_regions;
    local_pool = this;
    for (size_t i = 0; i < num_classes; ++i) {
        add_region(i);
    }
}

void rdma_buffer_pool::return_block(char* block) {
    // Called only on the owning shard.
    for (auto& r : _regions) {
        if (block >= r.start && block < r.start + r.size) {
            size_t class_idx = class_of(r.block_size);
            auto* node = reinterpret_cast<free_node*>(block);
            node->next = _free[class_idx];
            _free[class_idx] = node;
            _free_count[class_idx]++;
            return;
        }
    }
    // A block that is not ours can only be a bug.
    std::abort();
}

uint32_t rdma_buffer_pool::lkey_of(const char* block) const noexcept {
    for (auto& r : _regions) {
        if (block >= r.start && block < r.start + r.size) {
            return r.lkey;
        }
    }
    std::abort();
}

uint32_t rdma_buffer_pool::lkey_of_safe(const char* block) const noexcept {
    for (auto& r : _regions) {
        if (block >= r.start && block < r.start + r.size) {
            return r.lkey;
        }
    }
    return 0;
}

rdma_buffer_pool::~rdma_buffer_pool() {
    const verbs* v = verbs_provider();
    for (auto& r : _regions) {
        v->dereg_mr(r.mr);
        // Free with the same alignment used at allocation time.
        size_t alignment = std::max(alignof(std::max_align_t), r.block_size >= 4096 ? size_t(4096) : size_t(64));
        ::operator delete(reinterpret_cast<void*>(r.start), std::align_val_t(alignment));
    }
    _regions.clear();
    if (local_pool == this) {
        local_pool = nullptr;
    }
}

namespace rdma_detail {

// make_deleter-compatible functor.
struct block_returner {
    char* block;
    shard_id home;
    void operator()() noexcept {
        release_pooled_block(block, home);
    }
};

} // namespace rdma_detail

temporary_buffer<char> rdma_buffer_pool::borrow(size_t size) {
    size_t class_idx = class_of(size);
    if (!_free[class_idx]) {
        size_t regions_of_class = 0;
        for (auto& r : _regions) {
            if (r.block_size == class_sizes[class_idx]) {
                regions_of_class++;
            }
        }
        if (regions_of_class >= _max_regions_per_class) {
            throw std::system_error(ENOMEM, std::system_category(),
                                    "rdma_buffer_pool: region cap reached");
        }
        add_region(class_idx);
    }
    auto* node = _free[class_idx];
    _free[class_idx] = node->next;
    _free_count[class_idx]--;
    char* block = reinterpret_cast<char*>(node);
    return temporary_buffer<char>(block, class_sizes[class_idx],
                                  make_deleter(rdma_detail::block_returner{block, this_shard_id()}));
}

} // namespace seastar::net::rdma

#endif // SEASTAR_HAVE_RDMA
