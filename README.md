Seastar
=======

[![Test](https://github.com/scylladb/seastar/actions/workflows/tests.yaml/badge.svg)](https://github.com/scylladb/seastar/actions/workflows/tests.yaml)
[![Version](https://img.shields.io/github/tag/scylladb/seastar.svg?label=version&colorB=green)](https://github.com/scylladb/seastar/releases)
[![License: Apache2](https://img.shields.io/github/license/scylladb/seastar.svg)](https://github.com/scylladb/seastar/blob/master/LICENSE)
[![n00b issues](https://img.shields.io/github/issues/scylladb/seastar/n00b.svg?colorB=green)](https://github.com/scylladb/seastar/labels/n00b)

Introduction
------------

Seastar is an event-driven framework that allows you to write non-blocking,
asynchronous code in a relatively straightforward manner (once understood).
It is based on [futures](https://en.wikipedia.org/wiki/Futures_and_promises).

Building Seastar
--------------------

For more details and alternative workflows, read [HACKING.md](./HACKING.md).

Assuming that you would like to use system packages (RPMs or DEBs) for Seastar's dependencies, first install them:

```
$ sudo ./install-dependencies.sh
```

Then configure (in "release" mode):

```
$ ./configure.py --mode=release
```
Then compile:

```
$ ninja -C build/release
```

If compilation fails with an error such as `g++: internal compiler error: Killed (program cc1plus)`,
try giving GCC more memory. You can limit the number of parallel jobs with `-j1` and/or allocate at least 4 GiB of RAM to your
machine.

If you're missing a dependency of Seastar, then it is possible to have the configuration process fetch a version of the dependency locally for development.

For example, to fetch `fmt` locally, configure Seastar like this:

```
$ ./configure.py --mode=dev --cook fmt
```

`--cook` can be repeated many times for selecting multiple dependencies.


Build modes
----------------------------------------------------------------------------

The `configure.py` script is a wrapper around CMake. The `--mode` argument
maps to `CMAKE_BUILD_TYPE` and supports the following modes:

|          | CMake mode          | Debug info | Optimi&shy;zations | Sanitizers   | Allocator | Checks   | Use for                                |
| -------- | ------------------- | ---------- | ------------------ |------------- | --------- | -------- | -------------------------------------- |
| debug    | `Debug`             | Yes        | `-O0`              | ASAN, UBSAN  | System    | All      | gdb                                    |
| release  | `RelWithDebInfo`    | Yes        | `-O3`              | None         | Seastar   | Asserts  | production                             |
| dev      | `Dev` (Custom)      | No         | `-O1`              | None         | Seastar   | Asserts  | build and test cycle                   |
| sanitize | `Sanitize` (Custom) | Yes        | `-Os`              | ASAN, UBSAN  | System    | All      | second level of tests, track down bugs |

Note that Seastar is more sensitive to allocators and optimizations than
usual. As a rough rule of thumb, `release` is twice as fast as `dev`, 150
times as fast as `sanitize`, and 300 times as fast as `debug`.

Using Seastar from its build directory (without installation)
----------------------------------------------------------------------------

It's possible to consume Seastar directly from its build directory with CMake or `pkg-config`.

We'll assume that the Seastar repository is located in a directory at `$seastar_dir`.


Via `pkg-config`:

```
$ g++ my_app.cc $(pkg-config --libs --cflags --static $seastar_dir/build/release/seastar.pc) -o my_app
```

and with CMake using the `Seastar` package:


`CMakeLists.txt` for `my_app`:

```
set (CMAKE_CXX_STANDARD 23)

find_package (Seastar REQUIRED)

add_executable (my_app
  my_app.cc)

target_link_libraries (my_app
  Seastar::seastar)
```

```
$ mkdir $my_app_dir/build
$ cd $my_app_dir/build
$ cmake -DCMAKE_PREFIX_PATH="$seastar_dir/build/release;$seastar_dir/build/release/_cooking/installed" -DCMAKE_MODULE_PATH=$seastar_dir/cmake $my_app_dir
```

The `CMAKE_PREFIX_PATH` values ensure that CMake can locate Seastar and its compiled submodules. The `CMAKE_MODULE_PATH` value ensures that CMake can use Seastar's CMake scripts to locate its dependencies.

Using an installed Seastar
--------------------------------

You can also consume Seastar after it has been installed to the filesystem.

**Important:**

- Seastar works with a customized version of DPDK, so by default builds and installs the DPDK submodule to `$build_dir/_cooking/installed`

First, configure the installation path:

```
$ ./configure.py --mode=release --prefix=/usr/local
```

Then run the `install` target:

```
$ ninja -C build/release install
```

Then consume it from `pkg-config`:

```
$ g++ my_app.cc $(pkg-config --libs --cflags --static seastar) -o my_app
```

or consume it with the same `CMakeLists.txt` as before but with a simpler CMake invocation:

```
$ cmake ..
```

(If Seastar has not been installed to a "standard" location like `/usr` or `/usr/local`, then you can invoke CMake with `-DCMAKE_PREFIX_PATH=$my_install_root`.)

There are also instructions for building on any host that supports [Docker](doc/building-docker.md).

Use of [DPDK](https://www.dpdk.org/) is [optional](doc/building-dpdk.md).

RDMA Support
------------

Seastar includes an optional RDMA networking stack that provides ultra-low-latency,
zero-copy data transfer using RDMA verbs over RoCEv2. The stack runs RDMA over a
TCP connection: TCP carries the initial handshake (and stays open as a liveness detector),
while data flows over an RC (Reliable Connected) QP pair established by the handshake.

Connections to peers that do not speak the RDMA handshake automatically downgrade to
plain TCP on the same connection.

### Hardware Requirements

For optimal RDMA performance:

* **NIC**: InfiniBand or RoCEv2-capable network interface card (10G or 40G recommended)
* **Cores**: As many as you need; RDMA scales well with multiple cores and NUMA systems
* **Memory**: Sufficient memory for registered buffers (see configuration options below)

### Installation

Ensure your system has RDMA libraries installed:

```bash
# Ubuntu/Debian
sudo apt-get install libibverbs-dev librdmacm-dev infiniband-diags

# RHEL/CentOS
sudo yum install libibverbs-utils librdmacm-utils rdma-core-devel
```

### Building with RDMA

Configure Seastar to enable RDMA support:

```bash
# Using system packages
sudo ./install-dependencies.sh

# Enable RDMA (default: enabled on systems with RDMA support)
./configure.py --mode=release
ninja -C build/release
```

The build system automatically detects RDMA support and compiles the RDMA stack when
`SEASTAR_HAVE_RDMA` is defined.

### Using the RDMA Stack in Your Application

#### Via CMake

```cmake
set (CMAKE_CXX_STANDARD 23)

find_package (Seastar REQUIRED)

add_executable (my_app
  my_app.cc)

target_link_libraries (my_app
  Seastar::seastar)

# Link against RDMA stack (if available)
target_link_libraries (my_app PRIVATE Seastar::seastar_rdma)
```

#### Via pkg-config

```bash
g++ my_app.cc $(pkg-config --libs --cflags --static seastar_rdma) -o my_app
```

### Configuration Options

When using the RDMA stack, you can configure several parameters:

```bash
./configure.py \
  --mode=release \
  --rdma-device=<device_name> \    # RDMA device (empty = first active)
  --rdma-port=<port_number> \      # Port number on device (default: 1)
  --rdma-gid-index=<index> \       # GID index (empty = auto)
  --rdma-polling=false \           # True for busy-polling CQs (lower latency, higher CPU)
  --rdma-qp-depth=128 \            # SQ/RQ depth per QP
  --rdma-qp-pool-size=256 \        # Pre-created QP pool per shard
  --rdma-block-size=8192 \         # Receive block size
  --rdma-zcopy-min-size=262144 \   # Minimum payload for zero-copy direct-send
  --rdma-max-regions=64            # Max registered memory regions per size class
```

### Using RDMA in Your Code

The RDMA stack integrates with Seastar's network API. Connections are automatically
upgraded to RDMA when both peers support it:

```cpp
#include <seastar/core/reactor.hh>
#include <seastar/net/api.hh>
#include <seastar/net/rdma-stack.hh>

using namespace seastar;
using namespace seastar::net;

seastar::future<> rdma_server() {
    // Register the RDMA stack
    auto stack = register_rdma_stack();
    unsigned port = 9000;

    return do_with(stack, port, [] (auto& stack, unsigned port) {
        return listen(stack, socket_address{ipv4_addr{127, 0, 0, 1}, port})
            .then([] (listening_socket ls) {
                return ls.accept();
            })
            .then([] (auto&& conn) {
                std::cout << "Connection established over RDMA!" << std::endl;
                // Handle connection - automatic zero-copy if RDMA supported
                return conn.input_stream().read_until_exactly(1024)
                    .then([] (temporary_buffer<char> buf) {
                        std::cout << "Received: " << buf.size() << " bytes" << std::endl;
                    });
            });
    });
}
```

#### Key Features

* **Automatic upgrade**: Connections automatically upgrade to RDMA when both peers support it
* **Graceful fallback**: Peers that don't support RDMA automatically downgrade to TCP
* **Zero-copy transfers**: Large messages (≥256KB by default) use direct-send path
* **Flow control**: Built-in receive-window and send-window management
* **Liveness detection**: TCP socket remains open as a heartbeat for connection health

#### Performance Tips

1. **Enable busy-polling** for lowest latency (increases CPU usage):
   ```bash
   --rdma-polling=true
   ```

2. **Increase receive block size** for larger zero-copy messages:
   ```bash
   --rdma-block-size=65536
   ```

3. **Adjust QP depth** based on your workload:
   ```bash
   --rdma-qp-depth=512
   ```

4. **Use multiple cores** on NUMA systems - bind NIC to specific cores for best performance

### Debugging and Monitoring

Enable debug logging for RDMA operations:

```bash
RUST_LOG=seastar::net::rdma=debug ./your_app
```

Common debugging commands:

```bash
# List RDMA devices
ibv_devinfo

# Check RDMA device status
ibstat

# Monitor RDMA connections
ibv_devinfo -v
```

#### <a id="cxx-standard"></a>Seastar's C++ standard: C++23 or C++26

Seastar supports both C++23 and C++26. The build defaults to the latest
standard supported by your compiler, but can be explicitly selected with
the `--c++-standard` configure option, e.g., `--c++-standard=23`,
or, if using CMake directly, by setting the `CMAKE_CXX_STANDARD` CMake
variable.

See the [compatibility statement](doc/compatibility.md) for more information.

Getting started
---------------

There is a [mini tutorial](doc/mini-tutorial.md) and a [more comprehensive one](doc/tutorial.md).

The documentation is available on the [web](https://docs.seastar.io/master/index.html).


Resources
---------

* Seastar Development Mailing List: Discuss challenges, propose improvements, send
  code contributions (patches), and get help from experienced developers.
  Subscribe or browse archives: [here](https://groups.google.com/g/seastar-dev)
  (or email seastar-dev@googlegroups.com).
* GitHub Discussions: For more casual conversations and quick questions, consider
  using the Seastar project's [discussions on GitHub](https://github.com/scylladb/seastar/discussions).
* Issue Tracker: File bug reports on the project's [issue tracker](https://github.com/scylladb/seastar/issues).

Learn more about Seastar on the main [project website](https://seastar.io/).

The Native TCP/IP Stack
-----------------------

Seastar comes with its own [userspace TCP/IP stack](doc/native-stack.md) for better performance.

Recommended hardware configuration for Seastar
----------------------------------------------

* CPUs - As many as you need. Seastar is well suited to multicore and NUMA systems.
* NICs - As fast as possible; we recommend 10G or 40G cards. It is possible to use
       1G cards too, but you may be limited by their capacity.
       In addition, the more hardware queues per CPU, the better for Seastar.
       Otherwise we have to emulate that in software.
* Disks - Fast SSDs with a high number of IOPS.
* Client machines - Usually a single client machine cannot fully load our servers.
       Both memaslap (memcached) and wrk (httpd) may be unable to overload their matching
       server counterparts. We recommend running clients on machines other than the servers
       and using several of them.

Projects using Seastar
----------------------------------------------

* [cpv-cql-driver](https://github.com/cpv-project/cpv-cql-driver): C++ driver for Cassandra/Scylla based on the Seastar framework
* [cpv-framework](https://github.com/cpv-project/cpv-framework): A web framework written in C++ based on the Seastar framework
* [Redpanda](https://www.redpanda.com/): A Kafka-compatible streaming data platform for mission-critical systems
* [Scylla](https://github.com/scylladb/scylla): A fast and reliable NoSQL data store compatible with Cassandra and DynamoDB
* [smf](https://github.com/smfrpc/smf): The fastest RPC in the West
* [Ceph - Crimson](https://github.com/ceph/ceph): Next-generation OSD (Object Storage Daemon) implementation based on the Seastar framework
