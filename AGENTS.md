# AGENTS

## Goal

Build a minimal DIY streaming backpack over `iroh`.

The system should:

- read fixed-size UDP packets from a local encoder
- prepend a tiny header: `[session_id: u32][seq: u64]`
- duplicate every packet over multiple independent `iroh` connections
- bind each connection to an explicit interface provided by CLI
- receive duplicated packets on the server
- drop duplicates
- forward payloads in order
- feed clean output into FFmpeg, then RTMP

This is application-level multipath duplication, not true QUIC multipath.

## V1 Scope

Do:

- fixed-size UDP ingest
- explicit `--interfaces ...` on the client
- one `iroh` connection per interface
- minimal packet codec in `crates/protocol`
- simple dedupe on the server
- simple in-order forwarding on the server
- UDP output for FFmpeg
- FFmpeg subprocess support

Do not add yet:

- metrics
- timestamps
- FEC
- retransmissions
- automatic interface discovery
- interface scoring
- Rust FFmpeg bindings
- advanced control plane

## Crate Responsibilities

### `crates/protocol`

- define the 12-byte header
- encode packet = header + payload
- decode packet = header + payload split

### `crates/client`

- parse CLI
- require `--interfaces ...`
- allow direct `--addr` inputs and optional `--relay` overrides
- use the built-in default relay when `--relay` is omitted
- resolve IPv4 for each named interface at startup
- fail clearly for invalid interface names
- fail clearly when an interface has no IPv4 address
- listen on local UDP for encoder packets
- create one `iroh` endpoint/connection per interface
- duplicate every packet on all active paths

### `crates/bench`

- mirror the client’s remote dialing and interface binding model
- generate synthetic fixed-size packets internally
- pace sending by configured throughput, for example `--throughput-mbps`
- use the same minimal packet header so server dedupe/reorder still works

### `crates/server`

- accept datagrams from all client paths
- parse packet header
- dedupe by `(session_id, seq)`
- keep the first copy
- reorder with a small in-memory buffer
- forward payloads in order
- feed clean payloads to FFmpeg over local UDP
- expose RTMP output via `--rtmp`
- allow stable server identity via `--secret`

## Constraints

- keep the implementation direct
- prefer the smallest design that works
- keep one client, one logical stream, one server for v1
- look up interface IPs once at startup
- avoid speculative features outside the scope above
