# AGENTS

## Goals

Multipath UDP stream over `iroh`.

The system should:

- read fixed-size UDP packets from a socket
- prepend a tiny header: `[session_id: u32][seq: u64]`
- duplicate every packet over multiple independent `iroh` connections
- bind each connection to an explicit interface provided by CLI
- receive duplicated packets on the server
- drop duplicates
- forward payloads in order to target socket
- feed clean output into FFmpeg, then RTMP

## Crate Responsibilities

### `crates/protocol`

- defines the protocol header
- encode packet = header + payload
- decode packet = header + payload split

### `crates/client`

- require `--interfaces ...`
- allow direct `--addr` inputs and optional `--relay` overrides
- use the built-in default relay when `--relay` is omitted
- resolve IPv4 for each named interface at startup
- fail clearly for invalid interface names
- fail clearly when an interface has no IPv4 address
- listen on local UDP for UDP packets
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
- dedupes by `(session_id, seq)`
- keep the first copy
- reorder with a small in-memory buffer
- forward payloads in order
- forwards clean payloads to local UDP socket

## Constraints

- keep the implementation direct
- prefer the smallest design that works
