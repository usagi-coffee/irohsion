# AGENTS

## Goals

Multipath UDP stream over `iroh`.

The system should:

- read fixed-size UDP packets from a socket
- prepend a tiny packed header: `[sequence: 26 bits][fragment: 3 bits][fragments: 3 bits]`
- support redundant mode, where each packet is duplicated over multiple independent `iroh` connections
- support split mode, where packets above a threshold are fragmented across interfaces and reassembled on the server
- fall back to redundant mode for future packets when a client path appears degraded
- bind each connection to an explicit interface provided by CLI
- receive duplicated or fragmented packets on the server
- drop duplicates
- reassemble fragments
- forward payloads in order to target socket, skipping stale gaps after a configured live-stream reorder delay
- forward responses back to the client
- expose optional phone-friendly BLE remote control from the client for monitoring and tuning path strategy

## Crate Responsibilities

### `crates/protocol`

- defines the protocol header
- header is 4 bytes total using `modular-bitfield`
- `sequence` is 26 bits and wraps client-side
- `fragment` is a zero-based 3-bit fragment index
- `fragments` is a 3-bit total fragment count, so split mode supports up to 7 paths
- encode packet = header + payload
- decode packet = header + payload split
- reject invalid fragment metadata, for example `fragments == 0` or `fragment >= fragments`

### `crates/client`

- require `--interfaces ...`
- allow direct `--addr` inputs and optional `--relay` overrides
- use the built-in default relay when `--relay` is omitted
- resolve IPv4 for each named interface at startup
- fail clearly for invalid interface names
- fail clearly when an interface has no IPv4 address
- listen on local UDP for UDP packets
- create one `iroh` endpoint/connection per interface
- duplicate every packet on all active paths in redundant mode
- expose `--split-threshold-bytes`; packets larger than the threshold use split mode when strategy allows it
- packets at or below `--split-threshold-bytes` are always sent redundantly
- split mode fragments a packet across paths according to remote-configurable split percentages
- unset split percentages mean an even split across all active paths
- normalize configured split percentages so they do not need to sum to exactly 100
- store remote-configurable target Mbps values per interface for future dynamic split weighting
- support strategy modes:
  - `auto`: split when allowed, but degrade to redundant based on heuristics
  - `split`: force split behavior for packets above the threshold
  - `redundant`: force full duplicate sends
- poll Linux `tc -s qdisc show dev <interface>` with `--tc-backlog-poll-ms`
- in auto mode, fall back to redundant when any path backlog reaches `--tc-backlog-degrade-bytes`
- in auto mode, recover to split when the worst observed backlog is at or below `--tc-backlog-recover-bytes`
- fall back to redundant for future packets on send errors
- support `--remote` to run an in-process Linux/BlueZ BLE GATT control server
- BLE remote exposes status and accepts JSON control patches for mode, packet monitoring, target Mbps, and split percentages
- BLE UUIDs:
  - service `8b4f82c8-4f5a-4e26-8f29-d1f0c0d10001`
  - status read `8b4f82c8-4f5a-4e26-8f29-d1f0c0d10002`
  - control write `8b4f82c8-4f5a-4e26-8f29-d1f0c0d10003`

### `crates/bench`

- mirror the client’s remote dialing and interface binding model
- generate synthetic fixed-size packets internally
- pace sending by configured throughput, for example `--throughput-mbps`
- use the same minimal packet header so server dedupe/reorder still works
- expose `--split-threshold-bytes`
- split packets above the threshold across paths and send smaller packets redundantly

### `crates/server`

- accept datagrams from all client paths
- parse packet header
- dedupes by `sequence`
- keep the first copy
- collect split fragments by `sequence`
- assemble fragments in fragment-index order before forwarding
- reorder with a small in-memory buffer
- forward payloads in order
- forwards clean payloads to local UDP socket
- expose `--flow-idle-reset-secs` to clear sequence expectation after idle periods
- expose `--max-reorder-delay-ms` for live-stream gap tolerance
- skip missing or incomplete sequences after `--max-reorder-delay-ms` so buffered newer packets can continue flowing
- track skipped packets in the TUI

### `packages/remote`

- Svelte 5 static web app for phone control
- uses Web Bluetooth to connect to the client `--remote` BLE service
- OLED-friendly black UI
- shows red/green connection indicator
- reads current status from the BLE status characteristic
- writes control patches to switch `auto`/`split`/`redundant`
- displays one card per interface
- provides target Mbps sliders per interface
- provides split percentage sliders in split mode
- defaults unset split percentages to an even split in the UI

## Constraints

- keep the implementation direct
- prefer the smallest design that works
- BLE is only for monitoring/control/provisioning; media transport stays on `iroh`
- split mode is for combining bandwidth, not maximizing reliability
