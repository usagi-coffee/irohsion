# AGENTS

< DO NOT KEEP BACKWARD COMPATIBLITY >

## Goals

Multipath UDP stream over `iroh`.

The system should:

- read fixed-size UDP packets from a socket
- prepend a tiny packed header: `[sequence: 26 bits][fragment: 3 bits][fragments: 3 bits]`
- support redundant mode, where each packet is duplicated over multiple independent `iroh` connections
- support split mode, where packets above a threshold are fragmented across interfaces and reassembled on the server
- support round-robin mode for one-copy packet distribution across active paths
- fall back to redundant mode for future packets when a client path appears degraded
- bind each connection to an explicit interface provided by CLI
- receive duplicated or fragmented packets on the server
- drop duplicates
- reassemble fragments
- forward payloads in order to target socket, skipping stale gaps after a configured live-stream reorder delay
- currently supports optional bounded server-side NACK repair for live gaps
- currently supports optional live XOR FEC parity frames that can recover one missing full media packet per group
- forward responses back to the client
- expose optional phone-friendly BLE remote control from the client for monitoring and tuning path strategy
- expose TUI/status metrics for live receive, forwarding, skips, repair, FEC, connection resets, and return-path pressure drops

## Crate Responsibilities

### `crates/protocol`

- defines the protocol header
- header is 4 bytes total using `modular-bitfield`
- `sequence` is 26 bits
- reserve the maximum 26-bit sequence value as `FEC_SEQUENCE`
- normal media sequence numbers wrap at `MAX_MEDIA_SEQUENCE = FEC_SEQUENCE - 1`
- `fragment` is a zero-based 3-bit fragment index
- `fragments` is a 3-bit total fragment count, so split mode supports up to 7 paths
- encode packet = header + payload
- decode packet = header + payload split
- reject invalid fragment metadata, for example `fragments == 0` or `fragment >= fragments`
- currently defines bounded NACK repair request frames as `sequence u32 le + missing_mask u8`
- reject repair requests with invalid length, sequence, or missing mask
- currently defines FEC frames carried as normal iroh datagrams with:
  - outer packet header `sequence = FEC_SEQUENCE`, `fragment = 0`, `fragments = 1`
  - payload magic/version `IFEC1`
  - base media sequence
  - group packet count
  - per-packet payload lengths
  - XOR parity bytes
- cap FEC group size at 32 packets
- reject malformed FEC frames, invalid base sequences, invalid group sizes, and impossible payload lengths

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
- support round-robin mode, sending each packet on one active path
- expose `--split-threshold-bytes`; packets larger than the threshold use split mode when strategy allows it
- packets at or below `--split-threshold-bytes` are always sent redundantly
- support `--mtu`; packets at or above MTU split even when the path strategy is redundant
- support `--mpeg-ts-chunk-bytes`; MPEG-TS payloads larger than the chunk size are split into sequence-numbered MPEG-TS chunks before path strategy is applied
- split mode fragments a packet across paths according to remote-configurable split percentages
- unset split percentages mean an even split across all active paths
- normalize configured split percentages so they do not need to sum to exactly 100
- store remote-configurable target Mbps values per interface for future dynamic split weighting
- support strategy modes:
  - `auto`: split when allowed, but degrade to redundant based on heuristics
  - `split`: force split behavior for packets above the threshold
  - `redundant`: force full duplicate sends
  - `round-robin`: force one-copy distribution across active paths
- poll Linux `tc -s qdisc show dev <interface>` with `--tc-backlog-poll-ms`
- in auto mode, fall back to redundant when any path backlog reaches `--tc-backlog-degrade-bytes`
- in auto mode, recover to split when the worst observed backlog is at or below `--tc-backlog-recover-bytes`
- support optional `--tc-qdisc-reset`
- when qdisc reset is enabled, run `tc qdisc replace dev <interface> root fq_codel` only when:
  - qdisc backlog reaches `--tc-qdisc-reset-backlog-bytes`
  - fresh server health reports the same path at or below `--tc-qdisc-reset-max-server-mbps`
  - per-interface reset cooldown `--tc-qdisc-reset-cooldown-ms` has elapsed
- run qdisc reset against the real Linux device name, not the display label
- qdisc reset is privileged; failures must be logged rather than treated as fatal
- fall back to redundant for future packets on send errors
- currently keeps a bounded repair cache for live packets/fragments
- expose `--repair-cache-ms` and `--repair-cache-packets`
- accept server repair requests on iroh uni streams
- resend only requested cached fragments/full packets, bounded by cache TTL and packet count
- dedupe repeated repair requests for a short window to avoid resend storms
- expose `--fec-group-packets`; `0` means adaptive FEC, `2..=32` pins a fixed XOR parity group size
- expose `--no-fec` to disable client FEC entirely
- currently sends FEC parity frames redundantly over all active paths
- adaptive FEC currently uses smaller groups under path lag or low server Mbps and larger groups when fresh server health looks clean
- record FEC overhead in send counters
- support `--remote` to run an in-process Linux/BlueZ BLE GATT control server
- BLE remote exposes status and accepts JSON control patches for mode, packet monitoring, target Mbps, and split percentages
- BLE remote can expose preview/status data, OBS integration status, chat history, and transport hints where configured
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
- expose `--fec-group-packets`; `0` disables FEC, `2..=32` sends synthetic XOR parity frames matching the client FEC format
- wrap generated media sequence numbers at `MAX_MEDIA_SEQUENCE`

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
- detect confirmed sequence restarts from a new connection and reset flow state
- expose optional server-side `--repair`
- when `--repair` is enabled, currently request missing whole packets or missing fragments before the live reorder deadline
- expose `--repair-request-interval-ms`; require it to be lower than `--max-reorder-delay-ms`
- currently sends repair requests over bounded iroh uni streams to active reply routes
- keep repair optional server-side; clients can accept repair requests without requiring a client-side enable flag
- currently decodes FEC frames identified by reserved `FEC_SEQUENCE`
- currently keeps FEC state bounded by frame count and payload cache count
- currently recovers at most one missing full media payload per FEC group
- currently feeds recovered payloads through the same in-order live reorder/forwarding path
- currently drops FEC frames that cannot recover before normal live skip behavior advances
- current FEC and repair behavior is live-only; future experiments may change this deliberately
- track the following in the TUI:
  - skipped packets
  - never-received skips
  - late-after-skip arrivals
  - incomplete-fragment skips
  - FEC recoveries
  - repair requests
  - return-path pressure drops
  - flow resets
  - connection resets

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

## Engineering Notes

- keep the implementation direct
- prefer the smallest design that works
- BLE is only for monitoring/control/provisioning; media transport stays on `iroh`
- split mode is for combining bandwidth, not maximizing reliability
- redundant mode is currently the primary reliability mode for live packets
- repair and FEC currently act as live-only helpers with bounded waiting and bounded state
- reliable ordered streams can introduce head-of-line blocking for live media; account for that tradeoff when experimenting with stream-based transport or repair
- FEC is currently datagram-based; separate FEC connections or other channel designs are valid experiments if they are explicit and measurable
- changing Linux qdisc state is currently opt-in and guarded by explicit CLI flags and clear logging
- prefer bounded caches and bounded per-tick work for repair, FEC, and reorder behavior unless deliberately testing a different reliability model
