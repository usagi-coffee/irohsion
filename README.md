# irohsion

Multipath UDP stream over `iroh`.

## Server

```bash
cargo run -p server -- \
  --tui \
  --port 1935 \
  --secret <server_secret_hex> \
  --flow-idle-reset-secs 30 \
  --max-reorder-delay-ms 100 \
  --relay <relay_url> \
  --relay <relay_url>
```

### Notes
- `--flow-idle-reset-secs` resets the expected sequence after an idle period so a new sender burst can start cleanly.
- `--max-reorder-delay-ms` is the live-stream gap tolerance. If a missing packet or fragment blocks newer complete packets longer than this, the server skips the missing sequence and forwards the buffered data behind it.

## Client

```bash
cargo run -p client -- \
  --tui \
  --port 5000 \
  --endpoint <server_id> \
  --relay <server_relay_url> \
  --split-threshold-bytes 100 \
  --tc-backlog-poll-ms 500 \
  --tc-backlog-degrade-bytes 65536 \
  --tc-backlog-recover-bytes 16384 \
  --remote \
  --remote-name irohsion \
  --interfaces eth0:<secret_hex> eth1
```

### Notes
- If `--port` is omitted, the client binds a random local UDP port. Each interface may optionally carry its own inline secret as `iface:<secret_hex>`.
- When `--split-threshold-bytes` is set, UDP packets larger than the threshold are split across interfaces; packets at or below the threshold are duplicated across all interfaces.
- The `tc` backlog options control a Linux qdisc heuristic for split mode: if any interface backlog reaches `--tc-backlog-degrade-bytes`, the client falls back to redundant full-packet sends for future packets; it returns to split mode when the worst observed backlog is at or below `--tc-backlog-recover-bytes`.
- Send errors also force future packets into redundant mode.
- `--remote` exposes a Linux/BlueZ BLE control service for phones. Media still goes over iroh networking; BLE is only for monitoring and tuning the running client.

### Remote BLE Control

Connect from the phone with nRF Connect or Web Bluetooth:

- Service UUID: `8b4f82c8-4f5a-4e26-8f29-d1f0c0d10001`
- Status characteristic UUID: `8b4f82c8-4f5a-4e26-8f29-d1f0c0d10002`
- Control characteristic UUID: `8b4f82c8-4f5a-4e26-8f29-d1f0c0d10003`

Read the status characteristic for JSON containing the endpoint, relay/address hints, effective strategy, packet counters, and per-interface targets.

Write a JSON patch to the control characteristic:

```json
{"mode":"redundant"}
```

```json
{"mode":"split","monitor_packets":true,"targets_mbps":{"eth0":4.0,"wwan0":2.5}}
```

Modes are `auto`, `split`, and `redundant`. `targets_mbps` are currently stored and reported for future dynamic split weighting.

## Bench

Mirrors the client connection model but synthesizes payloads instead of reading local UDP.

```bash
cargo run -p bench -- \
  --endpoint <server_id> \
  --relay <server_relay_url> \
  --interfaces eth0 eth1 \
  --throughput-mbps 20 \
  --packet-size 1316 \
  --split-threshold-bytes 100 \
  --duration-secs 30
```

## Identity

```bash
cargo run -p identity
```

This prints ad-hoc `secret=...` and `endpoint=...` pairs for reuse with the client and server.
