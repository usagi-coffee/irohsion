# irohsion

Minimal DIY streaming backpack over `iroh`.

## Workspace

`crates/protocol`
Shared 12-byte packet header codec.

`crates/client`
Reads fixed-size UDP packets locally, prepends `[session_id: u32][seq: u64]`, and duplicates every packet over one `iroh` connection per CLI-selected interface.

`crates/bench`
Generates synthetic fixed-size packets at a configured throughput for throttling and relay/direct-path testing.

`crates/server`
Accepts packets from all paths, drops duplicates, forwards in order, and pushes clean payloads into FFmpeg for RTMP output.

## Build

```bash
cargo build
```

## Server

```bash
cargo run -p server -- \
  --rtmp rtmp://live.twitch.tv/app/$TWITCH_STREAM_KEY
```

Add `--tui` for a live ratatui dashboard.

The server prints connection coordinates like:

```text
server_id=<endpoint-id>
server_addr=ip:203.0.113.10:4242
server_addr=relay:https://relay.example
```

Optional stable server identity:

```bash
cargo run -p server -- \
  --secret <64-hex-secret> \
  --rtmp rtmp://live.twitch.tv/app/$TWITCH_STREAM_KEY
```

Optional FFmpeg controls:

```bash
cargo run -p server -- \
  --rtmp rtmp://live.twitch.tv/app/$TWITCH_STREAM_KEY \
  --ffmpeg-input-udp 127.0.0.1:6000 \
  --ffmpeg-bin ffmpeg
```

## Client

```bash
cargo run -p client -- \
  --port 5000 \
  --endpoint <server_id> \
  --addr <server_ip:port> \
  --interfaces eth0 eth1
```

Add `--tui` for a live ratatui dashboard.

If `--relay` is omitted, the client uses the default relay:

```text
https://euc1-1.relay.n0.iroh-canary.iroh.link
```

Rules in v1:

- `--interfaces` is explicit and required.
- `--addr` is optional when relay connectivity is enough.
- `--relay` is optional because the client has a built-in default relay.
- every named interface must exist.
- every named interface must have an IPv4 address at startup.
- every packet is duplicated on every active interface path.

## Bench

```bash
cargo run -p bench -- \
  --endpoint <server_id> \
  --interfaces eth0 eth1 \
  --throughput-mbps 8 \
  --packet-size 1316 \
  --duration-secs 30
```

Optional direct and relay overrides:

```bash
cargo run -p bench -- \
  --endpoint <server_id> \
  --addr <server_ip:port> \
  --relay <server_relay_url> \
  --interfaces eth0 eth1 \
  --throughput-mbps 20
```

`bench` mirrors the client connection model but synthesizes payloads instead of reading local UDP.

## End-to-end demo

Backpack side:

```bash
ffmpeg -re -i input.mp4 -c copy -f mpegts udp://127.0.0.1:5000?pkt_size=1316
```

Server side:

```bash
cargo run -p server -- \
  --rtmp rtmp://live.twitch.tv/app/$TWITCH_STREAM_KEY
```

The server spawns FFmpeg and feeds it over local UDP internally.
