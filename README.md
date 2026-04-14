# irohsion

Multipath UDP stream over `iroh`.

## Server

```bash
cargo run -p server -- \
  --tui \
  --port 1935 \
  --secret <server_secret_hex> \
  --relay <relay_url> \
  --relay <relay_url>
```

`--port` is the local UDP destination port the server forwards reordered packets to and reads replies from.

## Client

```bash
cargo run -p client -- \
  --tui \
  --port 5000 \
  --endpoint <server_id> \
  --relay <server_relay_url> \
  --interfaces eth0:<secret_hex> eth1
```

If `--port` is omitted, the client binds a random local UDP port. Each interface may optionally carry its own inline secret as `iface:<secret_hex>`.

## Bench

```bash
cargo run -p bench -- \
  --endpoint <server_id> \
  --relay <server_relay_url> \
  --interfaces eth0 eth1 \
  --throughput-mbps 20 \
  --packet-size 1316 \
  --duration-secs 30
```

`bench` mirrors the client connection model but synthesizes payloads instead of reading local UDP.

## Identity

```bash
cargo run -p identity
```

This prints ad-hoc `secret=...` and `endpoint=...` pairs for reuse with the client and server.
