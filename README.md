# irohsion

Multipath UDP stream over `iroh`.

## Client

```bash
cargo run -p client -- \
  --port 5000 \
  --endpoint <server_id> \
  --addr <server_ip:port> \
  --interfaces eth0 eth1
```

Add `--tui` for a live ratatui dashboard.

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

