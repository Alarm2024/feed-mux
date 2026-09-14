# Bot 350 follow-up (out of scope for feed-mux)

feed-mux now publishes Titan hop-1 quote delivery keys and `mux:titan:size_board`.
Bot 350 hunt still reads Jupiter-only until the consumer PR lands.

## Redis keys Bot 350 should consume

| Key | Type | Description |
|-----|------|-------------|
| `mux:titan:hop1:<SIZE_LAMPORTS>` | JSON string | Hop-1 quote for hunt at that SOL size (e.g. `mux:titan:hop1:2500000035`) |
| `mux:titan:size_board` | JSON string | Aggregated per-size hop-1 ages for the KEEP-like SOL-USDC board |
| `mux:meta:triton_up` | `"0"` / `"1"` | Triton gRPC upstream liveness |
| `mux:meta:titan_up` | `"0"` / `"1"` | Titan WS upstream liveness |

Pub/sub channel `feed:350` also emits `titan.hop1.quote` events with `redis_key` in the payload.

## Local binds (Bot 350 uses mux — no second Yellowstone subscribe)

| Bind | Default | Purpose |
|------|---------|---------|
| `TRITON_LOCAL_BIND` | `127.0.0.1:19000` | Local Triton gRPC relay |
| `TITAN_LOCAL_BIND` | `127.0.0.1:19001` | Local Titan WS relay |

## Suggested Bot 350 changes

1. Read `mux:titan:hop1:<size>` for configured cycle borrow sizes instead of Jupiter-only hunt path.
2. Use `mux:titan:size_board` in eyes/`/needs` for per-size hop freshness.
3. Point Yellowstone client at `TRITON_LOCAL_BIND` (not upstream Triton URL).
