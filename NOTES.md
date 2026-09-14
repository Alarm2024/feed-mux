# Bot 350 follow-up (feed-mux landed)

feed-mux now publishes Titan hop-1 hunt data to Redis. Bot 350 must consume it for hunt (replacing Jupiter-only quotes on the Titan path).

## What 350 should read

| Redis key | Use |
|-----------|-----|
| `mux:titan:size_board` | `/titan` pin board — per-size `hop1_age_ms`, venue, amounts |
| `mux:titan:hop1:SOL-USDC:<size_lamports>` | Hunt quote row (schema `mux.titan.hop1.v1`, TTL ~2s) |
| `mux:titan:hop1_served` | Hop-1 delivery counter for `/titan` (not `mux:titan:served`) |

`mux:titan:served` stays **proxy-session only** (local WS relay on `:19001`). Do not repurpose it for hop-1.

## Suggested 350 changes

1. **`/titan` card:** show `size_board` when JSON parses; display `hop1_served > 0` from `mux:titan:hop1_served`.
2. **Hunt path:** for configured SOL-USDC sizes, read `mux:titan:hop1:SOL-USDC:<pin_lamports>` instead of Jupiter when row exists and `hop1_age_ms` is within TTL.
3. **Fallback:** if row missing/expired, keep existing Jupiter behavior and surface a named skip reason.

## Out of scope (feed-mux)

- KEEP / arb-bot paths — never shared
- Triton gRPC (`ENABLE_TRITON_GRPC`) — unchanged
- Send / execution — eyes and delivery only

See README **Titan Redis keys** for full JSON schemas.
