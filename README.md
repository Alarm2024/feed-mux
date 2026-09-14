# feed-mux

✝️🧿🪬 owner — elghaly / Wyndham Heaven

3️⃣🧿5️⃣ assistant

Local fan-out feed multiplexer for **Bot 350** (primary consumer) and the **python 35-bot** (may *know* and *say* in نشرة/eyes).

---

## Hard rules (non-negotiable)

| Rule | Detail |
|------|--------|
| **Consumers** | Bot 350 (primary) + python 35-bot only |
| **NEVER rust** | KEEP / arb-bot / Garden-Angel-35035 must **never** know about or connect to feed-mux |
| **No shared hop** | No shared Titan/Triton path with KEEP — mux is a **separate service** |
| **KEEP stays off** | `feed.redis_url` on KEEP remains **disabled forever** |
| **Separate rate limits** | Triton gRPC and Titan WS are rate-limited **independently** |
| **Secrets** | Redis AUTH supported; credentials are **never logged** |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        feed-mux                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Chainstack │  │   Helius    │  │ Triton gRPC (limit) │  │
│  │  RPC/WS stub│  │ backup stub │  │ Titan WS  (limit)   │  │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘  │
│         └────────────────┴────────────────────┘               │
│                          │ normalize / mux                   │
│                          ▼                                   │
│                   Redis PUBLISH (AUTH)                       │
│                   channel: feed:350                          │
└──────────────────────────┬──────────────────────────────────┘
                           │
           ┌───────────────┴───────────────┐
           ▼                               ▼
    ┌─────────────┐                 ┌─────────────┐
    │   Bot 350   │                 │ python 35-bot│
    │  (primary)  │                 │ نشرة / eyes  │
    └─────────────┘                 └─────────────┘

    ╳ KEEP / arb-bot / rust  —  MUST NOT connect or subscribe
```

**Status:** upstream connectors run as **stubs when `DRY_RUN=true`**. With **`DRY_RUN=false`** and feature flags enabled, live Triton gRPC and Titan WS connect with **independent rate limits**, publish mux Redis metrics, and expose **local relays** so Bot 350 never opens a second Yellowstone subscribe.

---

## FR production enable (Bot 350 only)

On the FR host (`ubuntu-35035-FR`), edit `/opt/feed-mux/.env` (mode `600`). Set flags only — **never commit or log secret values**:

```bash
# Core
DRY_RUN=false
REDIS_URL=redis://:<password>@127.0.0.1:6379/0
REDIS_CHANNEL=feed:350

# Triton gRPC (Yellowstone via mux — Bot 350 uses TRITON_LOCAL_BIND, not upstream URL)
ENABLE_TRITON_GRPC=true
TRITON_GRPC_URL=<your-triton-grpc-endpoint>
TRITON_GRPC_TOKEN=<your-triton-x-token-if-required>
TRITON_LOCAL_BIND=127.0.0.1:19000
TRITON_RATE_LIMIT_RPS=25

# Titan WS (Bot 350 cheap_2 wallet only — never KEEP)
ENABLE_TITAN_WS=true
TITAN_WS_URL=<your-titan-ws-url>
TITAN_WALLET_PUBKEY=<bot-350-cheap_2-pubkey>
TITAN_LOCAL_BIND=127.0.0.1:19001
TITAN_RATE_LIMIT_RPS=15
# Optional: override board sizes (lamports, comma-separated)
# TITAN_BOARD_SIZES_LAMPORTS=100000000,500000000,1000000000,2500000035,5000000000
```

Restart:

```bash
sudo systemctl restart feed-mux
sudo systemctl status feed-mux
```

Verify (no secrets in output):

```bash
curl -s http://127.0.0.1:8787/health | jq '.upstreams[] | select(.name=="triton_grpc" or .name=="titan_ws")'
redis-cli GET mux:meta:triton_up    # expect 1 when Triton live
redis-cli GET mux:meta:titan_up     # expect 1 when Titan connected
redis-cli GET mux:titan:size_board  # JSON per-size hop ages
redis-cli GET mux:titan:hop1:2500000035  # hop-1 quote at 2.5 SOL pin
ss -ltn | grep -E '19000|19001'     # local gRPC/WS relays listening
```

Bot 350 must **not** subscribe to Triton upstream directly — point Yellowstone client at `127.0.0.1:19000` (mux relay). See [`NOTES.md`](NOTES.md) for consumer follow-up.

---

## Quick start

### Prerequisites

- Rust 1.82+ (or use Docker)
- Redis (optional in dry-run; required for live fan-out)

### Run locally (dry-run default)

```bash
cp .env.example .env
cargo run
curl http://127.0.0.1:8787/health
curl -X POST http://127.0.0.1:8787/publish \
  -H 'Content-Type: application/json' \
  -d '{"event":"manual.test","source":"curl"}'
```

### Docker Compose

```bash
docker compose up --build
./scripts/smoke.sh
```

### systemd

See [`deploy/feed-mux.service`](deploy/feed-mux.service). Install binary to `/opt/feed-mux/`, place `.env` (mode `600`), then:

```bash
sudo cp deploy/feed-mux.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now feed-mux
```

---

## HTTP API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness + upstream stub status |
| `POST` | `/publish` | Publish a JSON event to Redis fan-out (mocked when `DRY_RUN=true`) |

### `GET /health` example

```json
{
  "status": "ok",
  "dry_run": true,
  "redis_configured": true,
  "redis_channel": "feed:350",
  "upstreams": [
    { "name": "chainstack", "enabled": false, "connected": false, "mode": "stub/dry-run" },
    { "name": "helius", "enabled": false, "connected": false, "mode": "stub/dry-run" },
    { "name": "triton_grpc", "enabled": false, "connected": false, "mode": "stub/dry-run", "rate_limit_rps": 25 },
    { "name": "titan_ws", "enabled": false, "connected": false, "mode": "stub/dry-run", "rate_limit_rps": 15 }
  ],
  "ts": "2026-09-07T00:00:00Z"
}
```

---

## Environment variables

Copy [`.env.example`](.env.example). Key settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `BIND_ADDR` | `127.0.0.1:8787` | HTTP listen address (loopback by default) |
| `DRY_RUN` | `true` | Mock upstreams + skip Redis writes |

**Listen surface:** On ocean-drop (and by default everywhere), admin/metrics HTTP (`/health`, `/publish`) binds to **localhost only**. Set `BIND_ADDR=0.0.0.0:8787` only when you intentionally expose the port publicly — always place auth or a reverse proxy in front. Docker Compose publishes `127.0.0.1:8787` on the host; the container still uses `0.0.0.0:8787` internally so bridge port mapping works.
| `REDIS_URL` | — | Redis URL with AUTH, e.g. `redis://:password@host:6379/0` |
| `REDIS_CHANNEL` | `feed:350` | Pub/sub channel for Bot 350 consumers |
| `MOCK_PUBLISH_INTERVAL_SECS` | `30` | Periodic mock tick in dry-run (`0` = off) |
| `ENABLE_CHAINSTACK` | `false` | Chainstack RPC/WS stub |
| `ENABLE_HELIUS` | `false` | Helius backup RPC (live when `DRY_RUN=false`) |
| `HELIUS_RPC_URL` | — | Helius HTTPS RPC URL (include API key in URL). Required when `ENABLE_HELIUS=true` and `DRY_RUN=false`. |
| `ENABLE_TRITON_GRPC` | `false` | Triton gRPC feed (live when `DRY_RUN=false`) |
| `TRITON_GRPC_URL` | — | Triton gRPC endpoint (`https://...`). Required when `ENABLE_TRITON_GRPC=true` and `DRY_RUN=false`. |
| `TRITON_GRPC_TOKEN` | — | Triton `x-token` header value when required. Never logged. |
| `TRITON_LOCAL_BIND` | `127.0.0.1:19000` | Local Triton gRPC relay (Bot 350 Yellowstone client) |
| `TRITON_RATE_LIMIT_RPS` | `25` | **Separate** Triton rate limit |
| `ENABLE_TITAN_WS` | `false` | Titan WebSocket feed (live when `DRY_RUN=false`) |
| `TITAN_WS_URL` | — | Titan Direct WebSocket URL (may include auth query param). Required when `ENABLE_TITAN_WS=true` and `DRY_RUN=false`. |
| `TITAN_WALLET_PUBKEY` | — | Solana wallet public key (base58). **Required** for live Titan — quotes are subscribed and compiled for this account. Use the **Bot 350 `cheap_2` wallet pubkey only**; never a KEEP / Garden Angel wallet. |
| `TITAN_RATE_LIMIT_RPS` | `15` | **Separate** Titan rate limit |
| `TITAN_BOARD_SIZES_LAMPORTS` | `100000000,...,5000000000` | Comma-separated SOL lamport sizes for hop-1 board |
| `TITAN_LOCAL_BIND` | `127.0.0.1:19001` | Local Titan WS relay (Bot 350 MUX_TITAN_BIND probe) |

### Redis mux schema (Bot 350 eyes + hunt)

| Key | Type | Description |
|-----|------|-------------|
| `mux:meta:heartbeat_ms` | string (ms) | 1 Hz liveness tick |
| `mux:meta:titan_up` | `"0"` / `"1"` | Titan WS upstream connected |
| `mux:meta:triton_up` | `"0"` / `"1"` | Triton gRPC upstream connected |
| `mux:titan:frames` | counter | Titan quote frames received |
| `mux:titan:decoded` | counter | Successfully decoded quotes |
| `mux:titan:served` | counter | Downstream-only (Bot 350 increments) |
| `mux:titan:pairs_live` | `"0"` / `"1"` | Set after real quote decode |
| `mux:titan:size_board` | JSON | Per-size hop-1 ages for KEEP-like SOL-USDC board |
| `mux:titan:hop1:<SIZE_LAMPORTS>` | JSON | Hop-1 quote delivery for hunt at that size |
| `mux:triton:frames` | counter | Triton slot/update frames received |
| `mux:triton:last_frame_ms` | string (ms) | Last Triton frame timestamp |

**Hop-1 quote JSON** (`mux:titan:hop1:2500000035` example):

```json
{
  "size_lamports": 2500000035,
  "input_mint": "So11111111111111111111111111111111111111112",
  "output_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
  "hop": 1,
  "out_amount": 42000000,
  "provider": "Titan",
  "quote_ms": 1694700000000,
  "age_ms": 120,
  "source": "titan_ws"
}
```

**Size board JSON** (`mux:titan:size_board`):

```json
{
  "updated_ms": 1694700000000,
  "pair": "SOL-USDC",
  "input_mint": "So11111111111111111111111111111111111111112",
  "output_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
  "sizes": [
    { "size_lamports": 2500000035, "hop1_age_ms": 120, "hop1_last_ms": 1694700000000 }
  ]
}
```

Pub/sub channel `feed:350` emits `titan.hop1.quote` and `triton.slot` events alongside legacy counters.

### Upstream activation matrix

| Upstream | Dry-run (`DRY_RUN=true`) | Live (`DRY_RUN=false`) |
|----------|--------------------------|-------------------------|
| Helius | Stub poll only | Requires `ENABLE_HELIUS=true` + `HELIUS_RPC_URL` |
| Triton gRPC | Stub poll, rate-limited | Requires `ENABLE_TRITON_GRPC=true` + `TRITON_GRPC_URL`, rate-limited |
| Titan WS | Stub poll, rate-limited | Requires `ENABLE_TITAN_WS=true` + `TITAN_WS_URL` + `TITAN_WALLET_PUBKEY`, rate-limited |

If Titan is enabled live without `TITAN_WALLET_PUBKEY`, feed-mux **refuses to connect** and logs: *wallet pubkey required for quote subscribe/compile*.

Upstream URL vars (`CHAINSTACK_*`, `HELIUS_*`, `TRITON_GRPC_URL`, `TITAN_WS_URL`) are only read when their feature flag is enabled.

---

## Tests & smoke

```bash
cargo test
./scripts/smoke.sh   # requires running server
```

Integration tests cover `/health`, `/publish`, dry-run defaults, and redacted logging (no Redis password in config summary).

---

## What this is NOT

- **Not** part of KEEP, arb-bot, or any rust trading stack
- **Not** a replacement for KEEP's internal feed configuration
- **Not** wired to shared Titan/Triton infrastructure used by KEEP

Consumers subscribe to Redis channel `feed:350` on **this** mux instance only.

---

## License

MIT — see [LICENSE](LICENSE).
