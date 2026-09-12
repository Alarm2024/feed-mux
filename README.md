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

**MVP status:** upstream connectors run as **stubs when `DRY_RUN=true`** (mock publish, no live connections). With **`DRY_RUN=false`** and feature flags enabled, live upstreams connect using the configured endpoints. Triton gRPC and Titan WS are rate-limited **independently**.

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
    { "name": "triton_grpc", "enabled": false, "connected": false, "mode": "stub/dry-run", "rate_limit_rps": 50 },
    { "name": "titan_ws", "enabled": false, "connected": false, "mode": "stub/dry-run", "rate_limit_rps": 30 }
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
| `TRITON_GRPC_URL` | — | Triton gRPC endpoint. Required when `ENABLE_TRITON_GRPC=true` and `DRY_RUN=false`. |
| `TRITON_RATE_LIMIT_RPS` | `50` | **Separate** Triton rate limit |
| `ENABLE_TITAN_WS` | `false` | Titan WebSocket feed (live when `DRY_RUN=false`) |
| `TITAN_WS_URL` | — | Titan Direct WebSocket URL (may include auth query param). Required when `ENABLE_TITAN_WS=true` and `DRY_RUN=false`. |
| `TITAN_WALLET_PUBKEY` | — | Solana wallet public key (base58). **Required** for live Titan — quotes are subscribed and compiled for this account. Use the **Bot 350 `cheap_2` wallet pubkey only**; never a KEEP / Garden Angel wallet. |
| `TITAN_RATE_LIMIT_RPS` | `30` | **Separate** Titan rate limit |

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
