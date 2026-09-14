# feed-mux offline release — mux-3da0c50

Prebuilt Linux x86_64 ELF for **Ubuntu 24.04 / glibc 2.39** (FR ocean-drop). Built from merge tip after **#13** (Titan hop-1 + Triton, no quote fabrication).

## Build identity

| Field | Value |
|-------|-------|
| **Version** | `feed-mux 0.1.0+3da0c50` |
| **Merge commit** | `3da0c50d38b230db2e04540395e20c26ad014927` |
| **Tip commit** | `2485dd2` (included in merge) |
| **PR** | #13 — Titan hop-1 + Triton only |
| **Target** | Linux x86_64, max GLIBC symbol ≤ 2.39 |
| **Max GLIBC symbol** | 2.34 (verified via `objdump -T`) |
| **Build host** | Ubuntu 24.04.4 LTS, Rust 1.89.0 |

## Download

**GitHub release (draft):** [mux-3da0c50](https://github.com/Alarm2024/feed-mux/releases/tag/mux-3da0c50)

| Asset | SHA-256 |
|-------|---------|
| `feed-mux-linux-gnu` | `668e5a3dd026aaa4cfe22dfa75ce9b9c27d7d6249e475cfd79ae103e58f6d6b3` |

Verify on droplet before install:

```bash
sha256sum -c <<< '668e5a3dd026aaa4cfe22dfa75ce9b9c27d7d6249e475cfd79ae103e58f6d6b3  feed-mux-linux-gnu'
```

## FR land steps (document only — do NOT run from agent)

> **Eyes-only.** Preserve existing `.env`; do not paste secrets. No `cargo` on droplet.

### 1. Discover live paths

On FR, confirm the unit paths before copying (repo template uses `/opt/feed-mux`; production often uses `~/350-bot/feed-mux`):

```bash
systemctl cat feed-mux | grep -E 'ExecStart|WorkingDirectory|EnvironmentFile'
```

Typical production layout:

| Item | Path |
|------|------|
| Binary | `~/350-bot/feed-mux/feed-mux` |
| Environment | `~/350-bot/feed-mux/.env` |
| systemd `ExecStart` | path from `systemctl cat feed-mux` |

### 2. Download and verify

```bash
cd ~/350-bot/feed-mux
# download feed-mux-linux-gnu from release mux-3da0c50
sha256sum -c <<< '668e5a3dd026aaa4cfe22dfa75ce9b9c27d7d6249e475cfd79ae103e58f6d6b3  feed-mux-linux-gnu'
cp feed-mux feed-mux.bak.$(date +%Y%m%d%H%M)
mv feed-mux-linux-gnu feed-mux
chmod +x feed-mux
```

**Do not overwrite `.env`.** Only replace the binary.

### 3. Restart services

```bash
sudo systemctl restart feed-mux
sleep 2
sudo systemctl restart 350-bot
```

### 4. Prove healthy (eyes)

With live upstreams enabled in `.env` (`DRY_RUN=false`, `ENABLE_TITAN_WS=true`, etc.):

| Check | Expected |
|-------|----------|
| Redis key `mux:titan:size_board` | EXISTS (SET with `mux.titan.size_board.v1` JSON) |
| `mux:titan:hop1_served` | Counter climbing |
| `/health` upstream `triton_grpc` | `connected: true` when `ENABLE_TRITON_GRPC=true` |

Example eyes probes (no secrets):

```bash
redis-cli EXISTS mux:titan:size_board
redis-cli GET mux:titan:hop1_served
curl -s http://127.0.0.1:8787/health | jq '.upstreams[] | select(.name=="triton_grpc" or .name=="titan_ws")'
```

## What changed in this build

- **#10 Titan hop-1:** publishes per-size hop-1 quotes and `mux:titan:size_board` to Redis for Bot 350 hunt
- **#11 Triton:** live gRPC feed when enabled (no quote fabrication)
- Listen surface hardening from #9 (default loopback bind)
