# Velum

Durable workflow orchestration platform in Go — event-sourced runs, task leasing, gRPC workers, timers, and idempotent completions.

**Module:** `github.com/0xrameshh/velum`

## Quick start (Docker)

Starts split control plane with **Redis wake dispatch** and **per-queue matchers** (`default` `:9090`, `email` `:9092`, `payments` `:9093`), plus `velum-history`, `velum-api`, and `velum-scheduler`.

```bash
docker compose up --build -d
make smoke   # greet + delayed_greet + order_saga
```

API listens on port **8080** by default. If that port is taken, set `VELUM_HOST_HTTP_PORT=8088` (or any free port) before `docker compose up` and `make smoke`:

```bash
VELUM_HOST_HTTP_PORT=8088 docker compose up --build -d
VELUM_HOST_HTTP_PORT=8088 make smoke
```

Or step by step:

```bash
make curl-start
sleep 2
make curl-status
```

### Timers demo (`delayed_greet`)

Workflow: `greet` → **sleep** (durable timer, no goroutine held) → `send_email`.

```bash
make curl-delayed-start
# wait for sleep_seconds (default 5) + activity time
make curl-delayed-status
```

Expect events: `TimerStarted` → `TimerFired` between greet and send_email.

## Local dev

**All-in-one** (fastest):

```bash
docker compose up -d postgres
make proto build
./bin/velum-migrate
./bin/velum   # API + gRPC + scheduler
./bin/velum-worker  # VELUM_TASK_QUEUE=default
VELUM_TASK_QUEUE=email VELUM_WORKER_ID=local-email ./bin/velum-worker
```

**Split binaries** (matches production Compose):

```bash
docker compose up -d postgres && make build && ./bin/velum-migrate
./bin/velum-history &
./bin/velum-api & ./bin/velum-matcher & ./bin/velum-scheduler &
make run-worker-default   # separate terminals for email/payments as needed
```

## API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness |
| `GET` | `/ready` | Readiness |
| `POST` | `/api/v1/namespaces/{ns}/workflows/{name}/start` | Start a workflow |
| `GET` | `/api/v1/namespaces/{ns}/runs/{run_id}` | Run + event history |

### Workflows

| Name | Steps |
|------|-------|
| `greet` | `greet` (default queue) → `send_email` (email queue) |
| `delayed_greet` | `greet` → timer (`sleep_seconds` in input) → `send_email` |
| `order_saga` | parallel `charge_card` + `reserve_stock` → `ship_order` → (on ship fail) compensate |

### Saga demo (`order_saga`)

Happy path:

```bash
make curl-saga-start
sleep 2
make curl-saga-status
```

Compensation path (ship fails, refunds/releases run):

```bash
make curl-saga-fail
sleep 3
make curl-saga-status
```

Input flags: `fail_charge`, `fail_reserve`, `fail_ship` (all simulated).

Input for `delayed_greet`:

```json
{"name": "Ramesh", "sleep_seconds": 5}
```

## gRPC WorkerService (`:9090`)

| RPC | Description |
|-----|-------------|
| `PollTask` | Lease next task on a queue |
| `RecordHeartbeat` | Extend lease while executing |
| `CompleteTask` | Idempotent completion |
| `FailTask` | Idempotent failure / retry |

Protos: `proto/velum/v1/worker.proto`, `proto/velum/v1/history.proto` — regenerate with `make proto`.

## HistoryService gRPC (`:9091`)

| RPC | Description |
|-----|-------------|
| `StartWorkflow` | Create run + schedule first step |
| `GetRun` | Run metadata + event history |
| `OnActivityCompleted` | Advance after task success |
| `OnActivityFailed` | Record failure event |
| `HandleTerminalFailure` | Fail workflow or start saga compensation |
| `OnTimerFired` | Advance after durable timer |

## Binaries

| Binary | Purpose |
|--------|---------|
| `velum-history` | Workflow state + event log (Postgres) |
| `velum-api` | HTTP API (history client only) |
| `velum-matcher` | gRPC task poll/complete/fail |
| `velum-scheduler` | Fire due timers |
| `velum-migrate` | Apply Postgres migrations (one-shot) |
| `velum-worker` | Execute activities |
| `velum` | All-in-one for local dev (`VELUM_ENABLE_*` flags) |

## Configuration

| Variable | Used by | Description |
|----------|---------|-------------|
| `VELUM_HTTP_ADDR` | api | HTTP listen (`:8080`) |
| `VELUM_GRPC_ADDR` | matcher, worker | Worker gRPC listen / dial |
| `VELUM_HISTORY_GRPC_ADDR` | history, api, matcher, scheduler | History gRPC listen / dial (`:9091`) |
| `VELUM_DATABASE_URL` | history, matcher, scheduler, migrate | PostgreSQL DSN |
| `VELUM_MIGRATE_ON_STARTUP` | history | Run migrations on boot (Compose uses `velum-migrate`) |
| `VELUM_SCHEDULER_POLL_EVERY` | scheduler | Timer poll interval |
| `VELUM_SCHEDULER_BATCH_SIZE` | scheduler | Max timers per tick |
| `VELUM_LEASE_RECLAIM_EVERY` | matcher | Reclaim expired task leases |
| `VELUM_TASK_QUEUE` | worker | Queue to poll |
| `VELUM_DISPATCH` | history, matcher, scheduler | `postgres` (default) or `redis` |
| `VELUM_REDIS_ADDR` | history, matcher, scheduler | Redis address when dispatch=redis |
| `VELUM_MATCHER_QUEUES` | matcher | Comma-separated queues this matcher serves (empty = all) |
| `VELUM_DISPATCH_WAIT` | matcher | Redis BRPOP wait before re-polling Postgres |
| `VELUM_ENABLE_EMBEDDED_WORKER` | `velum` only | In-process DB poller (dev) |

## Scaling (Phase 7)

Postgres remains the source of truth. Redis is an optional **wake signal** only:

- **Tasks:** history LPUSHes on create; matcher BRPOPs then leases from Postgres
- **Timers:** history publishes on create; scheduler wakes early instead of blind polling

Run one matcher per queue in Compose (`velum-matcher-default`, `velum-matcher-email`, `velum-matcher-payments`). Point each worker at its queue's matcher via `VELUM_GRPC_ADDR`.

Set `VELUM_DISPATCH=postgres` to disable Redis (legacy polling-only mode).

## Architecture

```mermaid
flowchart LR
  Client --> API[velum-api]
  API --> Hist[velum-history]
  Hist --> Redis[(Redis wake)]
  Hist --> PG[(PostgreSQL)]
  MatchD[matcher default] --> Hist
  MatchE[matcher email] --> Hist
  MatchP[matcher payments] --> Hist
  MatchD --> Redis
  MatchE --> Redis
  MatchP --> Redis
  MatchD --> PG
  Sched[velum-scheduler] --> Hist
  Sched --> Redis
  Sched --> PG
  W1[worker default] --> MatchD
  W2[worker email] --> MatchE
  W3[worker payments] --> MatchP
```

## Roadmap

- [x] Phase 2: gRPC workers, queues, idempotency
- [x] Phase 3: Timers / sleep workflows
- [x] Phase 4: Parallel branches + saga compensation
- [x] Phase 5: Split control-plane binaries
- [x] Phase 6: History gRPC service
- [x] Phase 7: Redis dispatch + per-queue matchers

## License

MIT
