# Velum — Durable Workflow Orchestration

Event-sourced workflow engine in Go. Distributed task queues, gRPC workers, durable timers, parallel branches, saga compensation, and idempotent completions.

I built this to understand how systems like Temporal and Azure Durable Functions work under the hood. The goal wasn't to clone them — it was to make the sharpest possible version of the core ideas: event-sourced state machines, lease-based task dispatch, and compensation-based sagas — without the operational complexity of a full Temporal deployment.

**Module:** `github.com/0xrameshh/velum`

## Quick start

```bash
# If port 8080 is free:
docker compose up --build -d
make smoke

# If port 8080 is taken:
VELUM_HOST_HTTP_PORT=8088 docker compose up --build -d
VELUM_HOST_HTTP_PORT=8088 make smoke
```

## Architecture

```
┌─────────┐   ┌──────────────┐   ┌───────────┐
│ Client   │ → │ velum-api    │ → │ velum-    │
│ (curl)   │   │ (HTTP Chi)   │   │ history   │
└─────────┘   └──────────────┘   │ (gRPC +   │
                                 │  Postgres)│
┌──────────┐   ┌────────────┐   └─────┬─────┘
│ workers  │ ← │ matchers   │    │    │
│ (gRPC)   │   │ (per queue) │    │    │
└──────────┘   └────────────┘    │    │
                              ┌──┴──┐ │
                              │ Redis│←┘
                              │ wake │
                              └─────┘
```

Key design decisions:

- **Postgres is the source of truth.** Tasks, runs, events, timers, state — all in Postgres. Redis is only a wake signal (BRPOP) so matchers don't blind-poll.
- **Each queue has its own matcher.** `default`, `email`, `payments` each run as a separate gRPC server. Workers connect to their queue's matcher. This isolates failure domains and makes per-queue scaling independent.
- **Idempotency keys on every completion/failure.** Workers generate deterministic keys (`taskID:attempt:suffix`). The matcher deduplicates on insert — safe for at-least-once delivery.

## Workflows

| Name | Steps |
|------|-------|
| `greet` | `greet` (default) → `send_email` (email) |
| `delayed_greet` | `greet` → durable timer → `send_email` |
| `order_saga` | parallel `charge_card` + `reserve_stock` → `ship_order` → (on ship fail) refund + release |

### Saga demo

```bash
# Happy path
make curl-saga-start && sleep 2 && make curl-saga-status

# Compensation path (ship fails, refund + release run)
make curl-saga-fail && sleep 3 && make curl-saga-status
```

```json
// Input for delayed_greet
{"name": "Ramesh", "sleep_seconds": 5}

// Input for order_saga
{"order_id": "ord-42", "fail_ship": false}
```

### What I learned

1. **State consistency is the hard part.** Parallel branches completing within <1ms would race on state updates — the second `OnActivityCompleted` loaded stale state and overwrote the first. Fix: `SELECT ... FOR UPDATE` inside a transaction to serialize saga state mutations.

2. **gRPC streaming would be better than polling.** Workers poll their matcher every 300ms. It works but it's wasteful at scale. A streaming `PollTask` RPC would be cleaner — the matcher holds the connection open and pushes tasks as they arrive.

3. **The event-sourced model pays off for debugging.** Every state transition is an immutable event in Postgres. When something goes wrong (like the race condition above), you just read the event log and replay the decisions. No mystery state.

4. **Saga compensation is subtle.** The refund must happen after charge succeeds, and the release after reserve succeeds. The compensation order matters — we schedule refund first (payments queue), then release (default queue). Each compensation step is an activity with its own retry policy.

## API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness |
| `GET` | `/ready` | Readiness |
| `POST` | `/api/v1/namespaces/{ns}/workflows/{name}/start` | Start a workflow |
| `GET` | `/api/v1/namespaces/{ns}/runs/{run_id}` | Run + event history |

## Performance

Run on a MacBook Air (M3) against Docker Postgres on localhost:

| Operation | Throughput |
|-----------|------------|
| Create → Poll → Complete | ~657 tasks/sec |
| Atomic state update (FOR UPDATE) | ~2,614 updates/sec |

```bash
go test -tags=integration -bench=. ./internal/persistence/
```

These are single-node, single-worker numbers. The system scales horizontally by adding matchers and workers per queue.

## Testing

```bash
# Unit tests (no dependencies)
go test ./...

# Integration tests (requires Postgres — set VELUM_TEST_DATABASE_URL)
go test -tags=integration ./internal/persistence/
go test -tags=integration ./internal/history/
go test -tags=integration ./internal/...

# Full smoke test (requires full Docker Compose stack)
make smoke
```

Integration tests exercise the full pipeline against real Postgres: task lifecycle, idempotency, lease reclaim, retry with backoff, parallel saga branches with atomic state updates, timer firing, and saga compensation.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `VELUM_HTTP_ADDR` | `:8080` | HTTP listen |
| `VELUM_GRPC_ADDR` | - | Worker gRPC listen / dial |
| `VELUM_HISTORY_GRPC_ADDR` | - | History gRPC listen / dial |
| `VELUM_DATABASE_URL` | - | PostgreSQL DSN |
| `VELUM_DISPATCH` | `postgres` | Wake dispatch mode (`postgres` / `redis`) |
| `VELUM_REDIS_ADDR` | - | Redis address |
| `VELUM_MATCHER_QUEUES` | (all) | Comma-separated queues for this matcher |
| `VELUM_TASK_QUEUE` | - | Queue for worker to poll |
| `VELUM_SCHEDULER_POLL_EVERY` | `1s` | Timer poll interval |
| `VELUM_LEASE_RECLAIM_EVERY` | `5s` | Lease reclaim interval |
| `VELUM_DISPATCH_WAIT` | - | Redis BRPOP wait duration |
| `VELUM_HOST_HTTP_PORT` | `8080` | Host port mapping for Compose |

## Binaries

| Binary | Role |
|--------|------|
| `velum-history` | Workflow state machine + event log |
| `velum-api` | HTTP API (stateless, proxies to history) |
| `velum-matcher` | gRPC task poll/complete (one per queue) |
| `velum-scheduler` | Fire due durable timers |
| `velum-worker` | Execute activities |
| `velum-migrate` | One-shot DB migrations |
| `velum` | All-in-one local dev mode |

## What I'd do differently

- **Streaming RPCs instead of polling.** Workers poll the matcher on a ticker. A server-streaming `PollTask` would let the matcher push tasks as they arrive, cutting latency and DB load.
- **OTel traces.** The saga compensation flow spans multiple services and queues — distributed traces would make debugging production issues much faster.
- **Rate limiting per queue.** Right now any worker can hammer a queue. A token-bucket limiter on the matcher would prevent thundering herds during retry storms.
- **Dedicated history partition.** A single history service is a bottleneck. Sharding by namespace or workflow type would let it scale.

## Scaling

- **Per-queue matchers** in Compose (`velum-matcher-default`, `velum-matcher-email`, `velum-matcher-payments`)
- **Workers** connect to their queue's matcher via `VELUM_GRPC_ADDR`
- **Redis** is an optional wake signal only — no state lives there
- **Postgres** remains the source of truth; add read replicas for history queries

## License

MIT
