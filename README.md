# PgPulse

**High-Throughput Concurrent Job Queue for PostgreSQL**

PgPulse is a Java/Spring Boot application that turns PostgreSQL into a reliable, high-performance job queue. It uses `FOR UPDATE SKIP LOCKED` for safe concurrent job fetching — no advisory locks, no contention, no deadlocks.

## Features

- **Concurrent processing** — 10 worker threads fetch jobs without conflicts using `SKIP LOCKED`
- **Lease-based locking** — `locked_until` timestamps prevent stale locks; expired leases are automatically reclaimed
- **Heartbeat & checkpointing** — workers extend their lease and persist progress so jobs can resume after crashes
- **Automatic retries** — jobs are retried up to 3 times; retries are incremented atomically in SQL to survive thread death
- **Partial index** — `idx_batch_jobs_poll` only covers `queued`/`processing` rows, preventing index bloat from completed jobs
- **Clock-drift safe** — all time logic uses PostgreSQL's `CURRENT_TIMESTAMP`, never Java's clock
- **No long transactions** — each repository call is auto-committed; the worker loop is never wrapped in `@Transactional`
- **Graceful shutdown** — `@PreDestroy` waits up to 10 seconds for in-flight jobs before forcing termination

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker & Docker Compose

## Quick Start

```bash
# 1. Start PostgreSQL
docker-compose up -d

# 2. Build & run
mvn spring-boot:run
```

The schema is created automatically on startup via `spring.sql.init.mode=always`.

## Project Structure

```
├── docker-compose.yml                          # PostgreSQL 16
├── pom.xml                                     # Spring Boot 3.x, JDBC, PostgreSQL
└── src/main/
    ├── java/com/pgpulse/
    │   ├── PgPulseApplication.java             # Spring Boot entry point
    │   ├── model/Job.java                      # Immutable Job record
    │   ├── repository/JobQueueRepository.java  # JdbcTemplate-based queue operations
    │   └── worker/WorkerManager.java           # Thread pool that polls & processes jobs
    └── resources/
        ├── application.properties              # DataSource & HikariCP config
        └── schema.sql                          # DDL with partial index
```

## How It Works

1. **Enqueue** — insert a row into `batch_jobs` with status `queued` and a JSONB payload.
2. **Dequeue** — a worker runs `SELECT ... FOR UPDATE SKIP LOCKED` to atomically claim one job, setting status to `processing`, extending `locked_until` by 5 minutes, and incrementing `retries` in the same SQL statement.
3. **Process** — the worker processes the job in chunks, calling `heartbeat()` after each chunk to extend the lease and persist a checkpoint.
4. **Complete / Fail** — on success the job is marked `completed`. On failure the worker lets the lease expire naturally so another worker can reclaim it. If retries exceed 3, the job is marked `failed`.

## Configuration

| Property | Default | Description |
|---|---|---|
| `spring.datasource.hikari.maximum-pool-size` | `20` | Max DB connections |
| `spring.datasource.hikari.minimum-idle` | `5` | Min idle connections |
| Worker threads | `10` | Concurrent worker threads |
| Lease duration | `5 min` | `locked_until` extension per heartbeat |
| Max retries | `3` | Attempts before marking failed |
| Idle poll delay | `1 s` | Sleep when queue is empty |

## License

MIT
