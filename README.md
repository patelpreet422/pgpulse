# PgPulse

**High-Throughput Concurrent Job Queue for PostgreSQL**

PgPulse is a Go library that turns PostgreSQL into a reliable, high-performance job queue. It uses `FOR UPDATE SKIP LOCKED` for safe concurrent job fetching — no advisory locks, no contention.

## Features

- **Concurrent processing** — multiple workers fetch jobs without conflicts using `SKIP LOCKED`
- **Priority queues** — higher priority jobs are processed first
- **Batch enqueuing** — insert many jobs in a single transaction
- **Automatic retries** — configurable retry count with exponential backoff
- **Scheduled jobs** — enqueue jobs to run at a future time
- **Multiple queues** — route different job types to different queues
- **Embedded migrations** — schema setup with a single function call

## Installation

```bash
go get github.com/patelpreet422/pgpulse/pgpulse
```

## Quick Start

```go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/patelpreet422/pgpulse/pgpulse"
)

func main() {
	db, err := sql.Open("postgres", "postgres://localhost:5432/mydb?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	// Run migrations (safe to call multiple times).
	if err := pgpulse.Migrate(ctx, db); err != nil {
		log.Fatal(err)
	}

	client := pgpulse.NewClient(db)

	// Enqueue a job.
	job, err := client.Insert(ctx, pgpulse.InsertParams{
		Kind:    "send_email",
		Payload: json.RawMessage(`{"to":"user@example.com","subject":"Hello"}`),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Enqueued job %d\n", job.ID)

	// Start a worker pool.
	worker := pgpulse.NewWorker(client, pgpulse.WorkerConfig{
		Concurrency: 5,
	}, func(ctx context.Context, job *pgpulse.Job) error {
		fmt.Printf("Processing job %d (kind=%s)\n", job.ID, job.Kind)
		// Do work here...
		return nil
	})

	worker.Run(ctx) // blocks until ctx is cancelled
}
```

## Batch Enqueuing

```go
jobs, err := client.InsertBatch(ctx, []pgpulse.InsertParams{
	{Kind: "send_email", Payload: json.RawMessage(`{"to":"a@example.com"}`)},
	{Kind: "send_email", Payload: json.RawMessage(`{"to":"b@example.com"}`)},
	{Kind: "generate_report", Priority: 10},
})
```

## Schema

PgPulse stores jobs in a single `pgpulse_jobs` table. Run the embedded migration or apply `pgpulse/schema.sql` manually:

```bash
psql -f pgpulse/schema.sql mydb
```

## Configuration

| Option | Default | Description |
|---|---|---|
| `Queue` | `"default"` | Queue name to poll |
| `Concurrency` | `5` | Number of worker goroutines |
| `FetchSize` | `= Concurrency` | Jobs fetched per poll cycle |
| `PollInterval` | `1s` | Delay between polls when idle |

## How It Works

1. **Enqueue**: Jobs are inserted into `pgpulse_jobs` with state `available`.
2. **Fetch**: Workers use `SELECT ... FOR UPDATE SKIP LOCKED` to atomically claim a batch of jobs, setting their state to `running`.
3. **Process**: The handler function processes each job.
4. **Complete/Fail**: On success, the job is marked `completed`. On failure, PgPulse increments the attempt counter and either re-schedules with exponential backoff or marks the job `discarded` if max retries are exhausted.

## License

MIT
