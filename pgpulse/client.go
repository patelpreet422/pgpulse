package pgpulse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// Client is the main entry point for enqueuing and managing jobs.
type Client struct {
	db *sql.DB
}

// NewClient creates a new PgPulse client from an existing *sql.DB connection.
func NewClient(db *sql.DB) *Client {
	return &Client{db: db}
}

// DB returns the underlying database connection.
func (c *Client) DB() *sql.DB {
	return c.db
}

// Insert enqueues a single job and returns the created Job.
func (c *Client) Insert(ctx context.Context, params InsertParams) (*Job, error) {
	if params.Kind == "" {
		return nil, fmt.Errorf("pgpulse: job kind is required")
	}
	if params.Queue == "" {
		params.Queue = "default"
	}
	if params.MaxRetries == 0 {
		params.MaxRetries = 3
	}
	if params.Payload == nil {
		params.Payload = json.RawMessage("{}")
	}

	scheduledAt := time.Now()
	if params.ScheduledAt != nil {
		scheduledAt = *params.ScheduledAt
	}

	job := &Job{}
	err := c.db.QueryRowContext(ctx,
		`INSERT INTO pgpulse_jobs (queue, kind, payload, priority, max_retries, scheduled_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id, queue, kind, payload, priority, state, attempt, max_retries,
		           errors, scheduled_at, created_at, attempted_at, completed_at`,
		params.Queue, params.Kind, params.Payload, params.Priority, params.MaxRetries, scheduledAt,
	).Scan(
		&job.ID, &job.Queue, &job.Kind, &job.Payload, &job.Priority,
		&job.State, &job.Attempt, &job.MaxRetries, &job.Errors,
		&job.ScheduledAt, &job.CreatedAt, &job.AttemptedAt, &job.CompletedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("pgpulse: insert job: %w", err)
	}
	return job, nil
}

// InsertBatch enqueues multiple jobs in a single transaction.
func (c *Client) InsertBatch(ctx context.Context, params []InsertParams) ([]*Job, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("pgpulse: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO pgpulse_jobs (queue, kind, payload, priority, max_retries, scheduled_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id, queue, kind, payload, priority, state, attempt, max_retries,
		           errors, scheduled_at, created_at, attempted_at, completed_at`)
	if err != nil {
		return nil, fmt.Errorf("pgpulse: prepare stmt: %w", err)
	}
	defer stmt.Close()

	jobs := make([]*Job, 0, len(params))
	for _, p := range params {
		if p.Kind == "" {
			return nil, fmt.Errorf("pgpulse: job kind is required")
		}
		if p.Queue == "" {
			p.Queue = "default"
		}
		if p.MaxRetries == 0 {
			p.MaxRetries = 3
		}
		if p.Payload == nil {
			p.Payload = json.RawMessage("{}")
		}
		scheduledAt := time.Now()
		if p.ScheduledAt != nil {
			scheduledAt = *p.ScheduledAt
		}

		job := &Job{}
		err := stmt.QueryRowContext(ctx,
			p.Queue, p.Kind, p.Payload, p.Priority, p.MaxRetries, scheduledAt,
		).Scan(
			&job.ID, &job.Queue, &job.Kind, &job.Payload, &job.Priority,
			&job.State, &job.Attempt, &job.MaxRetries, &job.Errors,
			&job.ScheduledAt, &job.CreatedAt, &job.AttemptedAt, &job.CompletedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("pgpulse: insert batch job: %w", err)
		}
		jobs = append(jobs, job)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("pgpulse: commit batch: %w", err)
	}
	return jobs, nil
}
