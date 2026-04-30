// Package pgpulse provides a high-throughput concurrent job queue backed by PostgreSQL.
//
// Jobs are stored in a PostgreSQL table and processed concurrently using
// FOR UPDATE SKIP LOCKED, which allows multiple workers to fetch jobs
// without conflicts or advisory locks.
package pgpulse

import (
	"encoding/json"
	"time"
)

// JobState represents the lifecycle state of a job.
type JobState string

const (
	JobStateAvailable JobState = "available"
	JobStateRunning   JobState = "running"
	JobStateCompleted JobState = "completed"
	JobStateFailed    JobState = "failed"
	JobStateDiscarded JobState = "discarded"
)

// Job represents a unit of work stored in the database.
type Job struct {
	ID          int64           `json:"id"`
	Queue       string          `json:"queue"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int16           `json:"priority"`
	State       JobState        `json:"state"`
	Attempt     int16           `json:"attempt"`
	MaxRetries  int16           `json:"max_retries"`
	Errors      json.RawMessage `json:"errors,omitempty"`
	ScheduledAt time.Time       `json:"scheduled_at"`
	CreatedAt   time.Time       `json:"created_at"`
	AttemptedAt *time.Time      `json:"attempted_at,omitempty"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
}

// InsertParams holds the parameters for inserting a new job.
type InsertParams struct {
	Queue       string          // Target queue name (default: "default").
	Kind        string          // Job kind / type identifier (required).
	Payload     json.RawMessage // Arbitrary JSON payload.
	Priority    int16           // Higher priority jobs are fetched first.
	MaxRetries  int16           // Maximum number of retry attempts (default: 3).
	ScheduledAt *time.Time      // When the job becomes available (default: now).
}

// JobError records a single attempt error.
type JobError struct {
	Attempt   int16     `json:"attempt"`
	Error     string    `json:"error"`
	OccurredAt time.Time `json:"occurred_at"`
}
