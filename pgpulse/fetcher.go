package pgpulse

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Fetch locks and returns a batch of available jobs using FOR UPDATE SKIP LOCKED.
// The returned jobs have their state set to "running" and their attempt count incremented.
// The caller is responsible for completing or failing the jobs after processing.
func (c *Client) Fetch(ctx context.Context, queue string, limit int) ([]*Job, error) {
	if queue == "" {
		queue = "default"
	}
	if limit <= 0 {
		limit = 1
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("pgpulse: begin fetch tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	now := time.Now()

	rows, err := tx.QueryContext(ctx,
		`UPDATE pgpulse_jobs
		 SET state = 'running', attempt = attempt + 1, attempted_at = $1
		 WHERE id IN (
		     SELECT id FROM pgpulse_jobs
		     WHERE queue = $2
		       AND state = 'available'
		       AND scheduled_at <= $1
		     ORDER BY priority DESC, scheduled_at ASC
		     FOR UPDATE SKIP LOCKED
		     LIMIT $3
		 )
		 RETURNING id, queue, kind, payload, priority, state, attempt, max_retries,
		           errors, scheduled_at, created_at, attempted_at, completed_at`,
		now, queue, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("pgpulse: fetch jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		job := &Job{}
		if err := rows.Scan(
			&job.ID, &job.Queue, &job.Kind, &job.Payload, &job.Priority,
			&job.State, &job.Attempt, &job.MaxRetries, &job.Errors,
			&job.ScheduledAt, &job.CreatedAt, &job.AttemptedAt, &job.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("pgpulse: scan job: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgpulse: rows error: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("pgpulse: commit fetch: %w", err)
	}
	return jobs, nil
}

// Complete marks a job as successfully completed.
func (c *Client) Complete(ctx context.Context, jobID int64) error {
	now := time.Now()
	res, err := c.db.ExecContext(ctx,
		`UPDATE pgpulse_jobs SET state = 'completed', completed_at = $1 WHERE id = $2 AND state = 'running'`,
		now, jobID,
	)
	if err != nil {
		return fmt.Errorf("pgpulse: complete job %d: %w", jobID, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// Fail records an error against a job. If the job has remaining retries it is
// re-scheduled with exponential backoff; otherwise it is marked as discarded.
func (c *Client) Fail(ctx context.Context, jobID int64, jobErr error) error {
	now := time.Now()

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("pgpulse: begin fail tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	var job Job
	err = tx.QueryRowContext(ctx,
		`SELECT id, attempt, max_retries, errors FROM pgpulse_jobs WHERE id = $1 AND state = 'running' FOR UPDATE`,
		jobID,
	).Scan(&job.ID, &job.Attempt, &job.MaxRetries, &job.Errors)
	if err != nil {
		return fmt.Errorf("pgpulse: fail job select %d: %w", jobID, err)
	}

	// Append error to the errors array.
	errEntry := fmt.Sprintf(`{"attempt":%d,"error":%q,"occurred_at":%q}`,
		job.Attempt, jobErr.Error(), now.Format(time.RFC3339))

	var errorsJSON string
	if job.Errors == nil || string(job.Errors) == "null" {
		errorsJSON = "[" + errEntry + "]"
	} else {
		raw := string(job.Errors)
		errorsJSON = raw[:len(raw)-1] + "," + errEntry + "]"
	}

	if job.Attempt >= job.MaxRetries {
		// No more retries — discard.
		_, err = tx.ExecContext(ctx,
			`UPDATE pgpulse_jobs SET state = 'discarded', errors = $1, completed_at = $2 WHERE id = $3`,
			errorsJSON, now, jobID,
		)
	} else {
		// Exponential backoff: 2^attempt seconds.
		backoff := time.Duration(1<<job.Attempt) * time.Second
		nextRun := now.Add(backoff)
		_, err = tx.ExecContext(ctx,
			`UPDATE pgpulse_jobs SET state = 'available', errors = $1, scheduled_at = $2 WHERE id = $3`,
			errorsJSON, nextRun, jobID,
		)
	}
	if err != nil {
		return fmt.Errorf("pgpulse: fail job update %d: %w", jobID, err)
	}

	return tx.Commit()
}
