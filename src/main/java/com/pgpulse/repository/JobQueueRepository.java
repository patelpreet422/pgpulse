package com.pgpulse.repository;

import com.pgpulse.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository that implements the job-queue operations using raw SQL via
 * {@link JdbcTemplate}.  Every public method executes as a short,
 * auto-committed transaction — no {@code @Transactional} wrapping.
 *
 * <p>All time-based logic uses {@code CURRENT_TIMESTAMP} inside SQL to
 * avoid clock-drift issues between the application and the database.
 */
@Repository
public class JobQueueRepository {

    private static final Logger log = LoggerFactory.getLogger(JobQueueRepository.class);

    private static final int MAX_RETRIES = 3;

    private final JdbcTemplate jdbc;

    public JobQueueRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // ------------------------------------------------------------------ enqueue

    /**
     * Insert a new job with the given JSON payload.
     */
    public void enqueue(String payload) {
        jdbc.update(
                "INSERT INTO batch_jobs (payload) VALUES (?::jsonb)",
                payload
        );
        log.info("Enqueued job with payload: {}", payload);
    }

    // ------------------------------------------------------------------ dequeue

    /**
     * Atomically claim the next available job using {@code FOR UPDATE SKIP LOCKED}.
     *
     * <p>A job is eligible if it is {@code queued}, or if it is {@code processing}
     * but its lease ({@code locked_until}) has expired.  The returned job has its
     * {@code retries} counter already incremented by the SQL statement itself,
     * protecting the system against thread-death scenarios where Java's catch block
     * might never execute.
     *
     * <p>If the retry limit is exceeded the job is moved to {@code failed} and
     * {@link Optional#empty()} is returned.
     */
    public Optional<Job> dequeue() {
        // Step 1 — claim an eligible row with SKIP LOCKED.
        var rows = jdbc.query(
                """
                UPDATE batch_jobs
                SET    status       = 'processing',
                       locked_until = CURRENT_TIMESTAMP + INTERVAL '5 minutes',
                       retries      = retries + 1,
                       updated_at   = CURRENT_TIMESTAMP
                WHERE  id = (
                           SELECT id
                           FROM   batch_jobs
                           WHERE  status = 'queued'
                              OR (status = 'processing' AND locked_until < CURRENT_TIMESTAMP)
                           ORDER  BY created_at ASC
                           FOR UPDATE SKIP LOCKED
                           LIMIT  1
                       )
                RETURNING id, payload, checkpoint_data, retries
                """,
                (rs, rowNum) -> new Job(
                        rs.getLong("id"),
                        rs.getString("payload"),
                        rs.getString("checkpoint_data"),
                        rs.getInt("retries")
                )
        );

        if (rows.isEmpty()) {
            return Optional.empty();
        }

        Job job = rows.get(0);

        // Step 2 — fail the job if it has exceeded the retry limit.
        if (job.retries() > MAX_RETRIES) {
            log.warn("Job {} exceeded max retries ({}), marking as failed", job.id(), MAX_RETRIES);
            markFailed(job.id(), "Exceeded maximum retries (" + MAX_RETRIES + ")");
            return Optional.empty();
        }

        log.debug("Dequeued job {} (attempt {})", job.id(), job.retries());
        return Optional.of(job);
    }

    // ------------------------------------------------------------------ heartbeat

    /**
     * Extend a job's lease by 5 minutes and persist a checkpoint so
     * processing can be resumed if the worker crashes.
     */
    public void heartbeat(Long jobId, String checkpointJson) {
        jdbc.update(
                """
                UPDATE batch_jobs
                SET    locked_until    = CURRENT_TIMESTAMP + INTERVAL '5 minutes',
                       checkpoint_data = ?::jsonb,
                       updated_at      = CURRENT_TIMESTAMP
                WHERE  id = ?
                """,
                checkpointJson, jobId
        );
        log.debug("Heartbeat for job {}: {}", jobId, checkpointJson);
    }

    // --------------------------------------------------------------- completion

    /**
     * Mark a job as successfully completed.
     */
    public void markCompleted(Long jobId) {
        jdbc.update(
                """
                UPDATE batch_jobs
                SET    status       = 'completed',
                       locked_until = NULL,
                       updated_at   = CURRENT_TIMESTAMP
                WHERE  id = ?
                """,
                jobId
        );
        log.info("Job {} completed", jobId);
    }

    /**
     * Mark a job as failed and record the error in {@code checkpoint_data}.
     */
    public void markFailed(Long jobId, String error) {
        jdbc.update(
                """
                UPDATE batch_jobs
                SET    status          = 'failed',
                       locked_until    = NULL,
                       checkpoint_data = jsonb_set(
                                             COALESCE(checkpoint_data, '{}'::jsonb),
                                             '{error}',
                                             to_jsonb(?::text)
                                         ),
                       updated_at      = CURRENT_TIMESTAMP
                WHERE  id = ?
                """,
                error, jobId
        );
        log.warn("Job {} marked as failed: {}", jobId, error);
    }
}
