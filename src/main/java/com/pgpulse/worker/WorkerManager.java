package com.pgpulse.worker;

import com.pgpulse.model.Job;
import com.pgpulse.repository.JobQueueRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages a pool of worker threads that continuously poll the job queue.
 *
 * <p><b>Important:</b> This class intentionally does <em>not</em> use
 * {@code @Transactional}.  Each repository call is a short, auto-committed
 * transaction so that database connections are returned to the pool quickly
 * and locks are never held across the entire processing loop.
 */
@Service
@ConditionalOnProperty(name = "pgpulse.worker.enabled", havingValue = "true", matchIfMissing = true)
public class WorkerManager {

    private static final Logger log = LoggerFactory.getLogger(WorkerManager.class);

    private static final int THREAD_COUNT = 10;
    private static final int CHUNKS = 5;
    private static final long CHUNK_SLEEP_MS = 500;
    private static final long IDLE_SLEEP_MS = 1000;

    private final JobQueueRepository repository;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private ExecutorService executor;

    public WorkerManager(JobQueueRepository repository) {
        this.repository = repository;
    }

    @PostConstruct
    public void start() {
        executor = Executors.newFixedThreadPool(THREAD_COUNT);
        for (int i = 0; i < THREAD_COUNT; i++) {
            final int workerId = i;
            executor.submit(() -> workerLoop(workerId));
        }
        log.info("PgPulse WorkerManager started with {} threads", THREAD_COUNT);
    }

    @PreDestroy
    public void stop() {
        log.info("Shutting down WorkerManager...");
        isRunning.set(false);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Workers did not terminate in time — forcing shutdown");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("WorkerManager shut down");
    }

    // ----------------------------------------------------------------- internal

    private void workerLoop(int workerId) {
        log.info("Worker-{} started", workerId);
        while (isRunning.get()) {
            try {
                var maybeJob = repository.dequeue();

                if (maybeJob.isEmpty()) {
                    Thread.sleep(IDLE_SLEEP_MS);
                    continue;
                }

                Job job = maybeJob.get();
                log.info("Worker-{} processing job {} (attempt {})",
                        workerId, job.id(), job.retries());

                processJob(job);

                repository.markCompleted(job.id());
                log.info("Worker-{} completed job {}", workerId, job.id());

            } catch (InterruptedException e) {
                // Restore interrupt flag and exit loop cleanly.
                Thread.currentThread().interrupt();
                log.info("Worker-{} interrupted, exiting", workerId);
                break;
            } catch (Exception e) {
                // Simulate a crash: do NOT update the DB.
                // The lease will expire and another worker (or this one)
                // will re-claim the job, with retries already incremented.
                log.error("Worker-{} encountered an error — letting lease expire", workerId, e);
            }
        }
        log.info("Worker-{} stopped", workerId);
    }

    /**
     * Simulate chunked processing with periodic heartbeats.
     */
    private void processJob(Job job) throws InterruptedException {
        for (int chunk = 1; chunk <= CHUNKS; chunk++) {
            // Simulate work
            Thread.sleep(CHUNK_SLEEP_MS);

            // Send heartbeat with checkpoint data
            String checkpoint = String.format("{\"chunk\": %d}", chunk);
            repository.heartbeat(job.id(), checkpoint);

            log.debug("Job {} — heartbeat chunk {}/{}", job.id(), chunk, CHUNKS);
        }
    }
}
