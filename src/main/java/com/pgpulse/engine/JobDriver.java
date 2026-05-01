package com.pgpulse.engine;

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
 * The driver coordinates job execution: it manages a thread pool, polls the
 * queue for work, and delegates each job to a {@link JobExecutor}.
 *
 * <p>Analogous to a Spark driver: it does not execute work itself but assigns
 * tasks to executors and tracks their lifecycle.
 *
 * <p><b>Important:</b> This class intentionally does <em>not</em> use
 * {@code @Transactional}.  Each repository call is a short, auto-committed
 * transaction so that database connections are returned to the pool quickly
 * and locks are never held across the entire processing loop.
 */
@Service
@ConditionalOnProperty(name = "pgpulse.driver.enabled", havingValue = "true", matchIfMissing = true)
public class JobDriver {

    private static final Logger log = LoggerFactory.getLogger(JobDriver.class);

    private static final int EXECUTOR_COUNT = 10;
    private static final long IDLE_SLEEP_MS = 1000;

    private final JobQueueRepository repository;
    private final JobExecutor executor;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private ExecutorService threadPool;

    public JobDriver(JobQueueRepository repository, JobExecutor executor) {
        this.repository = repository;
        this.executor = executor;
    }

    // ---------------------------------------------------------------- lifecycle

    @PostConstruct
    public void start() {
        threadPool = Executors.newFixedThreadPool(EXECUTOR_COUNT);
        for (int i = 0; i < EXECUTOR_COUNT; i++) {
            final int executorId = i;
            threadPool.submit(() -> executorLoop(executorId));
        }
        log.info("JobDriver started with {} executors", EXECUTOR_COUNT);
    }

    @PreDestroy
    public void stop() {
        log.info("Shutting down JobDriver...");
        isRunning.set(false);
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Executors did not terminate in time — forcing shutdown");
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("JobDriver shut down");
    }

    // -------------------------------------------------------------- poll loop

    private void executorLoop(int executorId) {
        log.info("Executor-{} started", executorId);
        while (isRunning.get()) {
            try {
                var maybeJob = repository.dequeue();

                if (maybeJob.isEmpty()) {
                    Thread.sleep(IDLE_SLEEP_MS);
                    continue;
                }

                Job job = maybeJob.get();
                log.info("Executor-{} processing job {} (attempt {})",
                        executorId, job.id(), job.retries());

                executor.execute(job, checkpoint -> repository.heartbeat(job.id(), checkpoint));

                repository.markCompleted(job.id());
                log.info("Executor-{} completed job {}", executorId, job.id());

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Executor-{} interrupted, exiting", executorId);
                break;
            } catch (Exception e) {
                // Let the lease expire so another executor re-claims the job.
                log.error("Executor-{} encountered an error — letting lease expire", executorId, e);
            }
        }
        log.info("Executor-{} stopped", executorId);
    }
}
