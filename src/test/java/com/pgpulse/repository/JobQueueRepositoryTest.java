package com.pgpulse.repository;

import com.pgpulse.model.Job;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(
        properties = {
                "spring.profiles.active=test",
                "pgpulse.worker.enabled=false"
        }
)
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class JobQueueRepositoryTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private JobQueueRepository repository;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void cleanTable() {
        jdbc.update("DELETE FROM batch_jobs");
    }

    // ------------------------------------------------------------------ enqueue

    @Test
    void enqueue_insertsJobWithQueuedStatus() {
        repository.enqueue("{\"task\": \"send_email\"}");

        var result = jdbc.queryForMap("SELECT status, payload::text, retries FROM batch_jobs");
        assertEquals("queued", result.get("status"));
        assertEquals("{\"task\": \"send_email\"}", result.get("payload"));
        assertEquals(0, (int) result.get("retries"));
    }

    // ------------------------------------------------------------------ dequeue

    @Test
    void dequeue_returnsEmptyWhenNoJobs() {
        Optional<Job> job = repository.dequeue();
        assertTrue(job.isEmpty());
    }

    @Test
    void dequeue_claimsQueuedJob() {
        repository.enqueue("{\"task\": \"process\"}");

        Optional<Job> job = repository.dequeue();

        assertTrue(job.isPresent());
        assertEquals("{\"task\": \"process\"}", job.get().payload());
        assertEquals(1, job.get().retries());

        // Verify status changed in DB
        String status = jdbc.queryForObject(
                "SELECT status FROM batch_jobs WHERE id = ?", String.class, job.get().id());
        assertEquals("processing", status);
    }

    @Test
    void dequeue_doesNotReturnAlreadyLockedJob() {
        repository.enqueue("{\"task\": \"a\"}");

        // First dequeue claims the job
        Optional<Job> first = repository.dequeue();
        assertTrue(first.isPresent());

        // Second dequeue finds nothing (job is locked and lease hasn't expired)
        Optional<Job> second = repository.dequeue();
        assertTrue(second.isEmpty());
    }

    @Test
    void dequeue_reclaimsJobWithExpiredLease() {
        repository.enqueue("{\"task\": \"retry_me\"}");

        // Dequeue and then manually expire the lease
        Optional<Job> first = repository.dequeue();
        assertTrue(first.isPresent());

        jdbc.update("UPDATE batch_jobs SET locked_until = CURRENT_TIMESTAMP - INTERVAL '1 minute' WHERE id = ?",
                first.get().id());

        // Now dequeue should reclaim it
        Optional<Job> second = repository.dequeue();
        assertTrue(second.isPresent());
        assertEquals(first.get().id(), second.get().id());
        assertEquals(2, second.get().retries()); // retries incremented
    }

    @Test
    void dequeue_failsJobAfterMaxRetries() {
        repository.enqueue("{\"task\": \"will_fail\"}");

        // Manually set retries to MAX_RETRIES (3), so next dequeue increments to 4 > 3
        jdbc.update("UPDATE batch_jobs SET retries = 3");

        Optional<Job> job = repository.dequeue();
        assertTrue(job.isEmpty()); // Should return empty because retries exceeded

        // Job should now be 'failed' in DB
        String status = jdbc.queryForObject("SELECT status FROM batch_jobs", String.class);
        assertEquals("failed", status);
    }

    // ------------------------------------------------------------------ heartbeat

    @Test
    void heartbeat_updatesCheckpointAndExtendsLease() {
        repository.enqueue("{\"task\": \"long_running\"}");
        Job job = repository.dequeue().orElseThrow();

        // Record the locked_until before heartbeat
        var beforeLease = jdbc.queryForObject(
                "SELECT locked_until FROM batch_jobs WHERE id = ?",
                java.sql.Timestamp.class, job.id());

        // Small delay to ensure timestamp difference
        repository.heartbeat(job.id(), "{\"chunk\": 2}");

        var result = jdbc.queryForMap(
                "SELECT checkpoint_data::text, locked_until FROM batch_jobs WHERE id = ?", job.id());

        assertEquals("{\"chunk\": 2}", result.get("checkpoint_data"));
        // locked_until should be updated (>= before)
        var afterLease = (java.sql.Timestamp) result.get("locked_until");
        assertTrue(afterLease.compareTo(beforeLease) >= 0);
    }

    // ------------------------------------------------------------------ markCompleted

    @Test
    void markCompleted_setsStatusToCompleted() {
        repository.enqueue("{\"task\": \"complete_me\"}");
        Job job = repository.dequeue().orElseThrow();

        repository.markCompleted(job.id());

        var result = jdbc.queryForMap("SELECT status, locked_until FROM batch_jobs WHERE id = ?", job.id());
        assertEquals("completed", result.get("status"));
        assertNull(result.get("locked_until"));
    }

    // ------------------------------------------------------------------ markFailed

    @Test
    void markFailed_setsStatusAndRecordsError() {
        repository.enqueue("{\"task\": \"fail_me\"}");
        Job job = repository.dequeue().orElseThrow();

        repository.markFailed(job.id(), "Something went wrong");

        var result = jdbc.queryForMap(
                "SELECT status, locked_until, checkpoint_data::text FROM batch_jobs WHERE id = ?", job.id());
        assertEquals("failed", result.get("status"));
        assertNull(result.get("locked_until"));
        String checkpoint = (String) result.get("checkpoint_data");
        assertTrue(checkpoint.contains("Something went wrong"));
    }

    // -------------------------------------------------------- FIFO ordering

    @Test
    void dequeue_respectsFIFOOrder() {
        repository.enqueue("{\"order\": 1}");
        repository.enqueue("{\"order\": 2}");
        repository.enqueue("{\"order\": 3}");

        Job first = repository.dequeue().orElseThrow();
        Job second = repository.dequeue().orElseThrow();
        Job third = repository.dequeue().orElseThrow();

        assertTrue(first.id() < second.id());
        assertTrue(second.id() < third.id());
        assertEquals("{\"order\": 1}", first.payload());
        assertEquals("{\"order\": 2}", second.payload());
        assertEquals("{\"order\": 3}", third.payload());
    }

    // ------------------------------------------------- concurrent dequeue

    @Test
    void dequeue_noDuplicatesUnderConcurrency() throws Exception {
        int jobCount = 20;
        for (int i = 0; i < jobCount; i++) {
            repository.enqueue("{\"job\": " + i + "}");
        }

        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);
        List<Job> claimed = Collections.synchronizedList(new ArrayList<>());
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            futures.add(executor.submit(() -> {
                try {
                    latch.await(); // synchronize start
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                while (true) {
                    Optional<Job> job = repository.dequeue();
                    if (job.isEmpty()) break;
                    claimed.add(job.get());
                }
            }));
        }

        latch.countDown(); // release all threads
        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();

        // Every job should be claimed exactly once
        assertEquals(jobCount, claimed.size());
        long distinctIds = claimed.stream().map(Job::id).distinct().count();
        assertEquals(jobCount, distinctIds, "Each job must be claimed by exactly one thread");
    }

    // ------------------------------------------------- full lifecycle

    @Test
    void fullLifecycle_enqueueProcessComplete() {
        repository.enqueue("{\"action\": \"lifecycle_test\"}");

        Job job = repository.dequeue().orElseThrow();
        assertEquals(1, job.retries());

        repository.heartbeat(job.id(), "{\"progress\": 50}");
        repository.heartbeat(job.id(), "{\"progress\": 100}");
        repository.markCompleted(job.id());

        var result = jdbc.queryForMap("SELECT status, checkpoint_data::text FROM batch_jobs WHERE id = ?", job.id());
        assertEquals("completed", result.get("status"));
        assertEquals("{\"progress\": 100}", result.get("checkpoint_data"));
    }
}
