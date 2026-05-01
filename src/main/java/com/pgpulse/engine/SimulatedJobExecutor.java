package com.pgpulse.engine;

import com.pgpulse.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Simulated job executor that processes work in chunks with periodic heartbeats.
 * Each chunk takes 3–6 seconds of random "work" to mimic a slow, realistic process.
 * Replace this with real execution logic (e.g., sending emails, resizing images).
 */
@Component
public class SimulatedJobExecutor implements JobExecutor {

    private static final Logger log = LoggerFactory.getLogger(SimulatedJobExecutor.class);

    private static final int CHUNKS = 5;
    private static final long MIN_CHUNK_MS = 3_000;
    private static final long MAX_CHUNK_MS = 6_000;

    @Override
    public void execute(Job job, HeartbeatCallback heartbeat) throws InterruptedException {
        for (int chunk = 1; chunk <= CHUNKS; chunk++) {
            long sleepMs = java.util.concurrent.ThreadLocalRandom.current()
                    .nextLong(MIN_CHUNK_MS, MAX_CHUNK_MS + 1);
            log.info("Job {} — chunk {}/{} will take {}ms", job.id(), chunk, CHUNKS, sleepMs);

            Thread.sleep(sleepMs);

            String checkpoint = String.format("{\"chunk\": %d, \"elapsedMs\": %d}", chunk, sleepMs);
            heartbeat.send(checkpoint);

            log.info("Job {} — executed chunk {}/{}", job.id(), chunk, CHUNKS);
        }
    }
}
