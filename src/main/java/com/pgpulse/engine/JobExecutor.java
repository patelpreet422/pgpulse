package com.pgpulse.engine;

import com.pgpulse.model.Job;

/**
 * Strategy interface for executing jobs dequeued from the batch_jobs table.
 * Implementations define the actual work to be done for each job.
 *
 * <p>Analogous to a Spark executor: the driver ({@link JobDriver}) assigns
 * work, and the executor carries it out.
 */
public interface JobExecutor {

    /**
     * Execute a single job.
     *
     * @param job       the job to execute
     * @param heartbeat callback to extend the job's lease and persist checkpoint data
     * @throws Exception if execution fails
     */
    void execute(Job job, HeartbeatCallback heartbeat) throws Exception;

    /**
     * Callback for sending heartbeats during long-running job execution.
     * Extends the job's lease and persists checkpoint data so execution
     * can be resumed if the executor crashes.
     */
    @FunctionalInterface
    interface HeartbeatCallback {
        void send(String checkpointJson);
    }
}
