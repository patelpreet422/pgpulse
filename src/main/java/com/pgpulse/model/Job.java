package com.pgpulse.model;

/**
 * Immutable representation of a job fetched from the queue.
 *
 * @param id             database-generated identifier
 * @param payload        JSONB payload describing the work
 * @param checkpointData JSONB checkpoint state for resumable processing
 * @param retries        number of times this job has been attempted
 */
public record Job(
        Long id,
        String payload,
        String checkpointData,
        int retries
) {}
