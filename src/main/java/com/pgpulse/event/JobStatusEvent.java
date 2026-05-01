package com.pgpulse.event;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * Payload sent over SSE whenever a job's state changes.
 *
 * @param jobId          the job this event refers to
 * @param status         current status (queued, processing, completed, failed)
 * @param checkpointData latest checkpoint/progress data, or null
 * @param timestamp      when the event occurred
 */
public record JobStatusEvent(
        Long jobId,
        String status,
        JsonNode checkpointData,
        Instant timestamp
) {}
