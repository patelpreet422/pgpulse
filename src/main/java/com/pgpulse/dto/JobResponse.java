package com.pgpulse.dto;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

public record JobResponse(
        Long id,
        JsonNode payload,
        String status,
        int retries,
        JsonNode checkpointData,
        Instant createdAt,
        Instant updatedAt
) {}
