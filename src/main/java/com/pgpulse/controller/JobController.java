package com.pgpulse.controller;

import com.pgpulse.dto.JobRequest;
import com.pgpulse.dto.JobResponse;
import com.pgpulse.dto.PageResponse;
import com.pgpulse.event.JobEventBus;
import com.pgpulse.repository.JobQueueRepository;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.net.URI;
import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobQueueRepository repository;
    private final JobEventBus eventBus;

    public JobController(JobQueueRepository repository, JobEventBus eventBus) {
        this.repository = repository;
        this.eventBus = eventBus;
    }

    @PostMapping
    public ResponseEntity<JobResponse> submit(@RequestBody JobRequest request) {
        if (request.payload() == null || request.payload().isNull()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "payload is required");
        }
        JobResponse job = repository.enqueueReturning(request.payload().toString());
        return ResponseEntity
                .created(URI.create("/api/jobs/" + job.id()))
                .body(job);
    }

    @GetMapping
    public PageResponse<JobResponse> list(
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        size = Math.min(Math.max(size, 1), 100);
        page = Math.max(page, 0);

        int offset = page * size;
        var content = repository.findAll(status, offset, size);
        long total = repository.count(status);
        return PageResponse.of(content, page, size, total);
    }

    @GetMapping("/stats")
    public Map<String, Long> stats() {
        return repository.countByStatus();
    }

    @GetMapping("/{id}")
    public JobResponse get(@PathVariable Long id) {
        return repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Job not found: " + id));
    }

    @GetMapping(path = "/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter events(
            @PathVariable Long id,
            @RequestHeader(name = "Last-Event-ID", required = false) String lastEventId,
            HttpServletResponse response) {

        var job = repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Job not found: " + id));

        // Terminal job with pruned history — return 204 to stop EventSource reconnects
        if (isTerminal(job.status()) && !eventBus.hasHistory(id)) {
            response.setStatus(HttpServletResponse.SC_NO_CONTENT);
            return null;
        }

        Long parsedLastEventId = parseLastEventId(lastEventId);
        SseEmitter emitter = eventBus.subscribe(id, parsedLastEventId);

        // Terminal job, client already caught up — return 204
        if (emitter == null) {
            response.setStatus(HttpServletResponse.SC_NO_CONTENT);
            return null;
        }

        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("X-Accel-Buffering", "no");
        return emitter;
    }

    private static boolean isTerminal(String status) {
        return "completed".equals(status) || "failed".equals(status);
    }

    private static Long parseLastEventId(String lastEventId) {
        if (lastEventId == null || lastEventId.isBlank()) return null;
        try {
            long val = Long.parseLong(lastEventId.trim());
            return val > 0 ? val : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
