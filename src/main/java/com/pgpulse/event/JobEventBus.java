package com.pgpulse.event;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Manages per-job SSE subscriptions with full event history replay.
 *
 * <p>Every event published for a job is stored in an in-memory log. When a
 * new client subscribes, the entire history is replayed so it sees the
 * complete lifecycle (queued → processing → progress… → completed/failed),
 * regardless of when it connects.
 *
 * <p>Event IDs are sequential <em>per job</em> (1, 2, 3…) so that the
 * SSE {@code Last-Event-ID} header is meaningful for reconnection.
 *
 * <p>A background thread sends keep-alive comments every 15 seconds and
 * prunes history for completed jobs after a configurable TTL.
 */
@Component
public class JobEventBus {

    private static final Logger log = LoggerFactory.getLogger(JobEventBus.class);
    private static final long EMITTER_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(30);
    private static final long KEEPALIVE_INTERVAL_SEC = 15;
    private static final long HISTORY_TTL_MS = TimeUnit.MINUTES.toMillis(5);

    private final ConcurrentMap<Long, CopyOnWriteArrayList<SseEmitter>> emitters = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, EventLog> eventLogs = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;

    private record StoredEvent(long sequence, String eventType, JobStatusEvent event) {}

    /**
     * Per-job event log.  All access is synchronized on the instance so that
     * replay-then-register in {@link #subscribe} and store-then-broadcast in
     * {@link #publish} are atomic — no events are duplicated or lost.
     */
    private static class EventLog {
        final List<StoredEvent> events = new ArrayList<>();
        long nextSequence = 1;
        boolean completed;
        long completedAtMs;
    }

    public JobEventBus() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sse-maintenance");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(
                this::maintenance, KEEPALIVE_INTERVAL_SEC, KEEPALIVE_INTERVAL_SEC, TimeUnit.SECONDS);
    }

    /**
     * Subscribe to a job's event stream.  Events with sequence {@code <= lastEventId}
     * are skipped (SSE reconnection support).  Returns {@code null} when the job
     * is terminal and the client is already caught up — the caller should respond
     * with HTTP 204 to stop EventSource reconnect loops.
     */
    public SseEmitter subscribe(Long jobId, Long lastEventId) {
        SseEmitter emitter = new SseEmitter(EMITTER_TIMEOUT_MS);

        Runnable cleanup = () -> removeEmitter(jobId, emitter);
        emitter.onCompletion(cleanup);
        emitter.onTimeout(cleanup);
        emitter.onError(e -> cleanup.run());

        var eventLog = eventLogs.computeIfAbsent(jobId, k -> new EventLog());

        synchronized (eventLog) {
            long startAfter = (lastEventId != null) ? lastEventId : 0;
            var toReplay = eventLog.events.stream()
                    .filter(e -> e.sequence() > startAfter)
                    .toList();

            // Terminal job with nothing new — signal caller to return 204
            if (eventLog.completed && toReplay.isEmpty()) {
                return null;
            }

            try {
                // Replay filtered history
                for (var stored : toReplay) {
                    emitter.send(SseEmitter.event()
                            .id(String.valueOf(stored.sequence()))
                            .name(stored.eventType())
                            .data(stored.event()));
                }
            } catch (IOException | IllegalStateException e) {
                log.debug("Failed to send to subscriber for job {}", jobId);
                return emitter;
            }

            if (eventLog.completed) {
                // Replayed remaining events for terminal job — close stream
                emitter.complete();
            } else {
                // Register for live events
                emitters.computeIfAbsent(jobId, k -> new CopyOnWriteArrayList<>()).add(emitter);
            }
        }

        log.debug("SSE client subscribed to job {} (lastEventId: {})", jobId, lastEventId);
        return emitter;
    }

    /**
     * Check whether any events have been recorded for a job.
     * Used by the controller to detect pruned history for terminal jobs.
     */
    public boolean hasHistory(Long jobId) {
        var eventLog = eventLogs.get(jobId);
        if (eventLog == null) return false;
        synchronized (eventLog) {
            return !eventLog.events.isEmpty();
        }
    }

    /**
     * Store an event and broadcast it to all live subscribers of the job.
     */
    public void publish(Long jobId, String eventType, JobStatusEvent event) {
        var eventLog = eventLogs.computeIfAbsent(jobId, k -> new EventLog());

        synchronized (eventLog) {
            long seq = eventLog.nextSequence++;
            eventLog.events.add(new StoredEvent(seq, eventType, event));

            var list = emitters.get(jobId);
            if (list == null || list.isEmpty()) return;

            String id = String.valueOf(seq);
            for (var emitter : list) {
                try {
                    emitter.send(SseEmitter.event()
                            .id(id)
                            .name(eventType)
                            .data(event));
                } catch (IOException | IllegalStateException e) {
                    removeEmitter(jobId, emitter);
                }
            }
        }
    }

    /**
     * Mark a job's event log as terminal and complete all live emitters.
     * History is retained so late subscribers can still replay the full lifecycle.
     */
    public void complete(Long jobId) {
        var eventLog = eventLogs.get(jobId);
        if (eventLog != null) {
            synchronized (eventLog) {
                eventLog.completed = true;
                eventLog.completedAtMs = System.currentTimeMillis();
            }
        }

        var list = emitters.remove(jobId);
        if (list != null) {
            for (var emitter : list) {
                try {
                    emitter.complete();
                } catch (Exception e) {
                    // already done
                }
            }
        }
    }

    private void removeEmitter(Long jobId, SseEmitter emitter) {
        var list = emitters.get(jobId);
        if (list != null) {
            list.remove(emitter);
            if (list.isEmpty()) {
                emitters.remove(jobId, list);
            }
        }
    }

    /**
     * Periodic maintenance: send keep-alive comments to all connected clients
     * and prune event history for jobs that completed more than 5 minutes ago.
     */
    private void maintenance() {
        // Keep-alive
        emitters.forEach((jobId, list) -> {
            for (var emitter : list) {
                try {
                    emitter.send(SseEmitter.event().comment("keepalive"));
                } catch (IOException | IllegalStateException e) {
                    removeEmitter(jobId, emitter);
                }
            }
        });

        // Prune stale history
        long now = System.currentTimeMillis();
        eventLogs.entrySet().removeIf(entry -> {
            var eventLog = entry.getValue();
            synchronized (eventLog) {
                return eventLog.completed
                        && (now - eventLog.completedAtMs) > HISTORY_TTL_MS
                        && !emitters.containsKey(entry.getKey());
            }
        });
    }

    @PreDestroy
    void shutdown() {
        scheduler.shutdown();
        emitters.values().forEach(list -> list.forEach(e -> {
            try {
                e.complete();
            } catch (Exception ex) {
                // ignore
            }
        }));
        emitters.clear();
        eventLogs.clear();
    }
}
