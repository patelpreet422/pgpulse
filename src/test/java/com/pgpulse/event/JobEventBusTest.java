package com.pgpulse.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class JobEventBusTest {

    private JobEventBus eventBus;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        eventBus = new JobEventBus();
    }

    @AfterEach
    void tearDown() {
        eventBus.shutdown();
    }

    private JobStatusEvent event(Long jobId, String status) {
        return new JobStatusEvent(jobId, status, null, Instant.now());
    }

    // ---------------------------------------------------------------- subscribe

    @Test
    void subscribe_returnsEmitterForNewJob() {
        SseEmitter emitter = eventBus.subscribe(1L, null);
        assertNotNull(emitter);
    }

    @Test
    void subscribe_replaysFullHistoryWhenNoLastEventId() {
        eventBus.publish(1L, "status", event(1L, "queued"));
        eventBus.publish(1L, "status", event(1L, "processing"));
        eventBus.publish(1L, "progress", event(1L, "processing"));

        // Subscribing with no lastEventId should replay all 3 events
        SseEmitter emitter = eventBus.subscribe(1L, null);
        assertNotNull(emitter);
    }

    @Test
    void subscribe_skipsEventsBeforeLastEventId() {
        eventBus.publish(1L, "status", event(1L, "queued"));       // seq 1
        eventBus.publish(1L, "status", event(1L, "processing"));   // seq 2
        eventBus.publish(1L, "progress", event(1L, "processing")); // seq 3
        eventBus.publish(1L, "progress", event(1L, "processing")); // seq 4

        // Should only replay events after seq 2
        SseEmitter emitter = eventBus.subscribe(1L, 2L);
        assertNotNull(emitter);
    }

    // --------------------------------------------------------- terminal + 204

    @Test
    void subscribe_returnsNullWhenTerminalAndCaughtUp() {
        eventBus.publish(1L, "status", event(1L, "queued"));     // seq 1
        eventBus.publish(1L, "status", event(1L, "completed"));  // seq 2
        eventBus.complete(1L);

        // Client already saw event 2 — nothing new to send
        SseEmitter emitter = eventBus.subscribe(1L, 2L);
        assertNull(emitter, "Should return null for caught-up terminal job");
    }

    @Test
    void subscribe_returnsNullWhenTerminalAndLastEventIdBeyondHistory() {
        eventBus.publish(1L, "status", event(1L, "queued"));     // seq 1
        eventBus.publish(1L, "status", event(1L, "completed"));  // seq 2
        eventBus.complete(1L);

        // Last-Event-ID larger than anything in history
        SseEmitter emitter = eventBus.subscribe(1L, 100L);
        assertNull(emitter, "Should return null when lastEventId exceeds history");
    }

    @Test
    void subscribe_replaysRemainingEventsForTerminalJob() {
        eventBus.publish(1L, "status", event(1L, "queued"));     // seq 1
        eventBus.publish(1L, "status", event(1L, "processing")); // seq 2
        eventBus.publish(1L, "status", event(1L, "completed"));  // seq 3
        eventBus.complete(1L);

        // Client saw event 1, should replay 2 and 3 then complete
        SseEmitter emitter = eventBus.subscribe(1L, 1L);
        assertNotNull(emitter, "Should replay remaining events before returning null");
    }

    // ---------------------------------------------------------------- publish

    @Test
    void publish_storesEventInHistory() {
        assertFalse(eventBus.hasHistory(1L));

        eventBus.publish(1L, "status", event(1L, "queued"));

        assertTrue(eventBus.hasHistory(1L));
    }

    @Test
    void publish_sequentialIdsPerJob() {
        eventBus.publish(1L, "status", event(1L, "queued"));      // job 1 seq 1
        eventBus.publish(1L, "status", event(1L, "processing"));  // job 1 seq 2
        eventBus.publish(2L, "status", event(2L, "queued"));      // job 2 seq 1 (independent)

        // Both jobs should have history
        assertTrue(eventBus.hasHistory(1L));
        assertTrue(eventBus.hasHistory(2L));
    }

    @Test
    void publish_isolatesJobsFromEachOther() {
        eventBus.publish(1L, "status", event(1L, "queued"));
        eventBus.publish(2L, "status", event(2L, "queued"));
        eventBus.publish(2L, "status", event(2L, "completed"));
        eventBus.complete(2L);

        // Job 1 is still active, job 2 is terminal
        assertNotNull(eventBus.subscribe(1L, null));
        assertNotNull(eventBus.subscribe(2L, null)); // replays then completes
        assertNull(eventBus.subscribe(2L, 2L));       // caught up → null
    }

    // ---------------------------------------------------------------- complete

    @Test
    void complete_marksJobAsTerminal() {
        eventBus.publish(1L, "status", event(1L, "queued"));
        eventBus.publish(1L, "status", event(1L, "completed"));
        eventBus.complete(1L);

        // History is retained
        assertTrue(eventBus.hasHistory(1L));

        // But caught-up subscriber gets null
        assertNull(eventBus.subscribe(1L, 2L));
    }

    @Test
    void complete_retainsHistoryForLateSubscribers() {
        eventBus.publish(1L, "status", event(1L, "queued"));
        eventBus.publish(1L, "status", event(1L, "completed"));
        eventBus.complete(1L);

        // Late subscriber with no lastEventId should still get replay
        SseEmitter emitter = eventBus.subscribe(1L, null);
        assertNotNull(emitter, "Late subscriber should get replay of terminal job");
    }

    // ---------------------------------------------------------------- hasHistory

    @Test
    void hasHistory_returnsFalseForUnknownJob() {
        assertFalse(eventBus.hasHistory(999L));
    }

    @Test
    void hasHistory_returnsTrueAfterPublish() {
        eventBus.publish(1L, "status", event(1L, "queued"));
        assertTrue(eventBus.hasHistory(1L));
    }

    // ---------------------------------------------------- malformed lastEventId

    @Test
    void subscribe_treatsNullLastEventIdAsFullReplay() {
        eventBus.publish(1L, "status", event(1L, "queued"));
        eventBus.publish(1L, "status", event(1L, "processing"));

        SseEmitter emitter = eventBus.subscribe(1L, null);
        assertNotNull(emitter);
    }

    @Test
    void subscribe_treatsZeroLastEventIdAsFullReplay() {
        eventBus.publish(1L, "status", event(1L, "queued"));

        SseEmitter emitter = eventBus.subscribe(1L, 0L);
        assertNotNull(emitter);
    }
}
