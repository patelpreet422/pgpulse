package com.pgpulse.controller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(
        properties = {
                "spring.profiles.active=test",
                "pgpulse.driver.enabled=false"
        }
)
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@AutoConfigureMockMvc
class JobControllerTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void cleanTable() {
        jdbc.update("DELETE FROM batch_jobs");
    }

    // ---------------------------------------------------------------- POST /api/jobs

    @Test
    void submit_returnsCreatedWithLocationHeader() throws Exception {
        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\": {\"task\": \"email\"}}"))
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.id").isNumber())
                .andExpect(jsonPath("$.status").value("queued"))
                .andExpect(jsonPath("$.retries").value(0))
                .andExpect(jsonPath("$.payload.task").value("email"));
    }

    @Test
    void submit_rejectsMissingPayload() throws Exception {
        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\": null}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void submit_rejectsEmptyBody() throws Exception {
        mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest());
    }

    // ---------------------------------------------------------------- GET /api/jobs/{id}

    @Test
    void get_returnsJobById() throws Exception {
        MvcResult created = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\": {\"task\": \"find_me\"}}"))
                .andReturn();

        String location = created.getResponse().getHeader("Location");

        mockMvc.perform(get(location))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.payload.task").value("find_me"))
                .andExpect(jsonPath("$.status").value("queued"));
    }

    @Test
    void get_returns404ForMissingJob() throws Exception {
        mockMvc.perform(get("/api/jobs/99999"))
                .andExpect(status().isNotFound());
    }

    // ---------------------------------------------------------------- GET /api/jobs

    @Test
    void list_returnsPaginatedResults() throws Exception {
        for (int i = 0; i < 5; i++) {
            mockMvc.perform(post("/api/jobs")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content("{\"payload\": {\"i\": " + i + "}}"));
        }

        mockMvc.perform(get("/api/jobs").param("size", "3"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content", hasSize(3)))
                .andExpect(jsonPath("$.totalElements").value(5))
                .andExpect(jsonPath("$.totalPages").value(2))
                .andExpect(jsonPath("$.page").value(0))
                .andExpect(jsonPath("$.size").value(3));
    }

    @Test
    void list_filtersByStatus() throws Exception {
        mockMvc.perform(post("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"payload\": {\"task\": \"a\"}}"));

        mockMvc.perform(get("/api/jobs").param("status", "queued"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content", hasSize(1)))
                .andExpect(jsonPath("$.content[0].status").value("queued"));

        mockMvc.perform(get("/api/jobs").param("status", "completed"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content", hasSize(0)));
    }

    @Test
    void list_clampsPageSize() throws Exception {
        mockMvc.perform(post("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"payload\": {\"task\": \"a\"}}"));

        // size=0 should be clamped to 1
        mockMvc.perform(get("/api/jobs").param("size", "0"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.size").value(1));

        // size=200 should be clamped to 100
        mockMvc.perform(get("/api/jobs").param("size", "200"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.size").value(100));
    }

    // ---------------------------------------------------------------- GET /api/jobs/stats

    @Test
    void stats_returnsCountsByStatus() throws Exception {
        mockMvc.perform(post("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"payload\": {\"task\": \"a\"}}"));
        mockMvc.perform(post("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"payload\": {\"task\": \"b\"}}"));

        mockMvc.perform(get("/api/jobs/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.queued").value(2));
    }

    // ----------------------------------------------------------- GET /api/jobs/{id}/events

    @Test
    void events_returns404ForMissingJob() throws Exception {
        mockMvc.perform(get("/api/jobs/99999/events")
                        .accept(MediaType.TEXT_EVENT_STREAM))
                .andExpect(status().isNotFound());
    }

    @Test
    void events_returnsSSEStreamForActiveJob() throws Exception {
        MvcResult created = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\": {\"task\": \"sse_test\"}}"))
                .andReturn();

        String location = created.getResponse().getHeader("Location");

        MvcResult sse = mockMvc.perform(get(location + "/events")
                        .accept(MediaType.TEXT_EVENT_STREAM))
                .andExpect(status().isOk())
                .andExpect(header().string("Cache-Control", "no-cache"))
                .andExpect(header().string("X-Accel-Buffering", "no"))
                .andReturn();

        String body = sse.getResponse().getContentAsString();
        // Should contain the queued event from enqueueReturning
        assertTrue(body.contains("event:status"), "Should contain SSE status event");
        assertTrue(body.contains("\"queued\""), "Should contain queued status");
    }

    @Test
    void events_returns204ForCompletedJobWithNoHistory() throws Exception {
        // Create a job and manually mark it completed (bypassing EventBus)
        jdbc.update("INSERT INTO batch_jobs (payload, status) VALUES ('{\"t\":1}'::jsonb, 'completed')");
        Long jobId = jdbc.queryForObject("SELECT id FROM batch_jobs", Long.class);

        mockMvc.perform(get("/api/jobs/" + jobId + "/events")
                        .accept(MediaType.TEXT_EVENT_STREAM))
                .andExpect(status().isNoContent());
    }

    @Test
    void events_handlesLastEventIdHeader() throws Exception {
        MvcResult created = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\": {\"task\": \"reconnect_test\"}}"))
                .andReturn();

        String location = created.getResponse().getHeader("Location");

        // Subscribe with Last-Event-ID — should still succeed (skips replay)
        MvcResult sse = mockMvc.perform(get(location + "/events")
                        .accept(MediaType.TEXT_EVENT_STREAM)
                        .header("Last-Event-ID", "1"))
                .andExpect(status().isOk())
                .andReturn();

        assertNotNull(sse.getResponse().getContentAsString());
    }

    @Test
    void events_handlesMalformedLastEventId() throws Exception {
        MvcResult created = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"payload\": {\"task\": \"malformed_test\"}}"))
                .andReturn();

        String location = created.getResponse().getHeader("Location");

        // Malformed Last-Event-ID should be treated as fresh subscription
        MvcResult sse = mockMvc.perform(get(location + "/events")
                        .accept(MediaType.TEXT_EVENT_STREAM)
                        .header("Last-Event-ID", "not-a-number"))
                .andExpect(status().isOk())
                .andReturn();

        String body = sse.getResponse().getContentAsString();
        assertTrue(body.contains("\"queued\""), "Malformed ID should trigger full replay");
    }
}
