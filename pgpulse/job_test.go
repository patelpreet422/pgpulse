package pgpulse

import (
	"encoding/json"
	"testing"
	"time"
)

func TestInsertParamsDefaults(t *testing.T) {
	p := InsertParams{Kind: "email"}

	if p.Queue != "" {
		t.Errorf("expected empty queue before defaults, got %q", p.Queue)
	}
	if p.MaxRetries != 0 {
		t.Errorf("expected 0 max_retries before defaults, got %d", p.MaxRetries)
	}
}

func TestJobStateConstants(t *testing.T) {
	states := []JobState{
		JobStateAvailable,
		JobStateRunning,
		JobStateCompleted,
		JobStateFailed,
		JobStateDiscarded,
	}
	expected := []string{"available", "running", "completed", "failed", "discarded"}

	for i, s := range states {
		if string(s) != expected[i] {
			t.Errorf("state %d: expected %q, got %q", i, expected[i], s)
		}
	}
}

func TestJobJSONSerialization(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	job := Job{
		ID:          42,
		Queue:       "default",
		Kind:        "email",
		Payload:     json.RawMessage(`{"to":"user@example.com"}`),
		Priority:    5,
		State:       JobStateAvailable,
		Attempt:     0,
		MaxRetries:  3,
		ScheduledAt: now,
		CreatedAt:   now,
	}

	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded Job
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.ID != 42 {
		t.Errorf("expected ID 42, got %d", decoded.ID)
	}
	if decoded.Kind != "email" {
		t.Errorf("expected kind 'email', got %q", decoded.Kind)
	}
	if decoded.State != JobStateAvailable {
		t.Errorf("expected state 'available', got %q", decoded.State)
	}
}

func TestJobErrorJSON(t *testing.T) {
	je := JobError{
		Attempt:    1,
		Error:      "connection refused",
		OccurredAt: time.Now().Truncate(time.Second),
	}

	data, err := json.Marshal(je)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded JobError
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Error != "connection refused" {
		t.Errorf("expected 'connection refused', got %q", decoded.Error)
	}
}
