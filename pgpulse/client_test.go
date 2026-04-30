package pgpulse

import (
	"encoding/json"
	"testing"
)

func TestNewClient(t *testing.T) {
	c := NewClient(nil)
	if c == nil {
		t.Fatal("expected non-nil client")
	}
	if c.DB() != nil {
		t.Error("expected nil db when created with nil")
	}
}

func TestInsertParamsValidation(t *testing.T) {
	// Verify that defaults for InsertParams work correctly.
	p := InsertParams{
		Kind:    "email",
		Payload: json.RawMessage(`{"to":"test@example.com"}`),
	}

	// Queue should default to empty (set at Insert time).
	if p.Queue != "" {
		t.Errorf("expected empty queue, got %q", p.Queue)
	}

	// Priority should default to zero.
	if p.Priority != 0 {
		t.Errorf("expected 0 priority, got %d", p.Priority)
	}
}
